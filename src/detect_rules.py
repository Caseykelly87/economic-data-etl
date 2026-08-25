"""Pure-pandas exception detection rules for store-day metrics.

This module owns the logic of phase 2: it consumes a normalized
``store_daily_metrics`` DataFrame plus a ``dim_stores`` DataFrame and
returns an ``anomaly_flags`` DataFrame. It performs no IO outside of
:func:`load_rules_config`, which reads the YAML config; everything else
is pandas + numpy + the schema/exception contracts.

Five statistical-band rules evaluate at store-day grain against the
``store_daily_metrics`` frame:

* ``revenue_band`` — total_sales vs ``base_daily_revenue`` ± ``band_pct``
* ``labor_pct_band`` — labor_cost_pct vs profile center ± ``half_width_pp``
* ``avg_ticket_band`` — avg_basket_size vs profile center ± ``band_pct``
* ``transactions_band`` — transaction_count vs ``base_daily_revenue / avg_ticket_center``
  ± ``band_pct``
* ``yoy_comp`` — current/T-365 sales ratio outside ``[ratio_lower, ratio_upper]``

One rolling-baseline rule evaluates at store-day grain against the
``store_daily_metrics`` frame, comparing each day's ``total_sales``
against a learned per-store baseline instead of a configured band:

* ``revenue_zscore_28d`` — z-score of ``total_sales`` against the
  trailing 28-day rolling mean and stddev for the store

Three rules evaluate per store-day against the department-grain
``department_daily_metrics`` frame, when that frame is supplied. Margin
and the cross-grain sales total live only at department grain, so these
rules bridge to store-day grain by grouping the department frame on
``(date, store_id)`` rather than reading the store-day band frame:

* ``department_coverage`` — department row count not equal to
  ``expected_row_count``, or a ``department_id`` repeated within a
  store-day
* ``gross_margin_band`` — any department's ``gross_margin_pct`` outside
  ``center ± half_width``; one flag per store-day carrying the single
  most extreme department
* ``department_reconciliation`` — the sum of a store-day's department
  ``net_sales`` differs from the store-grain ``total_sales`` by more
  than ``tolerance`` dollars

Severity bucketing for the band rules: ``info`` if score ≤ ``info_max``,
``warning`` if score ≤ ``warning_max``, else ``critical``. Score is the
distance past the nearer band edge expressed in band-half-widths. The
``gross_margin_band`` rule reuses this ladder. The
``revenue_zscore_28d`` rule uses its own bucketing on ``|z|`` itself
(``info`` 2.5–3, ``warning`` 3–4, ``critical`` ≥ 4). The
``department_coverage`` and ``department_reconciliation`` structural
rules do not produce a graded score; each emits the fixed severity
declared in its config.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import yaml

from src.exceptions import DetectionConfigError, DetectionInputError
from src.schemas import (
    ANOMALY_FLAG_COLUMNS,
    KNOWN_PROFILES,
    RULE_IDS,
)

# ==============================================================================
# Config loader
# ==============================================================================


def load_rules_config(path: Path) -> dict:
    """Load and structurally validate ``config/detection_rules.yaml``.

    Raises
    ------
    DetectionConfigError
        When a required top-level section is missing, when any rule
        referenced in :data:`RULE_IDS` is absent, when a profile-keyed
        band references a profile not in :data:`KNOWN_PROFILES`, or
        when a required key inside any rule block is missing.
    """
    with Path(path).open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        raise DetectionConfigError("rules config must be a mapping", path=str(path))

    for section in ("rules", "severity"):
        if section not in cfg:
            raise DetectionConfigError(
                "rules config missing required section",
                path=str(path),
                missing_section=section,
            )

    rules = cfg["rules"]
    for rule_id in RULE_IDS:
        if rule_id not in rules:
            raise DetectionConfigError(
                "rules config missing required rule",
                path=str(path),
                missing_rule=rule_id,
            )

    _validate_profile_keys(rules.get("labor_pct_band", {}), path,
                            rule_id="labor_pct_band")
    _validate_profile_keys(rules.get("avg_ticket_band", {}), path,
                            rule_id="avg_ticket_band")

    return cfg


def _validate_profile_keys(rule_cfg: dict, path: Path, rule_id: str) -> None:
    bands = rule_cfg.get("bands_by_profile", {})
    if not isinstance(bands, dict):
        return
    unknown = sorted(set(bands.keys()) - KNOWN_PROFILES)
    if unknown:
        raise DetectionConfigError(
            "unknown trade_area_profile referenced by rule",
            path=str(path),
            rule_id=rule_id,
            unknown_profiles=unknown,
        )


# ==============================================================================
# Orchestrator
# ==============================================================================


def run_all_rules(
    metrics_df: pd.DataFrame,
    dim_stores_df: pd.DataFrame,
    rules_config: dict,
    *,
    department_metrics_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Evaluate every enabled rule and return the combined anomaly flags.

    The statistical-band rules run against ``metrics_df`` (store-day
    grain). The ``department_coverage``, ``gross_margin_band``, and
    ``department_reconciliation`` rules run against
    ``department_metrics_df`` (store-day-department grain) when that frame
    is supplied; when it is ``None`` they are skipped, leaving the
    band-rule output unchanged. ``department_reconciliation`` additionally
    needs the store-day frame to compare the two grains, so it receives the
    enriched store-day frame alongside the department frame.

    Returns a DataFrame whose columns are exactly
    :data:`ANOMALY_FLAG_COLUMNS`, sorted by ``(date, store_id, rule_id)``.

    Raises
    ------
    DetectionInputError
        When a ``store_id`` in ``metrics_df`` is absent from
        ``dim_stores_df``.
    """
    metrics_ids = set(int(x) for x in metrics_df["store_id"].unique()) \
        if len(metrics_df) else set()
    dim_ids = set(int(x) for x in dim_stores_df["store_id"].unique())
    orphans = sorted(metrics_ids - dim_ids)
    if orphans:
        raise DetectionInputError(
            "metrics_df references store_ids not in dim_stores",
            orphan_store_ids=orphans,
        )

    enriched = metrics_df.merge(
        dim_stores_df[["store_id", "base_daily_revenue", "trade_area_profile"]],
        on="store_id",
        how="left",
    )

    severity_cfg = rules_config.get("severity", {"info_max": 1.0, "warning_max": 2.0})
    rules_cfg = rules_config["rules"]
    avg_ticket_centers = _profile_to_avg_ticket(rules_config)

    flags: list[dict] = []
    for rule_id, rule_func in _RULE_FUNCS.items():
        rule_cfg = rules_cfg.get(rule_id, {})
        if not rule_cfg.get("enabled", False):
            continue
        flags.extend(
            rule_func(
                enriched, rule_cfg, severity_cfg,
                avg_ticket_centers=avg_ticket_centers,
            )
        )

    dept_cfg = rules_cfg.get("department_coverage", {})
    if dept_cfg.get("enabled", False) and department_metrics_df is not None:
        flags.extend(_department_coverage(department_metrics_df, dept_cfg))

    margin_cfg = rules_cfg.get("gross_margin_band", {})
    if margin_cfg.get("enabled", False) and department_metrics_df is not None:
        flags.extend(
            _gross_margin_band(department_metrics_df, margin_cfg, severity_cfg)
        )

    recon_cfg = rules_cfg.get("department_reconciliation", {})
    if recon_cfg.get("enabled", False) and department_metrics_df is not None:
        flags.extend(
            _department_reconciliation(enriched, department_metrics_df, recon_cfg)
        )

    return _flags_to_frame(flags)


# ==============================================================================
# Per-rule evaluators
# ==============================================================================


def _revenue_band(
    enriched: pd.DataFrame, rule_cfg: dict, severity_cfg: dict,
    *, avg_ticket_centers: dict[str, float] | None = None,
) -> list[dict]:
    flags: list[dict] = []
    band_pct = float(rule_cfg["band_pct"])
    for row in enriched.itertuples(index=False):
        expected = float(row.base_daily_revenue)
        half_width = expected * band_pct
        low = expected - half_width
        high = expected + half_width
        actual = float(row.total_sales)
        flag = _maybe_flag(
            d=row.date, store_id=int(row.store_id), rule_id="revenue_band",
            actual=actual, low=low, high=high, half_width=half_width,
            severity_cfg=severity_cfg,
        )
        if flag is not None:
            flags.append(flag)
    return flags


def _labor_pct_band(
    enriched: pd.DataFrame, rule_cfg: dict, severity_cfg: dict,
    *, avg_ticket_centers: dict[str, float] | None = None,
) -> list[dict]:
    flags: list[dict] = []
    bands = rule_cfg["bands_by_profile"]
    for row in enriched.itertuples(index=False):
        if float(row.total_sales) == 0:
            continue
        actual = row.labor_cost_pct
        if actual is None or _isnan(actual):
            continue
        profile_band = bands.get(row.trade_area_profile)
        if profile_band is None:
            continue
        center = float(profile_band["center"])
        half_width = float(profile_band["half_width_pp"])
        low = center - half_width
        high = center + half_width
        flag = _maybe_flag(
            d=row.date, store_id=int(row.store_id), rule_id="labor_pct_band",
            actual=float(actual), low=low, high=high, half_width=half_width,
            severity_cfg=severity_cfg,
        )
        if flag is not None:
            flags.append(flag)
    return flags


def _avg_ticket_band(
    enriched: pd.DataFrame, rule_cfg: dict, severity_cfg: dict,
    *, avg_ticket_centers: dict[str, float] | None = None,
) -> list[dict]:
    flags: list[dict] = []
    bands = rule_cfg["bands_by_profile"]
    for row in enriched.itertuples(index=False):
        if float(row.total_sales) == 0:
            continue
        actual = row.avg_basket_size
        if actual is None or _isnan(actual):
            continue
        profile_band = bands.get(row.trade_area_profile)
        if profile_band is None:
            continue
        center = float(profile_band["center"])
        half_width = center * float(profile_band["band_pct"])
        low = center - half_width
        high = center + half_width
        flag = _maybe_flag(
            d=row.date, store_id=int(row.store_id), rule_id="avg_ticket_band",
            actual=float(actual), low=low, high=high, half_width=half_width,
            severity_cfg=severity_cfg,
        )
        if flag is not None:
            flags.append(flag)
    return flags


def _transactions_band(
    enriched: pd.DataFrame, rule_cfg: dict, severity_cfg: dict,
    *, avg_ticket_centers: dict[str, float] | None = None,
) -> list[dict]:
    flags: list[dict] = []
    band_pct = float(rule_cfg["band_pct"])
    centers = avg_ticket_centers or {}
    for row in enriched.itertuples(index=False):
        if float(row.total_sales) == 0:
            continue
        center_ticket = centers.get(row.trade_area_profile)
        if center_ticket is None:
            continue
        expected = float(row.base_daily_revenue) / float(center_ticket)
        half_width = expected * band_pct
        low = expected - half_width
        high = expected + half_width
        actual = float(row.transaction_count)
        flag = _maybe_flag(
            d=row.date, store_id=int(row.store_id), rule_id="transactions_band",
            actual=actual, low=low, high=high, half_width=half_width,
            severity_cfg=severity_cfg,
        )
        if flag is not None:
            flags.append(flag)
    return flags


def _yoy_comp(
    enriched: pd.DataFrame, rule_cfg: dict, severity_cfg: dict,
    *, avg_ticket_centers: dict[str, float] | None = None,
) -> list[dict]:
    flags: list[dict] = []
    ratio_lower = float(rule_cfg["ratio_lower"])
    ratio_upper = float(rule_cfg["ratio_upper"])
    half_width = (ratio_upper - ratio_lower) / 2.0
    # Build (store_id, date) → total_sales lookup.
    lookup: dict[tuple[int, date], float] = {}
    for row in enriched.itertuples(index=False):
        lookup[(int(row.store_id), row.date)] = float(row.total_sales)
    for row in enriched.itertuples(index=False):
        prior_key = (int(row.store_id), row.date - timedelta(days=365))
        if prior_key not in lookup:
            continue
        prior_sales = lookup[prior_key]
        if prior_sales == 0:
            continue
        ratio = float(row.total_sales) / prior_sales
        flag = _maybe_flag(
            d=row.date, store_id=int(row.store_id), rule_id="yoy_comp",
            actual=ratio, low=ratio_lower, high=ratio_upper,
            half_width=half_width, severity_cfg=severity_cfg,
        )
        if flag is not None:
            flags.append(flag)
    return flags


def _revenue_zscore_28d(
    enriched: pd.DataFrame, rule_cfg: dict, severity_cfg: dict,
    *, avg_ticket_centers: dict[str, float] | None = None,
) -> list[dict]:
    """Flag store-days whose ``total_sales`` is far from the store's recent baseline.

    For each store-day, the trailing rolling mean and stddev of
    ``total_sales`` are computed over the prior ``window_days``
    observations for that store (the current row is excluded from its
    own window — the same shape as ``yoy_comp`` comparing against an
    independent T-365 row). A store-day fires when the resulting
    ``|z|`` is at least ``zscore_threshold``.

    Semantics of ``min_history_days``: counts prior observations
    excluding the current row. With ``min_history_days=14`` a store's
    earliest evaluable day is its 15th observation; rows with fewer
    than 14 prior observations are silently skipped, the same way
    ``yoy_comp`` silently skips dates without a T-365 row. Rows whose
    rolling stddev is zero or NaN are also skipped — z-score is
    undefined when the recent history is constant.

    Flag fields mirror the band-rule shape for portability:
    ``expected_low`` and ``expected_high`` both carry the rolling mean
    (the point-estimate baseline this rule learned), ``actual_value``
    is the observed ``total_sales``, ``distance_from_band`` is
    ``|actual - rolling_mean|`` in dollars, and ``severity_score`` is
    ``|z|`` — a unitless distance in stddev-multiples rather than a
    distance in band-half-widths.
    """
    window = int(rule_cfg.get("window_days", 28))
    min_periods = int(rule_cfg.get("min_history_days", 14))
    threshold = float(rule_cfg.get("zscore_threshold", 2.5))

    if len(enriched) == 0:
        return []

    df = enriched[["date", "store_id", "total_sales"]].sort_values(
        ["store_id", "date"]
    ).reset_index(drop=True)

    sales_by_store = df.groupby("store_id", sort=False)["total_sales"]
    rolling_mean = sales_by_store.transform(
        lambda s: s.shift(1).rolling(window=window, min_periods=min_periods).mean()
    )
    rolling_std = sales_by_store.transform(
        lambda s: s.shift(1).rolling(window=window, min_periods=min_periods).std(ddof=1)
    )

    valid = rolling_mean.notna() & rolling_std.notna() & (rolling_std > 0)
    if not valid.any():
        return []

    actual = df["total_sales"].astype(float)
    abs_z = pd.Series(np.nan, index=df.index, dtype="float64")
    abs_z.loc[valid] = (
        (actual.loc[valid] - rolling_mean.loc[valid]) / rolling_std.loc[valid]
    ).abs()

    fire = valid & (abs_z >= threshold)
    if not fire.any():
        return []

    fired_idx = df.index[fire]
    fired_mean = rolling_mean.loc[fired_idx].astype(float).to_numpy()
    fired_actual = actual.loc[fired_idx].to_numpy()
    fired_abs_z = abs_z.loc[fired_idx].to_numpy()

    fired = pd.DataFrame({
        "date": df.loc[fired_idx, "date"].to_numpy(),
        "store_id": df.loc[fired_idx, "store_id"].astype(np.int64).to_numpy(),
        "rule_id": "revenue_zscore_28d",
        "actual_value": fired_actual,
        "expected_low": fired_mean,
        "expected_high": fired_mean,
        "distance_from_band": np.abs(fired_actual - fired_mean),
        "severity_score": fired_abs_z,
        "severity_level": _zscore_severity(fired_abs_z),
    })
    return fired.to_dict("records")


def _zscore_severity(abs_z: np.ndarray) -> np.ndarray:
    """Bucket ``|z|`` into info / warning / critical for the z-score rule.

    The cutoffs (3 and 4) are the rule's own thresholds and are
    independent of the band rules' ``severity_cfg`` ladder, which is
    expressed in band-half-widths rather than stddev-multiples.
    """
    levels = np.full(len(abs_z), "info", dtype=object)
    levels[abs_z >= 3.0] = "warning"
    levels[abs_z >= 4.0] = "critical"
    return levels


def _department_coverage(
    department_metrics_df: pd.DataFrame, rule_cfg: dict,
) -> list[dict]:
    """Flag store-days whose department-grain shape is irregular.

    Unlike the band rules this evaluates the department-grain frame, one
    group per ``(date, store_id)``. A store-day fires when its department
    row count is not ``expected_row_count`` or — when ``detect_duplicates``
    is set — when a ``department_id`` is repeated within the store-day.

    One flag is emitted per offending store-day. ``actual_value`` carries
    the observed row count, which keeps the missing-department case (count
    below expected) and the duplicated-department case (count above
    expected) distinguishable in the flags output. ``severity_score`` is a
    fixed ``1.0`` rather than a graded distance: a structural irregularity
    is binary, not a position on a band.
    """
    if department_metrics_df is None or len(department_metrics_df) == 0:
        return []

    expected = int(rule_cfg.get("expected_row_count", 10))
    detect_duplicates = bool(rule_cfg.get("detect_duplicates", True))
    severity_level = str(rule_cfg.get("severity_level", "warning"))

    flags: list[dict] = []
    for (d, store_id), group in department_metrics_df.groupby(
        ["date", "store_id"], sort=True,
    ):
        row_count = len(group)
        has_duplicate = bool(group["department_id"].duplicated().any())
        count_off = row_count != expected
        if not count_off and not (detect_duplicates and has_duplicate):
            continue
        flags.append({
            "date": d,
            "store_id": int(store_id),
            "rule_id": "department_coverage",
            "actual_value": float(row_count),
            "expected_low": float(expected),
            "expected_high": float(expected),
            "distance_from_band": float(abs(row_count - expected)),
            "severity_score": 1.0,
            "severity_level": severity_level,
        })
    return flags


def _gross_margin_band(
    department_metrics_df: pd.DataFrame, rule_cfg: dict, severity_cfg: dict,
) -> list[dict]:
    """Flag store-days carrying a department gross-margin outlier.

    Gross margin exists only at department grain, so this rule evaluates
    the department-grain frame one group per ``(date, store_id)`` — the
    same grain-bridging shape as :func:`_department_coverage` — rather than
    the store-day band frame. A store-day fires when any of its departments
    has a ``gross_margin_pct`` outside ``center ± half_width``.

    One flag is emitted per offending store-day, carrying the single most
    extreme department (the one furthest past a band edge). Aggregating
    margin to the store-day level first would not work: a single department
    swinging to a 0.95 margin barely moves the sales-weighted store-day
    mean, so the outlier has to be caught at the grain it occurs on.

    ``severity_score = distance_past_edge / half_width`` reuses the band
    rules' ladder, so margin flags bucket into the same info / warning /
    critical levels through ``severity_cfg``.
    """
    if department_metrics_df is None or len(department_metrics_df) == 0:
        return []

    center = float(rule_cfg["center"])
    half_width = float(rule_cfg["half_width"])
    low = center - half_width
    high = center + half_width

    flags: list[dict] = []
    for (d, store_id), group in department_metrics_df.groupby(
        ["date", "store_id"], sort=True,
    ):
        worst_margin: float | None = None
        worst_distance = 0.0
        for margin in group["gross_margin_pct"]:
            if margin is None or _isnan(margin):
                continue
            m = float(margin)
            if low <= m <= high:
                continue
            distance = (low - m) if m < low else (m - high)
            if worst_margin is None or distance > worst_distance:
                worst_margin = m
                worst_distance = distance
        if worst_margin is None:
            continue
        score = worst_distance / half_width if half_width > 0 else float("inf")
        flags.append({
            "date": d,
            "store_id": int(store_id),
            "rule_id": "gross_margin_band",
            "actual_value": worst_margin,
            "expected_low": low,
            "expected_high": high,
            "distance_from_band": worst_distance,
            "severity_score": score,
            "severity_level": _severity(score, severity_cfg),
        })
    return flags


def _department_reconciliation(
    enriched: pd.DataFrame, department_metrics_df: pd.DataFrame, rule_cfg: dict,
) -> list[dict]:
    """Flag store-days whose department net_sales do not sum to the store total.

    A cross-grain integrity check: a store-day's ``total_sales`` must equal
    the sum of its department-grain ``net_sales``. The sim engine derives
    the store total from department detail, so in clean data the two grains
    agree to floating-point precision (the largest residual across
    well-formed store-days is on the order of 1e-11). A store-day fires when
    the absolute difference exceeds ``tolerance`` dollars.

    Tolerance choice: the smallest injected integrity breach shifts a
    department's ``net_sales`` by about $50, while legitimate residual is
    sub-cent rounding from aggregating two-decimal currency values. A $1.00
    default sits well inside that gap — wide enough to absorb rounding,
    tight enough that no injected mismatch slips through.

    Structural like :func:`_department_coverage`: the violation is binary,
    so ``severity_score`` is a fixed 1.0 and ``severity_level`` comes from
    config. ``actual_value`` carries the department sum and
    ``expected_low`` / ``expected_high`` the store total, so the direction
    and size of the mismatch stay legible. Skipped cleanly when no
    department frame is supplied.
    """
    if department_metrics_df is None or len(department_metrics_df) == 0:
        return []

    tolerance = float(rule_cfg.get("tolerance", 1.0))
    severity_level = str(rule_cfg.get("severity_level", "warning"))

    store_totals: dict[tuple[date, int], float] = {
        (row.date, int(row.store_id)): float(row.total_sales)
        for row in enriched.itertuples(index=False)
    }

    dept_sums = department_metrics_df.groupby(
        ["date", "store_id"], sort=True,
    )["net_sales"].sum()

    flags: list[dict] = []
    for (d, store_id), dept_sum in dept_sums.items():
        store_total = store_totals.get((d, int(store_id)))
        if store_total is None:
            continue
        dept_sum = float(dept_sum)
        residual = abs(store_total - dept_sum)
        if residual <= tolerance:
            continue
        flags.append({
            "date": d,
            "store_id": int(store_id),
            "rule_id": "department_reconciliation",
            "actual_value": dept_sum,
            "expected_low": store_total,
            "expected_high": store_total,
            "distance_from_band": residual,
            "severity_score": 1.0,
            "severity_level": severity_level,
        })
    return flags


# The dispatch table covers the rules that evaluate against the
# store-day metrics frame: the five statistical-band rules plus the
# rolling-baseline z-score rule. The structural ``department_coverage``,
# ``gross_margin_band``, and ``department_reconciliation`` rules read the
# department-grain frame (the last also the store-day frame) and are
# invoked directly by :func:`run_all_rules`.
_RULE_FUNCS: dict[str, Callable[..., list[dict]]] = {
    "revenue_band":       _revenue_band,
    "labor_pct_band":     _labor_pct_band,
    "avg_ticket_band":    _avg_ticket_band,
    "transactions_band":  _transactions_band,
    "yoy_comp":           _yoy_comp,
    "revenue_zscore_28d": _revenue_zscore_28d,
}


# ==============================================================================
# Helpers
# ==============================================================================


def _isnan(v: Any) -> bool:
    try:
        return bool(np.isnan(v))
    except (TypeError, ValueError):
        return False


def _maybe_flag(
    *,
    d: date,
    store_id: int,
    rule_id: str,
    actual: float,
    low: float,
    high: float,
    half_width: float,
    severity_cfg: dict,
) -> dict | None:
    if low <= actual <= high:
        return None
    distance = (low - actual) if actual < low else (actual - high)
    score = distance / half_width if half_width > 0 else float("inf")
    return {
        "date": d,
        "store_id": store_id,
        "rule_id": rule_id,
        "actual_value": actual,
        "expected_low": low,
        "expected_high": high,
        "distance_from_band": distance,
        "severity_score": score,
        "severity_level": _severity(score, severity_cfg),
    }


def _severity(score: float, severity_cfg: dict) -> str:
    info_max = float(severity_cfg.get("info_max", 1.0))
    warning_max = float(severity_cfg.get("warning_max", 2.0))
    if score <= info_max:
        return "info"
    if score <= warning_max:
        return "warning"
    return "critical"


def _profile_to_avg_ticket(rules_config: dict | None) -> dict[str, float]:
    """Pull the per-profile avg_ticket centers out of avg_ticket_band config.

    Resolved by walking the rules_config; the transactions_band rule
    needs the same profile centers so they live in one place.
    """
    if not rules_config:
        return {}
    bands = rules_config.get("rules", {}).get("avg_ticket_band", {}).get(
        "bands_by_profile", {}
    )
    return {profile: float(b["center"]) for profile, b in bands.items()}


def _flags_to_frame(flags: list[dict]) -> pd.DataFrame:
    """Build the output DataFrame with deterministic dtypes and sort."""
    if not flags:
        empty = {
            "date":               pd.Series([], dtype="object"),
            "store_id":           pd.Series([], dtype="int64"),
            "rule_id":            pd.Series([], dtype="object"),
            "actual_value":       pd.Series([], dtype="float64"),
            "expected_low":       pd.Series([], dtype="float64"),
            "expected_high":      pd.Series([], dtype="float64"),
            "distance_from_band": pd.Series([], dtype="float64"),
            "severity_score":     pd.Series([], dtype="float64"),
            "severity_level":     pd.Series([], dtype="object"),
        }
        return pd.DataFrame(empty)[list(ANOMALY_FLAG_COLUMNS)]

    df = pd.DataFrame(flags)[list(ANOMALY_FLAG_COLUMNS)]
    df["store_id"] = df["store_id"].astype(np.int64)
    for col in ("actual_value", "expected_low", "expected_high",
                "distance_from_band", "severity_score"):
        df[col] = df[col].astype(np.float64)
    df["rule_id"] = df["rule_id"].astype(str)
    df["severity_level"] = df["severity_level"].astype(str)
    df = df.sort_values(["date", "store_id", "rule_id"]).reset_index(drop=True)
    return df
