"""Pure-pandas exception detection rules for store-day metrics.

This module owns the logic of phase 2: it consumes a normalized
``store_daily_metrics`` DataFrame plus a ``dim_stores`` DataFrame and
returns an ``anomaly_flags`` DataFrame. It performs no IO outside of
:func:`load_rules_config`, which reads the YAML config; everything else
is pandas + numpy + the schema/exception contracts.

The five rules evaluate at store-day grain:

* ``revenue_band`` — total_sales vs ``base_daily_revenue`` ± ``band_pct``
* ``labor_pct_band`` — labor_cost_pct vs profile center ± ``half_width_pp``
* ``avg_ticket_band`` — avg_basket_size vs profile center ± ``band_pct``
* ``transactions_band`` — transaction_count vs ``base_daily_revenue / avg_ticket_center`` ± ``band_pct``
* ``yoy_comp`` — current/T-365 sales ratio outside ``[ratio_lower, ratio_upper]``

Severity bucketing: ``info`` if score ≤ ``info_max``, ``warning`` if
score ≤ ``warning_max``, else ``critical``. Score is the distance past
the nearer band edge expressed in band-half-widths.
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
) -> pd.DataFrame:
    """Evaluate every enabled rule against ``metrics_df``.

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
    for rule_id in RULE_IDS:
        rule_cfg = rules_cfg.get(rule_id, {})
        if not rule_cfg.get("enabled", False):
            continue
        flags.extend(
            _RULE_FUNCS[rule_id](
                enriched, rule_cfg, severity_cfg,
                avg_ticket_centers=avg_ticket_centers,
            )
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


_RULE_FUNCS: dict[str, Callable[..., list[dict]]] = {
    "revenue_band":     _revenue_band,
    "labor_pct_band":   _labor_pct_band,
    "avg_ticket_band":  _avg_ticket_band,
    "transactions_band": _transactions_band,
    "yoy_comp":         _yoy_comp,
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
