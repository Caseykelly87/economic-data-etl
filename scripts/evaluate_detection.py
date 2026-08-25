"""Detection-quality evaluation script — NOT part of the ETL package.

This script measures phase 2 detection recall and false-positive rate
against the sim engine's ``anomaly_log.csv`` ground-truth file. It is
the only piece of code in this repository permitted to read that log.

Why isolated:
- The ETL itself must not have any knowledge of the injection log.
  Detection is supposed to operate purely on operational data; if the
  generator's diary leaks into the package, recall numbers measure
  pattern-matching, not real detection.
- The package must remain importable and runnable without this script
  existing or being on PYTHONPATH. Nothing under ``src/`` may import
  from here.

How to run::

    .venv/Scripts/python.exe scripts/evaluate_detection.py \\
        --flags-path data/processed/anomaly_flags.parquet \\
        --anomaly-log-path /path/to/sim/output/anomaly_log.csv \\
        --metrics-path data/processed/store_daily_metrics.parquet

Reports:
    * Global recall: of injected (date, store_id) pairs, fraction with
      at least one flag.
    * Per-anomaly-type recall: same, grouped by anomaly_type.
    * False-positive rate: of unflagged (date, store_id) pairs, fraction
      that nonetheless received a flag.
    * Flag rate: total flag rows / total metrics rows (advisory).

Pass/fail verdict: phase 2 contract is global recall >= 0.35 AND
false-positive rate <= 0.10. Exits 0 on PASS, 1 on FAIL.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

CONTRACT_GLOBAL_RECALL = 0.35
CONTRACT_FPR = 0.10


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="evaluate_detection",
        description=(
            "Measure detection recall and false-positive rate against "
            "the sim engine's anomaly_log.csv ground truth."
        ),
    )
    p.add_argument("--flags-path", type=Path, required=True,
                   help="Path to anomaly_flags.parquet produced by detect_cli.")
    p.add_argument("--anomaly-log-path", type=Path, required=True,
                   help="Path to the sim engine's anomaly_log.csv.")
    p.add_argument("--metrics-path", type=Path, required=True,
                   help="Path to store_daily_metrics.parquet (for FPR universe).")
    p.add_argument("--output-path", type=Path,
                   help="Optional path to write the JSON report to.")
    return p


def load_inputs(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    flags = pd.read_parquet(args.flags_path)
    metrics = pd.read_parquet(args.metrics_path)
    log = pd.read_csv(args.anomaly_log_path)
    if "date_key" in log.columns:
        log["date"] = pd.to_datetime(log["date_key"]).dt.date
    elif "date" in log.columns:
        log["date"] = pd.to_datetime(log["date"]).dt.date
    else:
        raise ValueError("anomaly_log.csv must have date_key or date column")
    if "store_id" not in log.columns:
        raise ValueError("anomaly_log.csv must have store_id column")
    log["store_id"] = log["store_id"].astype("Int64")
    return flags, metrics, log


def _store_day_set(df: pd.DataFrame) -> set[tuple[Any, int]]:
    return {(d, int(s)) for d, s in zip(df["date"], df["store_id"])}


def compute_metrics(
    flags: pd.DataFrame, metrics: pd.DataFrame, log: pd.DataFrame,
) -> dict:
    flagged = _store_day_set(flags)
    metric_cells = _store_day_set(metrics)

    log_rows = log.dropna(subset=["store_id"]).copy()
    log_rows["store_id"] = log_rows["store_id"].astype(int)

    injected_pairs = {(d, sid) for d, sid in zip(log_rows["date"], log_rows["store_id"])}

    matched_pairs = injected_pairs & flagged
    global_recall = len(matched_pairs) / len(injected_pairs) if injected_pairs else 0.0

    by_type: dict[str, dict] = {}
    if "anomaly_type" in log_rows.columns:
        for atype, grp in log_rows.groupby("anomaly_type"):
            type_pairs = {(d, sid) for d, sid in zip(grp["date"], grp["store_id"])}
            type_matched = type_pairs & flagged
            by_type[str(atype)] = {
                "injected": len(type_pairs),
                "matched": len(type_matched),
                "recall": len(type_matched) / len(type_pairs) if type_pairs else 0.0,
            }

    unflagged_universe = metric_cells - injected_pairs
    fps = unflagged_universe & flagged
    fpr = len(fps) / len(unflagged_universe) if unflagged_universe else 0.0

    flag_rate = len(flags) / len(metrics) if len(metrics) else 0.0

    return {
        "global": {
            "injected_pairs": len(injected_pairs),
            "matched_pairs": len(matched_pairs),
            "recall": global_recall,
        },
        "by_anomaly_type": by_type,
        "false_positive_rate": fpr,
        "false_positives": len(fps),
        "negative_universe": len(unflagged_universe),
        "flag_rate": flag_rate,
        "total_flags": len(flags),
        "total_metric_rows": len(metrics),
    }


def render_report(report: dict) -> str:
    lines: list[str] = []
    g = report["global"]
    lines.append("=" * 60)
    lines.append("Detection quality evaluation")
    lines.append("=" * 60)
    lines.append(
        f"Global recall:  {g['recall']:.3f}  "
        f"({g['matched_pairs']} / {g['injected_pairs']} injected pairs)"
    )
    lines.append(
        f"FPR:            {report['false_positive_rate']:.3f}  "
        f"({report['false_positives']} / {report['negative_universe']} negatives)"
    )
    lines.append(
        f"Flag rate:      {report['flag_rate']:.3f}  "
        f"({report['total_flags']} flags / {report['total_metric_rows']} metric rows)"
    )
    if report["by_anomaly_type"]:
        lines.append("")
        lines.append("Per-anomaly-type recall:")
        for atype, stats in sorted(report["by_anomaly_type"].items()):
            lines.append(
                f"  {atype:<24} recall={stats['recall']:.3f}  "
                f"({stats['matched']} / {stats['injected']})"
            )
    lines.append("")
    pass_recall = g["recall"] >= CONTRACT_GLOBAL_RECALL
    pass_fpr = report["false_positive_rate"] <= CONTRACT_FPR
    verdict = "PASS" if (pass_recall and pass_fpr) else "FAIL"
    lines.append(
        f"Contract: global_recall >= {CONTRACT_GLOBAL_RECALL} "
        f"AND fpr <= {CONTRACT_FPR}  -->  {verdict}"
    )
    if not pass_recall:
        lines.append(f"  - global recall {g['recall']:.3f} < {CONTRACT_GLOBAL_RECALL}")
    if not pass_fpr:
        lines.append(f"  - fpr {report['false_positive_rate']:.3f} > {CONTRACT_FPR}")
    lines.append("=" * 60)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    flags, metrics, log = load_inputs(args)
    report = compute_metrics(flags, metrics, log)

    print(render_report(report))

    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(json.dumps(report, default=str, indent=2),
                                    encoding="utf-8")

    pass_recall = report["global"]["recall"] >= CONTRACT_GLOBAL_RECALL
    pass_fpr = report["false_positive_rate"] <= CONTRACT_FPR
    return 0 if (pass_recall and pass_fpr) else 1


if __name__ == "__main__":
    sys.exit(main())
