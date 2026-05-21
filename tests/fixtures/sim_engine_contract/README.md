# Sim engine contract fixture

A verbatim capture of the simulation engine's output for a single store-day,
`2024-07-01`, used by `tests/test_sim_engine_contract.py` to pin the contract
between sim engine output and the ETL transform.

## How this differs from `tests/fixtures/sim_engine/`

The `happy/`, `anomalous/`, `corrupt_missing_column/` and
`partial_missing_date/` fixtures are hand-authored. Their values are clean
round numbers (`net_sales_total 87400.00`, `labor_cost_pct 0.1050`,
`avg_ticket 50.00`) chosen so the detection-rule tests can assert predictable
band outcomes. They mirror the sim engine's directory layout and CSV schema
but are not engine output.

This fixture is the opposite: the numbers are the realism-applied output the
sim engine actually produces (`net_sales_total 86429.35`, `labor_cost_pct
0.1148`). It exists so at least one test exercises the ETL against genuine
upstream data rather than scaffolding.

## Contents

```
output/
  daily/07/01/2024/
    store_summary.csv       8 stores
    department_sales.csv    8 stores x 10 departments = 80 rows
  dimensions/dim_stores.csv 8 stores, full column set
```

`2024-07-01` was chosen because its `anomaly_log.csv` is empty — a normal
store-day with no injected anomalies — so the detection contract test can
assert that ordinary sim engine output produces zero anomaly flags.

## Regeneration

The sim engine produces byte-identical output for a given `(seed, date)`
input (verified by its own determinism test). To refresh this fixture, copy
the three files from a sim engine `output/` tree:

```
output/daily/07/01/2024/store_summary.csv
output/daily/07/01/2024/department_sales.csv
output/dimensions/dim_stores.csv
```

A failure in `test_sim_engine_contract.py` after regeneration means the sim
engine's output changed in a way the ETL must account for — that is the
signal the contract test exists to surface.
