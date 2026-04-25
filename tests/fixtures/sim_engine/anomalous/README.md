# Anomalous sim engine fixture

Mirrors the layout of `happy/` but injects three deliberate anomalies on
`2024-06-16` so phase 2 detection has fixed inputs to assert against.
`2024-06-15` and `2024-06-17` are normal store-days at the standard
baseline factor (`base_daily_revenue * 0.92`).

## Injections (all on 2024-06-16)

| store_id | profile          | injection                              | rules expected to fire                |
|----------|------------------|----------------------------------------|---------------------------------------|
| 1        | suburban-family  | net_sales 28000 (≈30% of 95K base)     | revenue_band, transactions_band       |
| 4        | urban-dense      | labor_cost_pct 0.22 (band 0.065–0.165) | labor_pct_band                        |
| 7        | value-market     | transactions 860 (≈half of normal)     | transactions_band, avg_ticket_band    |

## Why store 1 fires two rules

Dropping revenue by 70% with the basket size held near profile-typical
(≈$38) requires transactions to drop in the same proportion. That is the
realistic shape of a low-revenue day, and both bands fire together —
which is the right business signal. The integration test asserts both
flags are present rather than narrowing the injection to fire exactly
one band.

## Why store 7 fires two rules

Halving transactions while keeping net sales at the normal level forces
the average basket size to roughly double. The transactions band
catches the count anomaly; the basket band catches the implied per-txn
inflation. Again, two flags from one root-cause anomaly is the correct
shape — distinct symptoms of the same underlying problem.

## Total expected flag count

Five rows on `2024-06-16`, zero on `2024-06-15` and `2024-06-17`.
`yoy_comp` is silently skipped — the fixture covers only three days in
2024 with no T-365 reference data.
