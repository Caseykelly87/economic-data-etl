# ETL Testing Notes

Reference for how this repository's test suite is structured and what "a
good test" means here. Written for engineers extending the suite, and for
the downstream repositories (api, portal) that consume the canonical
parquet artifacts this ETL produces.

This repository is the first consumer in the data pipeline: the simulation
engine produces daily CSVs, this ETL ingests and transforms them into
canonical parquets, and the api serves those parquets. The conventions
below are inherited from the sim engine's `__TESTING_NOTES.md` and extended
for the ETL's position in the pipeline.

## Established patterns

The suite uses plain `pytest` — function-style tests, fixtures defined in
`tests/conftest.py`, `unittest.mock` for I/O isolation in the macro
pipeline, and on-disk CSV fixture trees for the grocery pipeline. No custom
framework, no shared assertion helpers beyond what `pytest` and
`pandas`/`sqlalchemy` provide.

Tests are graded into three categories, the shared platform vocabulary:

- **Business-correctness** — asserts specific values that are computable
  from the inputs independently of the implementation. Example:
  `test_parse_bls_values_parsed_correctly` asserts that the BLS response's
  `M01`/`M02`/`M03` observations land on `312.0`/`313.5`/`314.2` in
  oldest-first order — facts read off the fixture, not off a prior run.
- **Structural** — asserts shape (types, columns, row counts, value
  ranges) but not specific values. Useful as an entry-level floor; not
  sufficient on its own for hot-path code.
- **Ceremony** — runs code but verifies nothing beyond "it did not raise".

Business-correctness is the bar for hot-path code. For an ETL, that has a
specific meaning: a test of a transform must assert the *transformed
value*, computed by hand from the input, not merely that a DataFrame of
the right shape came back. Three techniques recur:

- **Independently-derived expectations.** Compute the expected value from
  the source row. `test_cli_parquet_has_target_schema_and_values` asserts
  `avg_basket_size == 87400.00 / 2300` — the quotient of two columns from
  the input CSV — rather than snapshotting whatever the transform emitted.
- **Invariant cross-checks.** Re-derive a field from a different grain and
  compare. `test_department_transform_and_cross_grain_reconciliation`
  asserts that the sum of a store's department `net_sales` equals that
  store's `net_sales_total` in `store_summary.csv`.
- **Determinism by hashing.** The CLI layers write parquet; repeat runs on
  identical input are SHA-256-compared (`test_cli_repeat_runs_byte_identical`
  and the `sim_cli` equivalents).

## Hot-path tests

The load-bearing logic and the tests that hold it.

**Macro pipeline** (FRED/BLS/ERS APIs → SQLite/Postgres):

- **Extraction idempotency** — `test_extract.py`. SHA-256 revision
  detection (`test_compute_hash_*`), incremental fetch from stored
  metadata (`test_fred_uses_incremental_start_date`), no-change-skips-write
  and change-creates-snapshot for all three sources, retry/backoff scoped
  to network errors only (`test_fetch_with_retry_*`).
- **Transformation** — `test_transform.py`. Value parsing for all three
  sources (`test_parse_fred_values_parsed_correctly`,
  `test_parse_bls_values_parsed_correctly`,
  `test_parse_ers_csv_values_parsed_correctly`), the source-specific
  missing-value quirks (`.` for FRED, `-` for BLS → `NaN`), BLS
  most-recent-first → oldest-first normalisation, ERS category mapping and
  start-year filtering.
- **Loading** — `test_load.py`. Idempotent upsert with inserted/updated/
  unchanged stats, `NaN` persisted as SQL `NULL`, and the
  non-destructiveness of repeat `ensure_tables_exist` calls
  (`test_ensure_tables_is_idempotent` — `CREATE IF NOT EXISTS`, never
  `CREATE OR REPLACE`).
- **Orchestration** — `test_main.py`. Per-phase error isolation (a failed
  extract must stop the run before transform; a failed transform before
  load) and the call contract between `run_pipeline` and each stage.

**Grocery pipeline** (sim engine CSVs → canonical parquets → detection):

- **Ingestion** — `test_sim_ingest.py`, `test_sim_ingest_department.py`.
  The date-tree walker, CSV parsing into typed records, schema validation
  against the `*_REQUIRED_COLUMNS` contracts, and the schema-vs-
  reconciliation error distinction.
- **Store-day transform** — `test_sim_transform.py`. `avg_basket_size`
  computed as `total_sales / transaction_count` with `NaN` for
  zero-transaction days, `labor_cost_pct` blanked on closed days,
  deterministic `(date, store_id)` sort, referential validation against
  `dim_stores`.
- **Department-grain transform** — `test_sim_transform_department.py`.
  Same contract at store-day-department grain, value pass-through, empty
  input treated as a reconciliation failure.
- **Canonical output** — `test_sim_cli.py`. Parquet schema and values,
  byte-identical repeat runs, the always-written `dim_stores.parquet`.
- **Detection** — `test_detect_rules.py`. The five static-band rules
  (`revenue_band`, `labor_pct_band`, `avg_ticket_band`,
  `transactions_band`, `yoy_comp`), severity scoring as
  `distance_from_band / band_half_width`, and each rule's skip
  behavior. The same file covers the `revenue_zscore_28d`
  rolling-baseline rule: it reads `total_sales` per store-day and
  flags days whose `|z|` against the trailing 28-day mean (current
  row excluded) is at least 2.5; the test suite pins the
  hand-derived rolling mean and severity score, the cold-start skip
  (fewer than 14 prior observations), the zero-stddev skip
  (degenerate divide-by-zero guard), and the per-store independence
  of the learned baseline. The same file also covers the
  `department_coverage` structural-integrity rule: it reads the
  department-grain frame and flags store-days whose department row
  count departs from the ten-department baseline or that carry a
  duplicated `department_id`. Two more department-grain rules live in
  the same file: `gross_margin_band` flags a store-day when any of its
  departments has a `gross_margin_pct` outside the configured band
  (asserted with a hand-built high-margin outlier, a negative margin, an
  in-band no-fire, and the worst-department selection when two are out of
  band), and `department_reconciliation` flags a store-day when its
  department `net_sales` sum diverges from the store-grain `total_sales`
  by more than the dollar tolerance (asserted with a balanced no-fire, a
  $100 break that fires, and a sub-dollar break the tolerance absorbs).
- **Anomaly flagging output** — `test_detect_cli.py`,
  `test_detect_integration.py`. The anomalous fixture fires its expected
  `(date, store_id, rule_id)` set; the happy fixture fires nothing; the
  flags parquet is byte-identical across runs. `test_detect_cli.py` also
  exercises the `--department-metrics-path` and `--dim-stores-path`
  inputs that drive the structural rule.

Calendar-dimension joining is listed as a platform hot path but is not
exercised at this layer: `store_daily_metrics` and `department_daily_metrics`
carry a raw `date` column and no fiscal-calendar fields. Any future
calendar join would need its own business-correctness coverage.

## Upstream contract tests

`tests/test_sim_engine_contract.py` pins the contract between the sim
engine's output and the ETL transform. It is the first test file that
exercises the ETL against genuine upstream data rather than scaffolding.

The fixture lives at `tests/fixtures/sim_engine_contract/` — a verbatim
capture of the sim engine's realism-applied output for `2024-07-01` (8
stores, 10 departments each). The sim engine produces byte-identical
output for a given `(seed, date)`, so a captured slice is a stable input.
See that directory's `README.md` for provenance and regeneration steps.

The three tests assert:

- **`test_store_day_transform_produces_expected_values`** — the store-day
  transform yields the exact `total_sales` / `transaction_count` /
  `labor_cost_pct` from the source CSV and the derived `avg_basket_size`.
- **`test_department_transform_and_cross_grain_reconciliation`** — the
  department transform yields the exact per-department values, and each
  store's department `net_sales` sums to its `net_sales_total`.
- **`test_detection_produces_no_flags_on_normal_sim_day`** — a normal
  store-day (empty `anomaly_log`) falls inside every detection band, so
  the rules engine emits zero flags; a perturbation confirms the engine
  is live and the zero-flag result is not vacuous.

A failure here after regenerating the fixture means the sim engine's
output changed in a way the ETL must account for — which is the signal
the contract test exists to surface.

## Structural-integrity detection

Detection began as five statistical-band rules evaluated at store-day
grain: each checks whether a value — revenue, labor percentage, average
ticket, transaction count, year-over-year ratio — falls inside an
expected band. Those rules never read the department-grain frame, so
they cannot see irregularities in the *shape* of that data: a store-day
missing a department's row, or carrying the same `department_id` twice.

The canonical `department_daily_metrics.parquet` carries 52 such
store-days — 39 with nine department rows (one department absent) and 13
with eleven (one department duplicated). These are upstream simulation
engine injections, not an ETL defect, and they pass schema validation
because each individual row is well-formed. The `department_coverage`
rule covers that gap. It evaluates one group per `(date, store_id)` on
the department-grain frame and fires when the row count is not the
configured `expected_row_count` or when a `department_id` repeats.

The contract test for a structural rule has a different shape from the
band-rule contract. A band rule's contract is "values inside the band
produce no flag" (`test_sim_engine_contract.py`). A structural rule's
contract is "the rule fires on the known structural irregularities and
nowhere else": `test_detect_structural_contract.py` reads the committed
canonical, asserts the rule fires on exactly the 52 irregular
store-days, and asserts the missing-department and duplicated-department
cases stay distinguishable by their flagged row count (9 versus 11). It
also pins the regenerated `anomaly_flags.parquet` at 178 rows — 52
`department_coverage` flags, 72 `department_reconciliation` flags, 24
`gross_margin_band` flags, and 30 store-day flags from the value and
rolling rules (18 `transactions_band`, 1 `yoy_comp`, 11
`revenue_zscore_28d`). `revenue_band`, `labor_pct_band`, and
`avg_ticket_band` fire nothing on the canonical once the bands are
widened to the natural-variance envelope.

## Test categories observed

Categorization snapshot taken when the suite held 246 tests. The suite
is now at 287; the 41 added tests since the snapshot have not been
graded under the business/structural/ceremony split, so the table below
is a dated point-in-time record rather than a current breakdown. The
suite at the start of the pass that produced the snapshot held 243
tests.

| Category             | At start | After pass |
|----------------------|----------|------------|
| Business-correctness | 158      | 168        |
| Structural           | 81       | 77         |
| Ceremony             | 4        | 1          |
| Uncategorizable      | 0        | 0          |

That pass converted seven structural/ceremony tests covering hot-path
code into business-correctness tests (three macro-transform value
checks, two grocery-transform checks, the load-idempotency check, the
canonical-output check) and added three contract tests, bringing the
suite to 246. The structural/business split involves judgment at the
margin — a row-count assertion and an exact-value assertion sit close
together — but the direction of travel is what matters: hot-path code
earns value assertions.

## Known weak areas

Tests left as structural or ceremony, with the reason each was not
strengthened:

- `test_observability.py::test_default_invocation_runs_without_error` —
  ceremony. It exercises structlog configuration, which is not a data hot
  path. A candidate for strengthening or removal in a later pass; left
  here, matching the sim engine pass's decision on its analogous test.
- The remaining structural tests are predominantly `returns_dataframe`,
  `expected_columns`, and dtype guards. Most sit directly beside a
  business-correctness test of the same function, so they act as a cheap
  shape floor rather than the primary check. The `*_columns_match_*_schema`
  tests that compare against a `schemas.py` constant are deliberate
  schema-contract guards and are worth keeping as written.
- `test_main.py` orchestration tests that assert only `assert_called_once`
  (without argument matching) are structural. For an orchestrator whose
  job is wiring, "the stage was invoked exactly once" is a legitimate
  contract; the argument-level assertions in the same file carry the
  business-correctness weight.

No production bugs were discovered while strengthening the targeted
tests. Every strengthened test passes against the current code.

## For downstream consumers

The api and portal repositories consume this ETL's canonical parquets and
should carry the same conventions:

- **The canonical parquet is a stable test fixture.** `sim_cli` and
  `detect_cli` produce byte-identical parquet output for identical input
  (verified here by SHA-256 comparison). Downstream repos can commit a
  canonical parquet and treat it as a fixed input, the same way this repo
  treats captured sim engine output.
- **Contract testing works layer by layer.** This repo pins the sim engine
  → ETL contract with `test_sim_engine_contract.py`. The api repo should
  pin the ETL → api contract the same way: capture a canonical parquet,
  run it through the api's read path, assert specific served values. The
  contract is the fixture; a stale-fixture failure is the signal.
- **Business-correctness means independently-derived expectations.** A
  test that asserts a served or aggregated value should compute the
  expectation from the input parquet by hand or from a spec, not snapshot
  whatever the code currently returns. A snapshot is a regression guard,
  not a correctness test.
- **Exclude timestamped artifacts from byte comparisons.** The sim engine's
  `manifest.json` carries a wall-clock field and is excluded from its
  determinism check. Any downstream byte-identical assertion must compare
  data files and exclude metadata that records run time.
- **The three-category vocabulary** — business-correctness, structural,
  ceremony — is the shared language for grading test strength across the
  platform. Hot-path code earns business-correctness tests; structural
  coverage is acceptable only for non-load-bearing surfaces.
