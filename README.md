# Economic Data ETL

[![Tests](https://github.com/Caseykelly87/economic-data-etl/actions/workflows/test.yml/badge.svg)](https://github.com/Caseykelly87/economic-data-etl/actions/workflows/test.yml)

A Python ETL repository containing two pipelines that share infrastructure but address different data domains:

- **Macro pipeline** — ingests 23 monthly U.S. macroeconomic series from FRED and BLS (including the food-category CPI indexes the sim engine's realism layer consumes), plus 8 annual food-price forecast series from USDA ERS, normalizes them into a tidy long-format schema, lands them in a `raw` zone, and builds the `staging` and domain-mart layers (`public_analytics.mart_*`) the API serves.
- **Grocery pipeline** — ingests CSV output from the upstream [simulation engine](https://github.com/Caseykelly87/Knot-shore-grocery-simulation-engine), validates schemas, applies detection rules, and produces canonical parquet artifacts that downstream API and portal repositories consume.

Both pipelines share configuration, structured logging, and CI. The test suite covers both, with no live API calls or database connections.

## Table of contents

- [Where this fits in the platform](#where-this-fits-in-the-platform)
- [Quick start](#quick-start)
- [The macro pipeline](#the-macro-pipeline)
- [The grocery pipeline](#the-grocery-pipeline)
  - [Simulation engine ingestion](#simulation-engine-ingestion)
  - [Exception detection](#exception-detection)
  - [Canonical pipeline fixtures](#canonical-pipeline-fixtures)
- [Project structure](#project-structure)
- [Exception classes](#exception-classes)
- [Logging](#logging)
- [Testing](#testing)
- [Adjacent repositories](#adjacent-repositories)

## Where this fits in the platform

The platform's deployed portal is at https://knot-shore-portal.vercel.app (offline mode, bundled fixtures); the full-stack technical demo is the orchestration repo at https://github.com/Caseykelly87/knot-shore-platform.

The grocery pipeline sits between the upstream sim engine and the downstream API + portal:

```
knot-shore-grocery-simulation-engine    →    economic-data-etl    →    economic-data-api    →    knot-shore-portal
upstream csv generator                       this repo                  service layer              dashboards
                                            (grocery side)                                          + docs hub
```

The grocery side reads the sim engine's CSV output tree, transforms into canonical parquet artifacts, and writes them to `data/processed/canonical/`. Those artifacts are byte-identically copied into the API repo's bundled fixtures (`app/fixtures/`) so a clone-and-run demo of the API works without re-running this pipeline. Reader-grade documentation for the ETL — source-adapter / transform separation, detection rules, canonical fixture flow — lives at the portal's [`/about/etl`](https://github.com/Caseykelly87/knot-shore-portal) page.

The macro pipeline is independent of the grocery side. It connects to FRED, BLS, and ERS sources directly and writes to SQLite by default; Postgres is supported via `DATABASE_URL`. A side dependency: the sim engine's optional Stage 2 (the "realism layer") reads from the same database, so when the sim engine runs with realism enabled, the macro pipeline must have populated it first.

## Quick start

### Prerequisites

- Python 3.12+
- A free FRED API key: register at https://fred.stlouisfed.org/docs/api/fred/v2/index.html
- A free BLS API key: register at https://data.bls.gov/registrationEngine/

### Install

```bash
git clone <this-repo-url>
cd economic-data-etl

python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux

pip install -r requirements.txt
```

### Configure

Create a `.env` file in the project root (git-ignored — never commit):

```ini
FRED_API_KEY=your_fred_api_key_here
BLS_API_KEY=your_bls_api_key_here

# Optional — defaults to SQLite at data/economic_data.db
# DATABASE_URL=postgresql://user:password@localhost:5432/economic_data
```

### Run the macro pipeline

```bash
python -m src.main
```

Produces raw JSON snapshots under `data/raw/` and upserts to the configured database.

### Run the grocery pipeline

```bash
# Stage 1: ingest the sim engine's csv output and produce canonical parquets
python -m src.sim_cli \
  --input-root /path/to/sim/engine/output \
  --output-dir data/processed

# Stage 2: apply detection rules to the store-day metrics
python -m src.detect_cli \
  --metrics-path data/processed/store_daily_metrics.parquet \
  --sim-output-root /path/to/sim/engine/output \
  --rules-path config/detection_rules.yaml \
  --output-dir data/processed
```

### Run all tests

```bash
python -m pytest -q                            # full suite, no live api or db calls
python -m pytest --cov=src                     # with coverage
python -m pytest tests/test_detect_rules.py    # single file
```

## The macro pipeline

Three-stage flow: extract → transform → load. Implemented in `src/main.py`, `src/extract.py`, `src/transform.py`, `src/load.py`. Run via `python -m src.main`.

### Extract

Fetches each FRED series individually (10 calls) and all BLS series in a single batch request (1 call). USDA ERS data is fetched as a CSV summary file. Each response is SHA-256 hashed; files are only written when the response content has changed, so re-running is fully idempotent. State per series (last observation date, response hash) is tracked under `data/metadata/`.

Resilient networking: exponential backoff on transient HTTP errors, 3 retry attempts.

### Transform

Normalizes raw API responses into typed pandas DataFrames. Source-specific missing-value encodings (`"."` for FRED, `"-"` for BLS) are coerced to `NaN`. Output is a long-format fact table (`series_id`, `observation_date`, `value`) plus a series dimension table (`series_id`, `series_name`, `source`).

### Load

Upserts fact and dimension rows into the `raw` zone via SQLAlchemy. Each row is classified as inserted (new), updated (revision detected), or unchanged (skipped) based on primary key and value comparison. The pipeline reports `{"inserted": N, "updated": N, "unchanged": N}` on every run. Defaults to SQLite at `data/economic_data.db`; swap to Postgres by setting `DATABASE_URL` — no code changes needed.

### Staging and marts

After the raw upsert, the load stage builds the rest of the layered warehouse (raw → staging → marts) in `src/marts.py`:

- **`staging.stg_economic_observations`** conforms the raw landing — it carries `series_id`, `series_name`, `value`, and `source`, and exposes the raw text `date` as a typed `observation_date`. It is materialized as a table rather than a view because SQLite (the test dialect) cannot define a view in one attached database that references a table in another; materializing keeps one code path that the SQLite suite exercises identically to Postgres. The whole chain is a deterministic full refresh on every run, so a materialized staging table is always fresh.
- **`public_analytics.mart_inflation` / `mart_labor_market` / `mart_gdp`** project staging filtered to each domain's series — the subsets the API serves at `/metrics/inflation`, `/metrics/unemployment`, and `/metrics/gdp`. The series → domain routing lives in `MART_DOMAINS` in `src/config.py`, keyed by technical `series_id`.
- **`public_analytics.mart_economic_summary`** carries the latest observation (`latest_date`, `latest_value`) for every ingested series. It is the no-loss rollup: series that map to no domain mart (consumption, sentiment, the savings and fed-funds rates, the Missouri grocery-sales series, the ERS food-price forecasts) still reach the serving layer here. The API reads it at `/insights/summary`.

The marts build is a full refresh (delete then insert-select from staging), so it is idempotent and safe to re-run with no duplicate rows. Column names and primary keys match the API's SQLAlchemy mart models exactly, so the API reads the produced tables without a schema change.

### Series catalog

#### FRED series (10 indicators)

| Key | Series ID | Description |
|---|---|---|
| `PCE_NOMINAL` | `PCEC` | Personal Consumption Expenditures |
| `PCE_REAL` | `PCECC96` | Real PCE (inflation-adjusted) |
| `RETAIL_SALES` | `RSXFS` | Advance Retail Sales (excl. food) |
| `SENTIMENT` | `UMCSENT` | University of Michigan Consumer Sentiment |
| `CPI_ALL` | `CPIAUCSL` | Consumer Price Index, All Urban Consumers |
| `GDP_REAL` | `GDPC1` | Real Gross Domestic Product |
| `UNRATE` | `UNRATE` | Unemployment Rate |
| `SAVINGS_RATE` | `PSAVERT` | Personal Saving Rate |
| `MONEY_COST` | `FEDFUNDS` | Federal Funds Effective Rate |
| `GROCERY_SALES_MO` | `MSRSMO445` | Missouri Food & Beverage Stores retail sales, YoY % change (NAICS 445) |

#### BLS series (13 indicators, batch request)

| Key | Series ID | Description |
|---|---|---|
| `CPI_URBAN` | `CUUR0000SA0` | Headline CPI — All Urban Consumers |
| `CPI_CORE` | `CUUR0000SA0L1E` | Core CPI (excludes food and energy) |
| `GAS_PRICE` | `APU000074714` | Average retail price: gasoline |
| `AVG_WAGES` | `CES0500000003` | Average Hourly Earnings, All Employees |
| `WAGE_INDEX` | `CIU2020000000000I` | Employment Cost Index |
| `ERS_ALL_FOOD` | `CUUR0000SAF1` | CPI: Food |
| `ERS_FOOD_HOME` | `CUUR0000SAF11` | CPI: Food at home |
| `ERS_FOOD_AWAY` | `CUUR0000SEFV` | CPI: Food away from home |
| `ERS_CEREALS` | `CUUR0000SAF111` | CPI: Cereals and bakery products |
| `ERS_MEATS` | `CUUR0000SAF112` | CPI: Meats, poultry, fish, and eggs |
| `ERS_DAIRY` | `CUUR0000SEFJ` | CPI: Dairy and related products |
| `ERS_FRUITS_VEG` | `CUUR0000SAF113` | CPI: Fruits and vegetables |
| `ERS_BEVERAGES` | `CUUR0000SAF114` | CPI: Nonalcoholic beverages |

The eight food-category CPI series carry an `ERS_` prefix even though the data comes from BLS: the sim engine's realism layer queries the database by these exact `series_name` values, and the names predate the platform's switch from the USDA ERS Food Price Outlook to the underlying BLS monthly indexes. The realism layer's multiplier math needs monthly index levels, which is what these supply.

#### USDA ERS food-price forecasts (8 series)

The USDA Economic Research Service publishes a monthly Food Price Outlook CSV with *annual* year-over-year CPI forecasts across eight food categories — a different granularity from the monthly CPI indexes above, which is why these live under distinct `ERS_FORECAST_*` names. `ERS_CATEGORY_MAP` in `src/config.py` maps the CSV's category strings to the internal `series_id` values stored in `dim_series` and `fact_economic_observations`:

| Series ID | Source category (CSV) |
|---|---|
| `ERS_FORECAST_ALL_FOOD` | `All food` |
| `ERS_FORECAST_FOOD_HOME` | `Food at home` |
| `ERS_FORECAST_FOOD_AWAY` | `Food away from home` |
| `ERS_FORECAST_CEREALS` | `Cereals and bakery products` |
| `ERS_FORECAST_MEATS` | `Meats, poultry, and fish` |
| `ERS_FORECAST_DAIRY` | `Dairy products` |
| `ERS_FORECAST_FRUITS_VEG` | `Fruits and vegetables` |
| `ERS_FORECAST_BEVERAGES` | `Nonalcoholic beverages and beverage materials` |

The CSV is loaded as a single batch — `ERS_SUMMARY_URL` in `src/config.py` points at the food-price-outlook landing page, and `src/extract.py` discovers the current monthly CSV URL by scraping that page (ERS rotates the media ID on every publication; a hard-coded fallback URL in `extract.py` covers discovery failures).

To add or remove series, edit `FRED_SERIES`, `BLS_SERIES`, or `ERS_CATEGORY_MAP` in `src/config.py`. No other files need to change.

## The grocery pipeline

### Simulation engine ingestion

A second ingest path, independent of the macro FRED/BLS/ERS pipeline above, consumes daily output from a grocery-chain simulation engine and produces three parquet artifacts: `store_daily_metrics.parquet` at the store-day grain, `department_daily_metrics.parquet` at the store-day-department grain, and `dim_stores.parquet` carrying the 8-store reference dimension. All three are the canonical input for downstream exception detection and the API.

**Operational context.** In the operational world this platform models, eight store managers record daily numbers in a shared Google Doc. For the simulation phase the sim engine emits the same granularity as a Google Sheets export would, and this ETL reads from the sim engine's local output tree. A Google Sheets transport is deliberately out of scope for this phase; the source adapter (`src/sim_ingest.py`) is structured so that substituting one is a drop-in replacement with no change to the transform layer or CLI.

The source-adapter / transform separation is the basic discipline of pipeline engineering. `sim_ingest.py` knows everything about the sim engine's CSV format — column names, type coercion rules, the directory walk pattern — and produces typed records. `sim_transform.py` knows nothing about CSV; it consumes typed records and produces canonical DataFrames. If the sim engine ever changed its output format, only `sim_ingest.py` would need updating.

**Input tree shape (consumed):**

```
output/
├── daily/{YYYY}/{MM}/{DD}/store_summary.csv
├── daily/{YYYY}/{MM}/{DD}/department_sales.csv
└── dimensions/dim_stores.csv
```

**Explicitly NOT consumed:** `anomaly_log.csv` (the sim engine's QA artifact). Detection has zero awareness of it — see [Exception detection](#exception-detection) below.

**Run the ingest:**

```bash
python -m src.sim_cli \
  --input-root /path/to/sim/engine/output \
  --output-dir data/processed
```

Outputs three parquet files at `data/processed/`. Re-running with the same input produces byte-identical output.

### Exception detection

A library of detection rules in `src/detect_rules.py`, with thresholds declared in `config/detection_rules.yaml`. The CLI `src/detect_cli.py` applies the rules to the canonical metrics parquets and writes `data/processed/anomaly_flags.parquet`.

```bash
python -m src.detect_cli \
  --metrics-path data/processed/store_daily_metrics.parquet \
  --department-metrics-path data/processed/department_daily_metrics.parquet \
  --sim-output-root /path/to/sim/engine/output \
  --rules-path config/detection_rules.yaml \
  --output-dir data/processed
```

Output: `data/processed/anomaly_flags.parquet` with one row per fired rule, sorted deterministically by `(date, store_id, rule_id)`. Schema is exactly `(date, store_id, rule_id, actual_value, expected_low, expected_high, distance_from_band, severity_score, severity_level)`.

#### The nine rules

Five statistical-band rules check whether a store-day value sits inside an expected band:

| rule_id | Checks | Band |
|---|---|---|
| `revenue_band` | `total_sales` vs `base_daily_revenue` | ± 60% |
| `labor_pct_band` | `labor_cost_pct` vs profile center | ± 5pp around profile center |
| `avg_ticket_band` | `avg_basket_size` vs profile center | ± 20% around profile center |
| `transactions_band` | `transaction_count` vs `base / avg_ticket_center` | ± 45% |
| `yoy_comp` | current/T-365 sales ratio | ratio outside `[0.55, 1.40]` |

The revenue, transactions, and year-over-year widths are set from the measured natural-variance envelope of the canonical negative universe rather than round numbers — daily revenue and transaction counts swing by roughly ±60% across the two-year window, so a tighter band floods false positives on ordinary high-traffic days. `avg_ticket_band` keeps ± 20% because basket size barely varies day to day and the wider value would only dull a useful signal.

Per-profile centers (from the live sim engine seed): `suburban-family` → labor 0.105, ticket $38.00; `urban-dense` → labor 0.115, ticket $28.00; `value-market` → labor 0.120, ticket $32.00.

The sixth rule, `revenue_zscore_28d`, is a rolling-baseline rule rather than a static band. For each store-day it computes the trailing 28-day mean and stddev of `total_sales` (current row excluded, the same shape `yoy_comp` uses against T-365) and flags any day whose `|z|` is at least 2.5. Severity reads `|z|` directly: `info` 2.5–3, `warning` 3–4, `critical` ≥ 4. Cold-start rows with fewer than 14 prior observations skip silently, as do rows whose rolling stddev is zero (z-score is undefined when the recent history is constant). The rule complements the static bands: static bands catch sharp one-day excursions from the configured baseline; the rolling rule catches days that sit inside the static band but well outside the store's recent distribution.

The seventh rule, `department_coverage`, is a structural-integrity rule rather than a band. It evaluates the department-grain metrics one group per `(date, store_id)` and flags any store-day whose department row count is not 10, or that carries a duplicated `department_id`.

The eighth rule, `gross_margin_band`, also reads the department-grain frame. Gross margin lives only at department grain, so the rule groups by `(date, store_id)` and fires a store-day when any of its departments has a `gross_margin_pct` outside `0.385 ± 0.235` (i.e. `[0.15, 0.62]`). One flag per store-day carries the single most extreme department; severity reuses the band ladder. Aggregating margin to the store-day level first does not work — one department swinging to a 0.95 margin barely moves the sales-weighted store mean — so the outlier has to be caught at the grain it occurs on.

The ninth rule, `department_reconciliation`, is the only rule that reads both grains. It checks that a store-day's department `net_sales` sum equals the store-grain `total_sales`; the sim engine derives the store total from department detail, so in clean data the two agree to floating-point precision. A store-day fires when the absolute difference exceeds a $1.00 tolerance — wide enough to absorb currency rounding, tight enough to catch the injected integrity breaches (the smallest moves a department by ~$50).

#### Severity

For the five static-band rules, `severity_score = distance_from_band / band_half_width`. Values are bucketed into `info` (score ≤ 1), `warning` (1 < score ≤ 2), and `critical` (> 2). `gross_margin_band` reuses the same ladder against its margin band. For `revenue_zscore_28d`, `severity_score` is `|z|` itself and the bucket cutoffs are 2.5–3 / 3–4 / ≥ 4 (info / warning / critical). The structural `department_coverage` and `department_reconciliation` rules emit a fixed `severity_level` (default `warning`) instead of a graded score. Closed-day rows (`total_sales == 0`) are skipped by `labor_pct_band`, `avg_ticket_band`, and `transactions_band`. `yoy_comp` is silently skipped when no T-365 row is present.

#### Grain

Detection runs at two grains. The five statistical-band rules and the `revenue_zscore_28d` rolling-baseline rule evaluate `store_daily_metrics` at store-day grain. The `department_coverage`, `gross_margin_band`, and `department_reconciliation` rules read `department_daily_metrics` at department grain, grouping one group per `(date, store_id)` to emit store-day flags; `department_reconciliation` additionally reads the store-day frame to compare the two grains. Department-grain issues — a missing or duplicated department row, a margin outlier, or department sales that don't reconcile to the store total — are caught here and flagged in `anomaly_flags.parquet`.

There is no day-of-week or seasonal adjustment in the current value bands. Their widths are set from the measured natural-variance envelope so weekend and holiday peaks don't flood false positives; that is why the revenue and transaction bands are wide. A future seasonal-baseline rule could produce empirical per-store-date expected values that a rule refactor would consume in place of the static bands, tightening them considerably.

#### Evaluating detection quality

`scripts/evaluate_detection.py` measures recall and false-positive rate against the sim engine's `anomaly_log.csv` ground-truth file. The script lives outside the `src/` package and is the only piece of code in this repository permitted to read that log — detection itself has no knowledge of it. Not collected by pytest; not imported by any `src/` module.

```bash
python scripts/evaluate_detection.py \
  --flags-path data/processed/anomaly_flags.parquet \
  --anomaly-log-path /path/to/sim/engine/output/anomaly_log.csv \
  --metrics-path data/processed/store_daily_metrics.parquet
```

Reports global recall, per-anomaly-type recall, and FPR. The contract: `global_recall ≥ 0.35` AND `fpr ≤ 0.10`. The script prints a `PASS` / `FAIL` verdict and exits non-zero on `FAIL`, so it can be wired into CI independently if desired.

The boundary matters: code in `src/` has no awareness that ground-truth labels exist. Detection is a real measurement, not a lookup against the answer key.

### Canonical pipeline fixtures

Four canonical parquet artifacts and one JSON measurement artifact produced by running the sim engine + ETL pipeline end-to-end are committed at `data/processed/canonical/`:

| File | Rows × Cols | Notes |
|---|---:|---|
| `store_daily_metrics.parquet` | 5,848 × 6 | 8 stores × 731 days (two full calendar years) |
| `department_daily_metrics.parquet` | 58,424 × 7 | Same window across 10 departments per store-day |
| `dim_stores.parquet` | 8 × 10 | One row per store with identification, location, and base_daily_revenue |
| `anomaly_flags.parquet` | 343 × 9 | One row per fired detection rule per store-day |
| `detection_quality.json` | — | Recall, false-positive rate, per-anomaly-type recall, and the phase 2 contract verdict measured against the sim engine's ground-truth `anomaly_log.csv`. |

**`store_daily_metrics.parquet`** spans the canonical two-year window, 2024-01-01 through 2025-12-31 (731 days: 2024 is a leap year), covering all 8 stores. 2025 is the demo year surfaced by the dashboard; the full prior year enables year-over-year comparison views consumed by the portal's store drilldown via the API's existing `start_date` / `end_date` query parameters. Filtering this parquet to 2025 yields 2,920 rows. Columns: `date`, `store_id`, `total_sales`, `transaction_count`, `avg_basket_size`, `labor_cost_pct`.

**`department_daily_metrics.parquet`** spans the same window across all 8 stores and all 10 departments. A complete tree would hold 58,480 rows (8 × 10 × 731); the actual count is 58,424 because the sim engine injects data-integrity anomalies into its own output — 83 store-days are missing a department row and 27 carry a duplicated one (−83 +27 = −56). Those irregularities are deliberate: they are part of the ground truth the detection layer is measured against. Columns: `date`, `store_id`, `department_id`, `net_sales`, `transactions`, `units_sold`, `gross_margin_pct` (preserved as a fraction; the portal's display layer handles percent formatting). Rows are sorted by `(date, store_id, department_id)`.

**`dim_stores.parquet`** is the canonical store reference dataset: 8 rows in `store_id` order. Columns: `store_id`, `store_name`, `address`, `city`, `zip`, `county_fips`, `trade_area_profile`, `sqft`, `open_date`, `base_daily_revenue`. Only `store_id` is type-coerced (to `int64`); other columns pass through as pandas reads them — `zip`, `county_fips`, `sqft` are `int64`; `open_date` is a string in `YYYY-MM-DD` form; `base_daily_revenue` is `float64`.

**`anomaly_flags.parquet`** is the `detect_cli` output: the five statistical-band rules and `revenue_zscore_28d` run against the store-day metrics, while `department_coverage`, `gross_margin_band`, and `department_reconciliation` run against the department metrics. The `yoy_comp` rule fires only where a prior-year baseline exists (all of 2025, since 2024 is fully covered), and `revenue_zscore_28d` fires only after a store has at least 14 prior observations. Of the 343 rows, 251 are department-grain flags (110 `department_coverage`, 141 `department_reconciliation`), 48 are `gross_margin_band` flags, and 44 are store-day value-and-rolling flags (22 `transactions_band`, 2 `yoy_comp`, 20 `revenue_zscore_28d`). With the value bands widened to the natural-variance envelope, `revenue_band`, `labor_pct_band`, and `avg_ticket_band` fire nothing on the canonical.

**`detection_quality.json`** is the output of `scripts/evaluate_detection.py` against the canonical parquets and the sim engine's ground-truth anomaly log. It captures global recall, false-positive rate, per-anomaly-type recall, and the counts behind them in a stable shape downstream consumers can render directly — the API exposes it at `/insights/detection-quality` and the portal renders the verdict on an about-page. The script that produces these numbers is isolated from `src/` by social contract: only `scripts/evaluate_detection.py` reads the ground-truth log, so the JSON is a real measurement rather than an answer-key lookup.

These five files are the authoritative downstream input for the `economic-data-api` repo's bundled fixtures. The API copies them byte-identically into `app/fixtures/` so a clone-and-run demo of the API works without re-running this pipeline. Committing them to git makes the canonical state visible in PR diffs and reproducible across clones without requiring downstream consumers to install and run the sim engine.

#### Regeneration workflow

The canonical fixtures are regenerated only when the underlying sim engine output deliberately changes. The workflow:

1. **Run the sim engine's `backfill` command** in its repository, producing a fresh `output/` directory with the desired window:

   ```bash
   python -m knot_shore backfill --output ./output
   ```

2. **Run `scripts/build_canonical_fixtures.py`** from this repo's root, pointing at the sim engine output:

   ```bash
   python scripts/build_canonical_fixtures.py \
       --sim-output-root /path/to/sim/engine/output \
       --output-dir data/processed/canonical/
   ```

   The script orchestrates `sim_cli` followed by `detect_cli` via subprocess, writes all four parquets to `--output-dir`, then invokes `evaluate_detection.py` to add `detection_quality.json` to the same directory. A failing contract verdict is logged as a warning but does not fail the build — the JSON artifact reflects whatever the current detection layer produces.

3. **Verify** the resulting parquets visually (date range, row counts, columns) and confirm byte-determinism by re-running the script to a temp directory and comparing SHA-256 hashes.

4. **Commit** the regenerated parquets with a message documenting why the canonical state changed (sim engine window shift, detection rule update, schema change, etc.).

#### Gitignore posture

Ad-hoc parquet output produced by running `sim_cli` or `detect_cli` directly (e.g. into `data/processed/store_daily_metrics.parquet`) remains gitignored — only the `canonical/` subdirectory is tracked. The `.gitignore` walks the `data/` tree one segment at a time so the canonical re-inclusion takes effect (git cannot un-ignore a file inside a fully-ignored parent directory).

## Project structure

```text
.
├── data/                       # Git-ignored except for processed/canonical/
│   ├── metadata/               # Per-series extraction state for the macro pipeline
│   ├── processed/
│   │   └── canonical/          # Committed canonical parquet artifacts
│   └── raw/                    # Immutable raw JSON snapshots from FRED/BLS/ERS
├── config/
│   └── detection_rules.yaml    # Threshold declarations for the detection rules
├── scripts/
│   ├── build_canonical_fixtures.py    # Regenerate canonical parquets end-to-end
│   ├── evaluate_detection.py          # Measure detection recall/fpr against ground truth
│   └── download_food_atlas.py         # Helper for ERS food atlas data
├── src/
│   ├── __init__.py
│   ├── config.py               # Macro pipeline series IDs, paths, env var loading
│   ├── exceptions.py           # SimIngestError, SchemaValidationError, etc.
│   ├── extract.py              # Macro pipeline FRED/BLS/ERS clients with idempotency
│   ├── transform.py            # Macro pipeline DataFrame normalization
│   ├── load.py                 # Macro pipeline raw schema + upsert
│   ├── marts.py                # Macro pipeline staging + domain marts build
│   ├── main.py                 # Macro pipeline entry point
│   ├── observability.py        # Shared structlog configurator with stdlib bridge
│   ├── schemas.py              # Typed records for the grocery-side ingestion
│   ├── sim_ingest.py           # Grocery-side source adapter (csv-aware)
│   ├── sim_transform.py        # Grocery-side transforms (csv-agnostic)
│   ├── sim_cli.py              # Grocery-side cli: ingest -> canonical parquets
│   ├── detect_rules.py         # Detection rule implementations
│   └── detect_cli.py           # Grocery-side cli: canonical -> anomaly flags
├── tests/
│   ├── conftest.py
│   ├── test_extract.py                    # Macro extract + idempotency
│   ├── test_transform.py                  # Macro transform + edge cases
│   ├── test_load.py                       # Schema, upsert, idempotency
│   ├── test_marts.py                      # Staging/marts build, routing, contract
│   ├── test_main.py                       # Macro pipeline orchestration
│   ├── test_observability.py              # Shared logging configurator
│   ├── test_realism_series_contract.py    # Macro catalog supplies the sim realism series
│   ├── test_sim_ingest.py                 # Store-grain csv adapter
│   ├── test_sim_ingest_department.py      # Department-grain csv adapter
│   ├── test_sim_transform.py              # Store-grain transforms
│   ├── test_sim_transform_department.py   # Department-grain transforms
│   ├── test_sim_cli.py                    # Grocery cli orchestration
│   ├── test_sim_integration.py            # End-to-end ingest happy path
│   ├── test_sim_engine_contract.py        # Sim engine output contract
│   ├── test_detect_rules.py               # Rule logic + edge cases
│   ├── test_detect_cli.py                 # Detect cli orchestration
│   ├── test_detect_integration.py         # End-to-end detection
│   ├── test_detect_structural_contract.py # Structural-integrity rule contract
│   └── test_build_canonical_fixtures.py   # Canonical regeneration script
├── .env                        # API keys — never commit
├── .gitignore
├── pytest.ini
├── README.md
└── requirements.txt
```

## Exception classes

The repo defines two exception hierarchies in `src/exceptions.py`:

**Sim engine ingestion failures** — all inherit from `SimIngestError`:

- `SimIngestError` — base class, accepts `message` plus arbitrary `**context` kwargs (path, column, store_id, etc.) that are stored on the instance and echoed into `str(exc)` for log clarity.
- `SchemaValidationError` — required column missing, unparseable type, referential violation (e.g. a `store_id` in `store_summary` absent from `dim_stores`).
- `ReconciliationError` — file presence or row count mismatches: a walked date directory missing its `store_summary.csv`, no date directories found under `daily/`, output row count not equal to sum of input rows.

**Detection failures** — inherit from `DetectionError`:

- `DetectionError` — base class, mirrors `SimIngestError`'s `message + **context` pattern.
- `DetectionConfigError` — malformed `detection_rules.yaml` (missing required field, unknown rule type, out-of-range threshold).
- `DetectionInputError` — input parquet schema violation (e.g. missing column expected by a rule).

The catch-all pattern is to handle each base class once: a top-level handler catches `SimIngestError` to log + exit non-zero from any ingestion call site, and `DetectionError` for any detection call site. The subclasses give callers the option of more granular handling when useful.

## Logging

Pipeline runs emit structured logs via [structlog](https://www.structlog.org/). The configurator lives at `src/observability.py` — single `configure_logging()` entry point called once at startup from each CLI (`main.py`, `sim_cli.py`, `detect_cli.py`). Structlog and stdlib loggers share the same renderer and level filter through structlog's stdlib bridge, so calls like `logging.info("foo", extra={"k": "v"})` propagate the structured fields through to the rendered output.

Output is human-readable colored text when stdout is a tty, single-line JSON otherwise. Format and verbosity are controlled by environment variables:

| Variable | Values | Default |
|---|---|---|
| `LOG_LEVEL` | `debug`, `info`, `warning`, `error`, `critical` | `info` |
| `LOG_FORMAT` | `console`, `json` | auto (console if tty, else json) |

Console output:

```
2026-05-02 14:55:44 [info     ] parquet_written                row_count=2944 output_path=data/processed/store_daily_metrics.parquet
```

JSON output:

```json
{"row_count": 2944, "output_path": "data/processed/store_daily_metrics.parquet", "event": "parquet_written", "level": "info", "timestamp": "2026-05-02T19:55:44.123456Z"}
```

Debug a failing run:

```bash
LOG_LEVEL=debug python -m src.sim_cli --input-root /path/to/sim/output --output-dir data/processed
```

Capture structured logs for offline analysis:

```bash
LOG_FORMAT=json python -m src.sim_cli --input-root /path/to/sim/output --output-dir data/processed > run.log
```

### Macro pipeline structured fields

The macro pipeline (`src/main.py`, `src/extract.py`) emits the same prose messages you see in console output, plus structured fields in JSON mode:

| Field | Values |
|---|---|
| `source` | `fred`, `bls`, `ers`, `pipeline`, `http_client` |
| `status` | `updated`, `skipped`, `failed`, `discovered`, `discovery_failed`, `using_fallback` |
| `stage` | `startup`, `extract`, `transform`, `load` (pipeline-level only) |
| `series_id` | FRED series identifier (FRED extraction events) |
| `error`, `error_type` | error message and exception class (error-path events only) |

Use these to filter logs in JSON mode without parsing the prose:

```bash
LOG_FORMAT=json python -m src.main 2>&1 | jq 'select(.source == "fred" and .status == "updated")'
```

The grocery-side pipelines emit similarly structured fields — `pipeline_started`, `parquet_written`, `rule_evaluated`, etc. — but without a top-level `source` discriminator since each grocery-side run targets one source.

### Windows note

On Windows, stdout defaults to cp1252 encoding which can't render some non-ASCII characters used in pre-existing log strings. The `observability.py` configurator reconfigures stdout to utf-8 with `errors="replace"` to avoid `UnicodeEncodeError` when output is piped or redirected.

## Testing

```bash
python -m pytest -q                              # full suite
python -m pytest -v                              # verbose
python -m pytest --cov=src                       # with coverage
python -m pytest tests/test_detect_rules.py      # single file
```

The test suite makes no live API calls and opens no live database connections. All file I/O is redirected to temporary directories via fixtures in `tests/conftest.py`. The load layer tests use an in-memory SQLite engine and execute real SQL queries.

Coverage emphasizes the boundary contracts:

- **Macro pipeline:** idempotency (response hashing prevents redundant writes), retry/backoff on transient errors, source-specific missing-value coercion, upsert correctness against the in-memory SQLite.
- **Grocery pipeline:** schema validation rejects malformed input with descriptive errors; the source adapter and transform are isolated (transform tests use synthetic typed records, not CSV fixtures); the canonical fixture builder produces byte-identical output across successive runs.
- **Detection rules:** edge cases per rule (closed days skipped where appropriate, `yoy_comp` silently skipped on missing T-365 row), severity bucketing thresholds, end-to-end detection via integration tests using committed fixtures.

The recall and false-positive contracts (`recall ≥ 0.35`, `fpr ≤ 0.10`) are enforced by `scripts/evaluate_detection.py`, which runs against the sim engine's ground-truth `anomaly_log.csv` — the only file in this repo that may read it.

## Adjacent repositories

- [`knot-shore-grocery-simulation-engine`](https://github.com/Caseykelly87/Knot-shore-grocery-simulation-engine) — produces the CSV input the grocery-side ingestion consumes; reads from this ETL's macro database when its Stage 2 realism layer runs.
- [`economic-data-api`](https://github.com/Caseykelly87/economic-data-api) — serves the canonical parquet artifacts as JSON via FastAPI.
- [`knot-shore-portal`](https://github.com/Caseykelly87/knot-shore-portal) — Next.js 14 application with three primary dashboards and the platform's `/about/*` documentation hub. The reader-grade narrative for this ETL repo lives at [`/about/etl`](https://github.com/Caseykelly87/knot-shore-portal/blob/main/app/about/etl/page.tsx).
