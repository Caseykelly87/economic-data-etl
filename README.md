# Economic Data ETL

[![Tests](https://github.com/Caseykelly87/economic-data-etl/actions/workflows/test.yml/badge.svg)](https://github.com/Caseykelly87/economic-data-etl/actions/workflows/test.yml)

A Python ETL repository containing two pipelines that share infrastructure but address different data domains:

- **Macro pipeline** — ingests 14 U.S. macroeconomic indicators from the FRED, BLS, and USDA ERS public data sources, normalizes them into a tidy long-format schema, and upserts them into a SQL database.
- **Grocery pipeline** — ingests CSV output from the upstream `knot-shore-grocery-simulation-engine`, validates schemas, applies detection rules, and produces canonical parquet artifacts that downstream API and portal repositories consume.

Both pipelines share configuration, structured logging, and CI. The repository contains 262 tests covering both, with no live API calls or database connections in the test suite.

## Table of contents

- [Where this fits in the platform](#where-this-fits-in-the-platform)
- [Quick start](#quick-start)
- [The macro pipeline](#the-macro-pipeline)
- [The grocery pipeline](#the-grocery-pipeline)
  - [Sim engine ingestion](#sim-engine-ingestion)
  - [Exception detection](#exception-detection)
  - [Canonical pipeline fixtures](#canonical-pipeline-fixtures)
- [Project structure](#project-structure)
- [Exception classes](#exception-classes)
- [Logging](#logging)
- [Testing](#testing)
- [Adjacent repositories](#adjacent-repositories)

## Where this fits in the platform

The grocery pipeline sits between the upstream sim engine and the downstream API + portal:

```
knot-shore-grocery-simulation-engine    →    economic-data-etl    →    economic-data-api    →    knot-shore-portal
upstream csv generator                       this repo                  service layer              dashboards
                                            (grocery side)                                          + docs hub
```

The grocery side reads the sim engine's CSV output tree, transforms into canonical parquet artifacts, and writes them to `data/processed/canonical/`. Those artifacts are byte-identically copied into the API repo's bundled fixtures (`app/fixtures/`) so a clone-and-run demo of the API works without re-running this pipeline. Reader-grade documentation for the ETL — source-adapter / transform separation, detection rules, canonical fixture flow — lives at the portal's [`/about/etl`](https://github.com/Caseykelly87/knot-shore-portal) page.

The macro pipeline is independent of the grocery side. It connects to FRED, BLS, and ERS sources directly and writes to Postgres. A side dependency: the sim engine's optional Stage 2 (the "realism layer") reads from the same Postgres, so when the sim engine runs with realism enabled, the macro pipeline must have populated the database first.

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
python -m pytest -q                            # 262 tests, no live api or db calls
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

Upserts fact and dimension rows via SQLAlchemy. Each row is classified as inserted (new), updated (revision detected), or unchanged (skipped) based on primary key and value comparison. The pipeline reports `{"inserted": N, "updated": N, "unchanged": N}` on every run. Defaults to SQLite at `data/economic_data.db`; swap to Postgres by setting `DATABASE_URL` — no code changes needed.

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

#### BLS series (5 indicators, batch request)

| Key | Series ID | Description |
|---|---|---|
| `CPI_URBAN` | `CUUR0000SA0` | Headline CPI — All Urban Consumers |
| `CPI_CORE` | `CUUR0000SA0L1E` | Core CPI (excludes food and energy) |
| `GAS_PRICE` | `APU000074714` | Average retail price: gasoline |
| `AVG_WAGES` | `CES0500000003` | Average Hourly Earnings, All Employees |
| `WAGE_INDEX` | `CIU2020000000000I` | Employment Cost Index |

#### USDA ERS food-price categories (8 series)

The USDA Economic Research Service publishes a monthly Food Price Outlook CSV with year-over-year CPI forecasts across eight food categories. `ERS_CATEGORY_MAP` in `src/config.py` maps the CSV's category strings to the internal `series_id` values stored in `dim_series` and `fact_economic_observations`:

| Series ID | Source category (CSV) | Description |
|---|---|---|
| `ERS_ALL_FOOD` | `All food` | Combined at-home and away-from-home |
| `ERS_FOOD_HOME` | `Food at home` | Groceries for at-home consumption |
| `ERS_FOOD_AWAY` | `Food away from home` | Restaurants and prepared meals |
| `ERS_CEREALS` | `Cereals and bakery products` | Cereals and bakery products |
| `ERS_MEATS` | `Meats, poultry, and fish` | Meats, poultry, and fish |
| `ERS_DAIRY` | `Dairy products` | Dairy products |
| `ERS_FRUITS_VEG` | `Fruits and vegetables` | Fresh fruits and vegetables |
| `ERS_BEVERAGES` | `Nonalcoholic beverages and beverage materials` | Nonalcoholic beverages |

The `ERS_` prefix marks the upstream source; the data is sourced directly from USDA ERS, not BLS. The CSV is loaded as a single batch — `ERS_SUMMARY_URL` in `src/config.py` points at the food-price-outlook landing page, and `src/extract.py` discovers the current monthly CSV URL by scraping that page (ERS rotates the media ID on every publication; a hard-coded fallback URL in `extract.py` covers discovery failures).

To add or remove series, edit `FRED_SERIES`, `BLS_SERIES`, or `ERS_CATEGORY_MAP` in `src/config.py`. No other files need to change.

## The grocery pipeline

### Sim engine ingestion

A second ingest path, independent of the macro FRED/BLS/ERS pipeline above, consumes daily output from a grocery-chain simulation engine and produces three parquet artifacts: `store_daily_metrics.parquet` at the store-day grain, `department_daily_metrics.parquet` at the store-day-department grain, and `dim_stores.parquet` carrying the 8-store reference dimension. All three are the canonical input for downstream exception detection and the API.

**Operational context.** In the operational world this platform models, eight store managers record daily numbers in a shared Google Doc. For the simulation phase the sim engine emits the same granularity as a Google Sheets export would, and this ETL reads from the sim engine's local output tree. A Google Sheets transport is deliberately out of scope for this phase; the source adapter (`src/sim_ingest.py`) is structured so that substituting one is a drop-in replacement with no change to the transform layer or CLI.

The source-adapter / transform separation is the basic discipline of pipeline engineering. `sim_ingest.py` knows everything about the sim engine's CSV format — column names, type coercion rules, the directory walk pattern — and produces typed records. `sim_transform.py` knows nothing about CSV; it consumes typed records and produces canonical DataFrames. If the sim engine ever changed its output format, only `sim_ingest.py` would need updating.

**Input tree shape (consumed):**

```
output/
├── daily/{MM}/{DD}/{YYYY}/store_summary.csv
├── daily/{MM}/{DD}/{YYYY}/department_sales.csv
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

#### The six rules

Five statistical-band rules check whether a store-day value sits inside an expected band:

| rule_id | Checks | Band |
|---|---|---|
| `revenue_band` | `total_sales` vs `base_daily_revenue` | ± 25% |
| `labor_pct_band` | `labor_cost_pct` vs profile center | ± 5pp around profile center |
| `avg_ticket_band` | `avg_basket_size` vs profile center | ± 20% around profile center |
| `transactions_band` | `transaction_count` vs `base / avg_ticket_center` | ± 25% |
| `yoy_comp` | current/T-365 sales ratio | ratio outside `[0.85, 1.25]` |

Per-profile centers (from the live sim engine seed): `suburban-family` → labor 0.105, ticket $38.00; `urban-dense` → labor 0.115, ticket $28.00; `value-market` → labor 0.120, ticket $32.00.

The sixth rule, `department_coverage`, is a structural-integrity rule rather than a band. It evaluates the department-grain metrics one group per `(date, store_id)` and flags any store-day whose department row count is not 10, or that carries a duplicated `department_id`.

#### Severity

`severity_score = distance_from_band / band_half_width`. Values are bucketed into `info` (score ≤ 1), `warning` (1 < score ≤ 2), and `critical` (> 2). Closed-day rows (`total_sales == 0`) are skipped by `labor_pct_band`, `avg_ticket_band`, and `transactions_band`. `yoy_comp` is silently skipped when no T-365 row is present.

#### Grain

Detection runs at two grains. The five statistical-band rules evaluate `store_daily_metrics` at store-day grain. The `department_coverage` structural-integrity rule evaluates `department_daily_metrics` at department-grain, one group per `(date, store_id)`. Department-grain integrity breaches — a store-day missing a department's row, or carrying a duplicated `department_id` — are detected by `department_coverage` and flagged in `anomaly_flags.parquet`.

There is no day-of-week or seasonal adjustment in the current rules. Bands are deliberately wide to tolerate weekend and holiday variance without false-positive flooding. A future seasonal-baseline phase could produce empirical per-store-date expected values that a rule refactor would consume in place of static bands.

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

Four canonical parquet artifacts produced by running the sim engine + ETL pipeline end-to-end are committed at `data/processed/canonical/`:

| File | Rows × Cols | Notes |
|---|---:|---|
| `store_daily_metrics.parquet` | 2,944 × 6 | 8 stores × 184 days × 2 years (paired-year canonical) |
| `department_daily_metrics.parquet` | 29,414 × 7 | Same window across 10 departments per store-day |
| `dim_stores.parquet` | 8 × 10 | One row per store with identification, location, and base_daily_revenue |
| `anomaly_flags.parquet` | 883 × 9 | 807 info, 76 warning, 0 critical |

**`store_daily_metrics.parquet`** spans two paired six-month windows: 2024-07-01 through 2024-12-31 and 2025-07-01 through 2025-12-31, each covering all 8 stores. The 2025 window is the demo dataset surfaced by the dashboard; the 2024 window enables year-over-year comparison views consumed by the portal's store drilldown via the API's existing `start_date` / `end_date` query parameters. Filtering this parquet to the 2025 window yields 1,472 rows. Columns: `date`, `store_id`, `total_sales`, `transaction_count`, `avg_basket_size`, `labor_cost_pct`.

**`department_daily_metrics.parquet`** spans the same two paired windows across all 8 stores and all 10 departments. The upper bound is 29,440 rows (8 × 10 × 184 × 2); the actual count is 29,414 because some store-day-department combinations are missing from the sim engine's output (a department closed for inventory or an opening-day register gap). Columns: `date`, `store_id`, `department_id`, `net_sales`, `transactions`, `units_sold`, `gross_margin_pct` (preserved as a fraction; the portal's display layer handles percent formatting). Rows are sorted by `(date, store_id, department_id)`.

**`dim_stores.parquet`** is the canonical store reference dataset: 8 rows in `store_id` order. Columns: `store_id`, `store_name`, `address`, `city`, `zip`, `county_fips`, `trade_area_profile`, `sqft`, `open_date`, `base_daily_revenue`. Only `store_id` is type-coerced (to `int64`); other columns pass through as pandas reads them — `zip`, `county_fips`, `sqft` are `int64`; `open_date` is a string in `YYYY-MM-DD` form; `base_daily_revenue` is `float64`.

**`anomaly_flags.parquet`** is the `detect_cli` output: the five statistical-band rules run against the store-day metrics, and the `department_coverage` structural rule against the department metrics. Detection runs across both the 2024 and 2025 windows; the `yoy_comp` rule fires only where a prior-year baseline exists. Of the 883 rows, 831 are band-rule flags and 52 are structural flags.

These four files are the authoritative downstream input for the `economic-data-api` repo's bundled fixtures. The API copies them byte-identically into `app/fixtures/` so a clone-and-run demo of the API works without re-running this pipeline. Committing them to git makes the canonical state visible in PR diffs and reproducible across clones without requiring downstream consumers to install and run the sim engine.

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

   The script orchestrates `sim_cli` followed by `detect_cli` via subprocess and writes all four parquets to `--output-dir`.

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
│   └── detection_rules.yaml    # Threshold declarations for the 6 detection rules
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
│   ├── load.py                 # Macro pipeline schema + upsert
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
│   ├── test_extract.py                    # 30 tests — macro extract + idempotency
│   ├── test_transform.py                  # 45 tests — macro transform + edge cases
│   ├── test_load.py                       # 16 tests — schema, upsert, idempotency
│   ├── test_main.py                       # 15 tests — macro pipeline orchestration
│   ├── test_observability.py              # 3 tests — shared logging configurator
│   ├── test_sim_ingest.py                 # 16 tests — store-grain csv adapter
│   ├── test_sim_ingest_department.py      # 12 tests — department-grain csv adapter
│   ├── test_sim_transform.py              # 20 tests — store-grain transforms
│   ├── test_sim_transform_department.py   # 12 tests — department-grain transforms
│   ├── test_sim_cli.py                    # 11 tests — grocery cli orchestration
│   ├── test_sim_integration.py            # 8 tests — end-to-end ingest happy path
│   ├── test_sim_engine_contract.py        # 3 tests — sim engine output contract
│   ├── test_detect_rules.py               # 41 tests — rule logic + edge cases
│   ├── test_detect_cli.py                 # 12 tests — detect cli orchestration
│   ├── test_detect_integration.py         # 7 tests — end-to-end detection
│   ├── test_detect_structural_contract.py # 4 tests — structural-integrity rule contract
│   └── test_build_canonical_fixtures.py   # 7 tests — canonical regeneration script
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
python -m pytest -q                              # all 262 tests
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
