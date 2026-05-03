# Economic Data ETL Pipeline

A production-style Python ETL pipeline with **85 unit tests at 99% coverage** that
ingests 14 U.S. macroeconomic indicators from the FRED and BLS public APIs, normalizes
them into a tidy star schema, and upserts them into a SQL database — with zero manual
intervention on repeat runs.

---

## How It Works

Extract → Transform → Load



1. **Extract** — Fetches 9 FRED series individually and 5 BLS series in a single batch
   request. Each response is SHA-256 hashed; files are only written when content has
   genuinely changed, making every run fully idempotent.

2. **Transform** — Normalizes raw API dicts into typed pandas DataFrames. Handles
   source-specific missing value encodings (`"."` for FRED, `"-"` for BLS) as `NaN`.
   Produces a long-format fact table and a dimension table ready for direct SQL load.

3. **Load** — Upserts fact and dimension rows via SQLAlchemy. New rows are inserted,
   revised rows are updated in place, and unchanged rows are skipped — reported as
   `{"inserted": N, "updated": N, "unchanged": N}` on every run. Defaults to SQLite;
   swap to Postgres by setting `DATABASE_URL` in `.env` with no code changes.

---

## Features

- **Idempotent extraction** — SHA-256 revision detection prevents redundant writes
- **Incremental requests** — stores the last observation date per series; only fetches new data
- **Resilient networking** — exponential backoff retry on transient HTTP errors (3 attempts)
- **Upsert-aware load** — insert, update, or skip each row based on primary key and value comparison
- **Database-agnostic** — SQLAlchemy engine abstraction; SQLite locally, Postgres in production
- **Test-driven** — 85 unit tests, 99% coverage, zero live API calls in the test suite

---

## Project Structure

```text
.
├── data/                       # Git-ignored; created automatically at runtime
│   ├── metadata/               # Per-series extraction state (hash, last date)
│   ├── processed/              # Transformed outputs (planned)
│   └── raw/                    # Immutable raw JSON snapshots from APIs
├── src/
│   ├── __init__.py
│   ├── config.py               # API keys, paths, and series ID mappings
│   ├── extract.py              # FRED and BLS API clients with idempotency logic
│   ├── transform.py            # DataFrame normalization and combination functions
│   ├── load.py                 # Schema creation and upsert operations
│   └── main.py                 # Pipeline entry point
├── tests/
│   ├── conftest.py             # Shared pytest fixtures (temp dirs, mock responses, DB engine)
│   ├── test_extract.py           # 26 tests — hashing, metadata, retry, FRED, BLS
│   ├── test_transform.py         # 31 tests — parsing, normalization, edge cases
│   ├── test_load.py              # 16 tests — schema creation, upsert, idempotency
├── .env                        # API keys — never commit this file
├── .gitignore
├── CLAUDE.md                   # AI tool governance rules
├── pytest.ini                  # Test runner configuration
├── README.md
└── requirements.txt
```

---

## Prerequisites

- Python 3.12+
- A free FRED ( VERSION 2 ) API key: register at https://fred.stlouisfed.org/docs/api/fred/v2/index.html
- A free BLS ( VERSION 2 ) API key: register at https://data.bls.gov/registrationEngine/
---

## Installation

```bash
# 1. Clone the repository
git clone <repo-url>
cd economic-data-etl

# 2. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Configuration

Create a `.env` file in the project root. This file is git-ignored and must
never be committed.

```ini
# .env
FRED_API_KEY=your_fred_api_key_here
BLS_API_KEY=your_bls_api_key_here
```
# Optional — defaults to SQLite at data/economic_data.db
# DATABASE_URL=postgresql://user:password@localhost:5432/economic_data

`src/config.py` loads these values automatically via `python-dotenv`.

### FRED Series (9 indicators via FRED REST API)

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

### BLS Series (5 indicators via BLS Public API v2, batch request)

| Key | Series ID | Description |
|---|---|---|
| `CPI_URBAN` | `CUUR0000SA0` | Headline CPI — All Urban Consumers |
| `CPI_CORE` | `CUUR0000SA0L1E` | Core CPI (excludes food and energy) |
| `GAS_PRICE` | `APU000074714` | Average retail price: gasoline |
| `AVG_WAGES` | `CES0500000003` | Average Hourly Earnings, All Employees |
| `WAGE_INDEX` | `CIU2020000000000I` | Employment Cost Index |

To add or remove series, edit `FRED_SERIES` or `BLS_SERIES` in
`src/config.py`. No other files need to change.

---

## Usage

```bash
# Run the full extraction pipeline
python -m src.main
```

Output goes to `data/raw/` using the naming convention
`{SOURCE}_{SERIES_ID}_{YYYY_MM_DD}.json`.

**Idempotency behavior:** On first run, the pipeline fetches all series, creates data/economic_data.db, and
loads the full history. subsequent runs, a series is only re-written if
the API response hash differs from the stored hash. If data is unchanged, the
pipeline logs a skip message and moves on. To force a full re-extraction,
delete the relevant files in `data/metadata/`.

```bash
# Force re-extraction of all FRED series by deleting metadata
del data\metadata\FRED_*_metadata.json   # Windows
# rm data/metadata/FRED_*_metadata.json  # macOS / Linux
```

---

## Sim Engine Ingestion

A second ingest path, independent of the macro FRED/BLS/ERS pipeline above,
consumes daily output from a grocery-chain simulation engine and produces
`store_daily_metrics.parquet` — the canonical input for downstream
exception detection and portal APIs.

**Narrative context.** In the operational world this project models, eight
store managers record daily numbers in a shared Google Doc. For the
simulation phase the sim engine emits the same granularity as a Google
Sheets export would, and this ETL reads from the sim engine's local
output tree. A Google Sheets transport is deliberately out of scope for
this phase; the source adapter (`src/sim_ingest.py`) is structured so
that substituting one is a drop-in replacement with no change to the
transform or CLI.

**Input tree shape (consumed):**

```
output/
├── daily/{MM}/{DD}/{YYYY}/store_summary.csv
└── dimensions/dim_stores.csv
```

**Explicitly NOT consumed in this phase:** `department_sales.csv` (reserved
for a later phase) and `anomaly_log.csv` (sim engine QA artifact).

**Run the ingest:**

```bash
python -m src.sim_cli \
  --input-root path/to/sim/output \
  --output-dir data/processed
```

Output: `data/processed/store_daily_metrics.parquet` with six columns in
this order — `date`, `store_id`, `total_sales` (net of returns),
`transaction_count`, `avg_basket_size`, `labor_cost_pct` (labor cost as
a fraction of net sales; `NaN` on closed days where `total_sales == 0`).

**Full-rebuild semantics.** Running the CLI twice against identical input
produces a byte-identical parquet file. Rows are sorted deterministically
by `(date, store_id)` before write. There is no append mode.

**Typed failure modes.** `SchemaValidationError` (missing required column,
unparseable row, or a `store_id` not present in `dim_stores`) and
`ReconciliationError` (a walked date directory missing `store_summary.csv`,
or output row count ≠ input row count) cause a non-zero exit without
writing partial output.

---

## Exception Detection (Phase 2)

Phase 2 reads the metrics parquet produced by `sim_cli`, evaluates five
business rules against every store-day, and writes
`anomaly_flags.parquet` — the canonical input for the upcoming portal
exceptions API and dashboard. Detection has zero awareness of the sim
engine's `anomaly_log.csv`; it sees only the metrics and operational
reference data a real business would have. Repeat invocations against
identical input produce a byte-identical flags parquet.

**Run detection:**

```bash
.venv/Scripts/python.exe -m src.detect_cli \
  --metrics-path data/processed/store_daily_metrics.parquet \
  --sim-output-root path/to/sim/output \
  --rules-path config/detection_rules.yaml \
  --output-dir data/processed
```

Output: `data/processed/anomaly_flags.parquet` with one row per fired
rule, sorted deterministically by `(date, store_id, rule_id)`. Schema
is exactly `(date, store_id, rule_id, actual_value, expected_low,
expected_high, distance_from_band, severity_score, severity_level)`.

### The five rules

| rule_id           | Checks                                  | Band                              |
|-------------------|-----------------------------------------|-----------------------------------|
| `revenue_band`    | `total_sales` vs `base_daily_revenue`   | ± 25%                             |
| `labor_pct_band`  | `labor_cost_pct` vs profile center      | ± 5pp around profile center       |
| `avg_ticket_band` | `avg_basket_size` vs profile center     | ± 20% around profile center       |
| `transactions_band` | `transaction_count` vs `base / avg_ticket_center` | ± 25%                             |
| `yoy_comp`        | current/T-365 sales ratio               | ratio outside `[0.85, 1.25]`      |

Per-profile centers (from the live sim engine seed):
`suburban-family` → labor 0.105, ticket $38.00; `urban-dense` →
labor 0.115, ticket $28.00; `value-market` → labor 0.120, ticket $32.00.

### Severity

`severity_score = distance_from_band / band_half_width`. Values are
bucketed into `info` (score ≤ 1), `warning` (1 < score ≤ 2), and
`critical` (> 2). Closed-day rows (`total_sales == 0`) are skipped by
`labor_pct_band`, `avg_ticket_band`, and `transactions_band`. `yoy_comp`
is silently skipped when no T-365 row is present in the metrics frame.

### Grain limit

Detection operates at **store-day grain only**. Row-level integrity
breaches (sub-percent effect on store totals) are inherently invisible
at this grain; a department-grain phase remains the recovery path if
demo quality demands it. There is **no day-of-week or seasonal
adjustment in phase 2** — bands are deliberately wide to tolerate
weekend and holiday variance without false-positive flooding. Phase 2.5
(Seasonal Baselines) will produce empirical per-store-date expected
values that a future detection refactor could consume in place of the
static bands.

### Evaluating detection quality

`scripts/evaluate_detection.py` measures recall and false-positive rate
against the sim engine's `anomaly_log.csv` ground-truth file. The
script lives outside the ETL package and is the **only piece of code in
this repository permitted to read that log** — detection itself has no
knowledge of it. Not collected by pytest; not imported by any `src/`
module.

```bash
.venv/Scripts/python.exe scripts/evaluate_detection.py \
  --flags-path data/processed/anomaly_flags.parquet \
  --anomaly-log-path path/to/sim/output/anomaly_log.csv \
  --metrics-path data/processed/store_daily_metrics.parquet
```

Reports global recall, per-anomaly-type recall, and FPR. Phase 2
contract: `global_recall ≥ 0.35` AND `fpr ≤ 0.10`. The script prints
a `PASS` / `FAIL` verdict and exits non-zero on `FAIL` so it can be
wired into CI independently if desired.

---

## Canonical Pipeline Fixtures

A pair of canonical parquet artifacts produced by running the sim
engine + ETL pipeline end-to-end is committed to this repository at:

```
data/processed/canonical/
├── store_daily_metrics.parquet   # 1,472 rows × 6 columns
└── anomaly_flags.parquet         # 453 rows × 9 columns
```

**`store_daily_metrics.parquet`** spans 2025-07-01 through 2025-12-31
across all 8 stores (184 days × 8 stores = 1,472 rows). Columns:
`date`, `store_id`, `total_sales`, `transaction_count`,
`avg_basket_size`, `labor_cost_pct`.

**`anomaly_flags.parquet`** is the `detect_cli` output produced by
running the five static detection rules against the metrics parquet.
Severity counts in the current canonical state: 438 `info`, 15
`warning`, 0 `critical`.

These two files are the authoritative downstream input for the
companion API repository's demo mode, which reads copies of these
parquets directly rather than regenerating its own demo data.
Committing them to git makes the canonical state visible in PR diffs
and reproducible across clones without requiring downstream consumers
to install and run the sim engine.

### Regeneration workflow

The canonical fixtures are regenerated only when the underlying sim
engine output deliberately changes. The workflow:

1. **Run the sim engine's `backfill` command** in its repository,
   producing a fresh `output/` directory with the desired window:

   ```bash
   python -m knot_shore backfill --output ./output
   ```

2. **Run `scripts/build_canonical_fixtures.py`** from this repo's
   root, pointing at the sim engine output:

   ```bash
   .venv/Scripts/python.exe scripts/build_canonical_fixtures.py \
       --sim-output-root /path/to/sim/engine/output \
       --output-dir data/processed/canonical/
   ```

   The script orchestrates `sim_cli` followed by `detect_cli` via
   subprocess and writes both parquets to `--output-dir`.

3. **Verify** the resulting parquets visually (date range, row
   counts, columns) and confirm byte-determinism by re-running
   the script to a temp directory and comparing sha256 hashes:

   ```bash
   sha256sum data/processed/canonical/*.parquet
   ```

4. **Commit** the regenerated parquets with a message documenting
   why the canonical state changed (sim engine window shift,
   detection rule update, schema change, etc.).

### Gitignore posture

Ad-hoc parquet output produced by running `sim_cli` or `detect_cli`
directly (e.g. into `data/processed/store_daily_metrics.parquet`)
remains gitignored — only the `canonical/` subdirectory is tracked.
The `.gitignore` walks the `data/` tree one segment at a time so the
canonical re-inclusion takes effect (git cannot un-ignore a file
inside a fully-ignored parent directory).

---

## Logging

Pipeline runs emit structured logs via [structlog](https://www.structlog.org/).
Output is human-readable colored text when stdout is a tty, single-line JSON
otherwise. Log level and format can both be overridden via environment
variables:

| Variable | Values | Default |
|---|---|---|
| `LOG_LEVEL` | `debug`, `info`, `warning`, `error`, `critical` | `info` |
| `LOG_FORMAT` | `console`, `json` | auto (console if tty, else json) |

Example console output (default in a terminal):

```
2026-05-02 14:55:44 [info     ] parquet_written                row_count=1472 output_path=data/processed/store_daily_metrics.parquet
```

Example JSON output (default when piped or redirected, or when `LOG_FORMAT=json`):

```json
{"row_count": 1472, "output_path": "data/processed/store_daily_metrics.parquet", "event": "parquet_written", "level": "info", "timestamp": "2026-05-02T19:55:44.123456Z"}
```

To debug a failing pipeline run:

```bash
LOG_LEVEL=debug python -m src.sim_cli --input-root /path/to/sim/output --output-dir data/processed
```

To capture structured logs for offline analysis:

```bash
LOG_FORMAT=json python -m src.sim_cli --input-root /path/to/sim/output --output-dir data/processed > run.log
```

### Macro pipeline structured fields

The macro pipeline (`src/main.py`, `src/extract.py`) emits the same prose
messages you see in console output, plus structured fields in JSON mode:

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

---

## Testing

```bash
# Run all 85 tests
python -m pytest

# Run with verbose output
python -m pytest -v

# Run with coverage report
python -m pytest --cov=src --cov-report=term-missing
```

The test suite makes no live API calls. All file I/O is redirected to
temporary directories by pytest fixtures in `tests/conftest.py`.
The load layer tests use an in_memory SQLite database and execute rael SQL queries