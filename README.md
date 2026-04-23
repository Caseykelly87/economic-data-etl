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

Output: `data/processed/store_daily_metrics.parquet` with five columns in
this order — `date`, `store_id`, `total_sales` (net of returns),
`transaction_count`, `avg_basket_size`.

**Full-rebuild semantics.** Running the CLI twice against identical input
produces a byte-identical parquet file. Rows are sorted deterministically
by `(date, store_id)` before write. There is no append mode.

**Typed failure modes.** `SchemaValidationError` (missing required column,
unparseable row, or a `store_id` not present in `dim_stores`) and
`ReconciliationError` (a walked date directory missing `store_summary.csv`,
or output row count ≠ input row count) cause a non-zero exit without
writing partial output.

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