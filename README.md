# Economic Data ETL Pipeline

A modular Python pipeline that extracts U.S. macroeconomic data from the
FRED (Federal Reserve Bank of St. Louis) and BLS (Bureau of Labor Statistics)
APIs and stores it as raw JSON snapshots for downstream analysis.

---

## Features

- **Incremental Extraction** — tracks the last observation date per series and
  only requests new data on subsequent runs.
- **Revision Detection** — SHA-256 hashes each API response; files are only
  written when content has actually changed.
- **Resilient Requests** — exponential backoff retries on transient network
  errors (up to 3 attempts).
- **Isolated Test Suite** — 30 unit tests with no live API calls; all I/O is
  redirected to pytest's temporary directories.

> **Planned (not yet implemented):** `transform.py` (normalization),
> `load.py` (persistence layer).

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
│   └── main.py                 # Pipeline entry point
├── tests/
│   ├── conftest.py             # Shared pytest fixtures
│   ├── test_extract.py         # Unit tests for extract.py
│   └── test_main.py            # Unit tests for pipeline orchestration
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

**Idempotency behavior:** On subsequent runs, a series is only re-written if
the API response hash differs from the stored hash. If data is unchanged, the
pipeline logs a skip message and moves on. To force a full re-extraction,
delete the relevant files in `data/metadata/`.

```bash
# Force re-extraction of all FRED series by deleting metadata
del data\metadata\FRED_*_metadata.json   # Windows
# rm data/metadata/FRED_*_metadata.json  # macOS / Linux
```

---

## Testing

```bash
# Run all tests
python -m pytest

# Run with verbose output
python -m pytest -v

# Run with coverage report
python -m pytest --cov=src --cov-report=term-missing
```

The test suite makes no live API calls. All file I/O is redirected to
temporary directories by pytest fixtures in `tests/conftest.py`.