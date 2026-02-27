# Economic Data ETL Pipeline

This project is a modular Python-based ETL (Extract, Transform, Load) pipeline that ingests, cleans, and transforms U.S. economic data from the BLS (Bureau of Labor Statistics) and FRED (Federal Reserve Bank of St. Louis) APIs. The output is standardized, analysis-ready datasets suitable for downstream modeling or simulation, such as synthetic retail sales generation.

---

## 🚀 Features

* **Automated Data Extraction:** Pulls economic indicators from BLS and FRED APIs using robust retrieval logic.
* **Data Normalization:** Standardizes temporal granularity (monthly/quarterly) across multiple sources into a unified timeline.
* **Robust Handling:** Includes comprehensive logging, error handling with exponential backoff, and missing data management.
* **Advanced Idempotency:** Implements a caching layer to prevent redundant API calls and respect provider rate limits.
* **Modular Architecture:** Separation of concerns across extraction, transformation, and loading modules for high maintainability.

---

## 📂 Project Structure

```text
.
├── data/
│   ├── metadata/           # Data dictionaries, source mappings, and schema definitions
│   ├── processed/          # Cleaned, transformed datasets (Parquet/CSV)
│   └── raw/                # Immutable raw JSON responses from API providers
├── src/
│   ├── config.py           # API configuration and Series ID mappings
│   ├── extract.py          # API wrappers and idempotency logic
│   ├── transform.py        # Data cleaning, normalization, and type conversion
│   ├── load.py             # Storage management and persistence layer
│   └── main.py             # Pipeline orchestration and execution entry point
├── .env                    # Environment variables (API Keys - Git Ignored)
├── .gitignore              # Project exclusion rules
├── README.md               # Project documentation
└── requirements.txt        # Python dependency manifest
