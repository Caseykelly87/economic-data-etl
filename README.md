# Economic Data ETL Pipeline

**Project Overview**  
This project is a modular Python-based ETL pipeline that ingests, cleans, and transforms U.S. economic data from the BLS (Bureau of Labor Statistics) and FRED (Federal Reserve Bank of St. Louis) APIs, with optional ERS datasets. The output is standardized, analysis-ready datasets suitable for downstream modeling or simulation, such as synthetic retail sales generation.

---

## Features

- **Automated Data Extraction:** Pulls economic indicators from BLS and FRED APIs.  
- **Data Normalization:** Standardizes temporal granularity (monthly/quarterly) across multiple sources.  
- **Robust Handling:** Includes logging, error handling, and missing data management.  
- **Reproducible Pipeline:** Uses Python virtual environments and modular scripts.  
- **Optional ERS Data Integration:** Supports static CSVs or automated downloads.  
- **Output Formats:** Parquet or CSV for easy downstream consumption.

---

## Project Structure

├── .venv/ # Python virtual environment
├── data/
│ ├── raw/ # Raw API/CSV data
│ └── processed/ # Cleaned, transformed datasets
├── src/
│ ├── extract.py # API extraction scripts
│ ├── transform.py # Cleaning and feature engineering
│ ├── load.py # Save processed data
│ └── main.py # Orchestrates ETL pipeline
├── requirements.txt # Python dependencies
├── .gitignore
└── README.md


## Setup & Installation

1. **Activate virtual environment**  
   (Assuming you already created `.venv` in your project root.)

bash
# Git Bash on Windows
source .venv/Scripts/activate

2. **Install dependencies**
bash
pip install -r requirements.txt

```
