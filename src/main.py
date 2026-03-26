import logging
from sqlalchemy import create_engine
from datetime import datetime

from src.extract import fetch_fred_data, fetch_bls_data, fetch_census_msrs, fetch_ers_price_outlook
from src.transform import (
    parse_fred_observations,
    parse_bls_batch,
    parse_census_msrs,
    parse_ers_csv,
    build_dim_series,
    combine_fact_tables,
)
from src.load import ensure_tables_exist, upsert_observations, upsert_dim_series
from src.config import (
    FRED_SERIES,
    BLS_SERIES,
    CENSUS_SERIES,
    ERS_CATEGORY_MAP,
    ERS_SERIES,
    GROCERY_CONFIG,
    DATABASE_URL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def run_pipeline():
    logging.info("Starting Economic Data ETL Pipeline")

    # ------------------------------------------------------------------
    # Phase 1: Extract — fetch from APIs and persist raw JSON snapshots
    # ------------------------------------------------------------------
    try:
        fred_data = {}
        for name, series_id in FRED_SERIES.items():
            fred_data[name] = fetch_fred_data(series_id)

        bls_data    = fetch_bls_data(BLS_SERIES, 2021, datetime.now().year)
        census_data = fetch_census_msrs()
        ers_data    = fetch_ers_price_outlook()

    except Exception as e:
        logging.error(f"Pipeline failed during extraction: {e}")
        return

    logging.info("Extraction complete.")

    # ------------------------------------------------------------------
    # Phase 2: Transform — normalize raw dicts into tidy DataFrames
    # ------------------------------------------------------------------
    try:
        fred_frames = [
            parse_fred_observations(fred_data[name], series_id, name)
            for name, series_id in FRED_SERIES.items()
            if fred_data.get(name) is not None
        ]

        bls_frame    = parse_bls_batch(bls_data, BLS_SERIES)
        census_frame = parse_census_msrs(
            census_data, "CENSUS_MSRS_MO_4451", "GROCERY_SALES_MO"
        )
        ers_frame    = parse_ers_csv(ers_data, ERS_CATEGORY_MAP, GROCERY_CONFIG["START_YEAR"])

        fact_df = combine_fact_tables(
            fred_frames, bls_frame, extra_frames=[census_frame, ers_frame]
        )
        dim_df  = build_dim_series(
            FRED_SERIES, BLS_SERIES,
            census_series=CENSUS_SERIES,
            ers_series=ERS_SERIES,
        )

    except Exception as e:
        logging.error(f"Pipeline failed during transform: {e}")
        return

    logging.info("Transform complete.")

    # ------------------------------------------------------------------
    # Phase 3: Load — upsert DataFrames into the database
    # ------------------------------------------------------------------
    try:
        engine = create_engine(DATABASE_URL)
        ensure_tables_exist(engine)
        obs_stats = upsert_observations(fact_df, engine)
        dim_stats = upsert_dim_series(dim_df, engine)

    except Exception as e:
        logging.error(f"Pipeline failed during load: {e}")
        return

    logging.info(
        f"Pipeline complete — observations: {obs_stats}, dim_series: {dim_stats}"
    )


if __name__ == "__main__":
    run_pipeline()
