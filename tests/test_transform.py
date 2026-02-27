import pandas as pd
import pytest
from src import transform


FRED_SERIES_MAP = {"UNRATE": "UNRATE", "PCE_NOMINAL": "PCEC"}
BLS_SERIES_MAP  = {"CPI_URBAN": "CUUR0000SA0", "AVG_WAGES": "CES0500000003"}


# ==========================================================
# FRED Parsing Tests
# Function under test: transform.parse_fred_observations(data, series_id, series_name)
# ==========================================================

def test_parse_fred_returns_dataframe(raw_fred_json):
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert isinstance(result, pd.DataFrame)


def test_parse_fred_expected_columns(raw_fred_json):
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert list(result.columns) == ["series_id", "series_name", "date", "value", "source"]


def test_parse_fred_row_count_matches_observations(raw_fred_json):
    """One row per observation, including rows where value is missing."""
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert len(result) == 3


def test_parse_fred_date_column_is_datetime(raw_fred_json):
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert pd.api.types.is_datetime64_any_dtype(result["date"])


def test_parse_fred_value_column_is_float(raw_fred_json):
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert result["value"].dtype == "float64"


def test_parse_fred_missing_value_dot_becomes_nan(raw_fred_json):
    """FRED encodes missing values as the string '.', not null. Must become NaN."""
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert pd.isna(result.loc[result["date"] == "2024-02-01", "value"].iloc[0])


def test_parse_fred_series_id_populated(raw_fred_json):
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert (result["series_id"] == "UNRATE").all()


def test_parse_fred_series_name_populated(raw_fred_json):
    result = transform.parse_fred_observations(raw_fred_json, "PCEC", "PCE_NOMINAL")
    assert (result["series_name"] == "PCE_NOMINAL").all()


def test_parse_fred_source_label_is_fred(raw_fred_json):
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert (result["source"] == "FRED").all()


def test_parse_fred_extra_api_fields_excluded(raw_fred_json):
    """realtime_start, realtime_end, and other FRED metadata must not appear as columns."""
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    for unexpected in ("realtime_start", "realtime_end", "output_type", "limit"):
        assert unexpected not in result.columns


# ==========================================================
# BLS Parsing Tests
# Function under test: transform.parse_bls_batch(data, series_map)
# ==========================================================

def test_parse_bls_returns_dataframe(raw_bls_json):
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    assert isinstance(result, pd.DataFrame)


def test_parse_bls_expected_columns(raw_bls_json):
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    assert list(result.columns) == ["series_id", "series_name", "date", "value", "source"]


def test_parse_bls_flattens_both_series(raw_bls_json):
    """Two series × 3 observations each = 6 total rows."""
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    assert len(result) == 6


def test_parse_bls_date_constructed_from_year_and_period(raw_bls_json):
    """year='2024' + period='M01' must produce 2024-01-01 as a datetime."""
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    jan = result[result["series_id"] == "CUUR0000SA0"].sort_values("date").iloc[0]
    assert jan["date"] == pd.Timestamp("2024-01-01")


def test_parse_bls_date_column_is_datetime(raw_bls_json):
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    assert pd.api.types.is_datetime64_any_dtype(result["date"])


def test_parse_bls_value_column_is_float(raw_bls_json):
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    assert result["value"].dtype == "float64"


def test_parse_bls_series_name_mapped_from_series_map(raw_bls_json):
    """Series ID 'CUUR0000SA0' must map to human-readable name 'CPI_URBAN'."""
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    cpi_rows = result[result["series_id"] == "CUUR0000SA0"]
    assert (cpi_rows["series_name"] == "CPI_URBAN").all()


def test_parse_bls_source_label_is_bls(raw_bls_json):
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    assert (result["source"] == "BLS").all()


def test_parse_bls_sorted_oldest_first(raw_bls_json):
    """BLS API returns most-recent-first. Output must be normalised to oldest-first."""
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    cpi = result[result["series_id"] == "CUUR0000SA0"]["date"].tolist()
    assert cpi == sorted(cpi)


# ==========================================================
# Dimension Table Tests
# Function under test: transform.build_dim_series(fred_series, bls_series)
# ==========================================================

def test_build_dim_series_returns_dataframe():
    result = transform.build_dim_series(FRED_SERIES_MAP, BLS_SERIES_MAP)
    assert isinstance(result, pd.DataFrame)


def test_build_dim_series_expected_columns():
    result = transform.build_dim_series(FRED_SERIES_MAP, BLS_SERIES_MAP)
    assert list(result.columns) == ["series_id", "series_name", "source"]


def test_build_dim_series_row_count():
    """One row per configured series across both sources."""
    result = transform.build_dim_series(FRED_SERIES_MAP, BLS_SERIES_MAP)
    expected = len(FRED_SERIES_MAP) + len(BLS_SERIES_MAP)
    assert len(result) == expected


def test_build_dim_series_fred_source_label():
    result = transform.build_dim_series(FRED_SERIES_MAP, BLS_SERIES_MAP)
    fred_rows = result[result["series_id"].isin(FRED_SERIES_MAP.values())]
    assert (fred_rows["source"] == "FRED").all()


def test_build_dim_series_bls_source_label():
    result = transform.build_dim_series(FRED_SERIES_MAP, BLS_SERIES_MAP)
    bls_rows = result[result["series_id"].isin(BLS_SERIES_MAP.values())]
    assert (bls_rows["source"] == "BLS").all()


# ==========================================================
# Fact Table Combiner Tests
# Function under test: transform.combine_fact_tables(fred_frames, bls_frame)
# ==========================================================

def test_combine_fact_tables_returns_dataframe(raw_fred_json, raw_bls_json):
    fred_df = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    bls_df  = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    result  = transform.combine_fact_tables([fred_df], bls_df)
    assert isinstance(result, pd.DataFrame)


def test_combine_fact_tables_combines_both_sources(raw_fred_json, raw_bls_json):
    fred_df = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    bls_df  = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    result  = transform.combine_fact_tables([fred_df], bls_df)
    assert set(result["source"].unique()) == {"FRED", "BLS"}


def test_combine_fact_tables_row_count(raw_fred_json, raw_bls_json):
    fred_df = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    bls_df  = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    result  = transform.combine_fact_tables([fred_df], bls_df)
    assert len(result) == len(fred_df) + len(bls_df)


def test_combine_fact_tables_sorted_by_date(raw_fred_json, raw_bls_json):
    fred_df = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    bls_df  = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    result  = transform.combine_fact_tables([fred_df], bls_df)
    dates   = result["date"].tolist()
    assert dates == sorted(dates)


def test_combine_fact_tables_accepts_multiple_fred_frames(raw_fred_json):
    fred_df1 = transform.parse_fred_observations(raw_fred_json, "UNRATE",   "UNRATE")
    fred_df2 = transform.parse_fred_observations(raw_fred_json, "FEDFUNDS", "MONEY_COST")
    empty_bls = pd.DataFrame(columns=["series_id", "series_name", "date", "value", "source"])
    result = transform.combine_fact_tables([fred_df1, fred_df2], empty_bls)
    assert len(result) == len(fred_df1) + len(fred_df2)