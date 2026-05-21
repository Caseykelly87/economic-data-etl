import pandas as pd
import pytest
from src import transform


FRED_SERIES_MAP = {"UNRATE": "UNRATE", "PCE_NOMINAL": "PCEC"}
BLS_SERIES_MAP  = {"CPI_URBAN": "CUUR0000SA0", "AVG_WAGES": "CES0500000003"}


ERS_CATEGORY_MAP_TEST = {
    "All food":          "ERS_ALL_FOOD",
    "Food at home":      "ERS_FOOD_HOME",
    "Food away from home": "ERS_FOOD_AWAY",
}
ERS_SERIES_MAP_TEST = {sid: sid for sid in ERS_CATEGORY_MAP_TEST.values()}



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


def test_parse_fred_values_parsed_correctly(raw_fred_json):
    """The value column must carry the actual numbers from the response,
    not just the right dtype. raw_fred_json holds "5.0", ".", "5.2"."""
    result = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    assert result["value"].dtype == "float64"
    by_date = result.set_index("date")["value"]
    assert by_date[pd.Timestamp("2024-01-01")] == pytest.approx(5.0)
    assert pd.isna(by_date[pd.Timestamp("2024-02-01")])  # "." → NaN
    assert by_date[pd.Timestamp("2024-03-01")] == pytest.approx(5.2)


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


def test_parse_bls_values_parsed_correctly(raw_bls_json):
    """Values must be the actual numbers, attached to the right month after
    the most-recent-first → oldest-first normalisation. raw_bls_json holds
    CUUR0000SA0 = 312.0/313.5/314.2 for Jan/Feb/Mar 2024."""
    result = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    assert result["value"].dtype == "float64"
    cpi = result[result["series_id"] == "CUUR0000SA0"].set_index("date")["value"]
    assert cpi[pd.Timestamp("2024-01-01")] == pytest.approx(312.0)
    assert cpi[pd.Timestamp("2024-02-01")] == pytest.approx(313.5)
    assert cpi[pd.Timestamp("2024-03-01")] == pytest.approx(314.2)
    wages = result[result["series_id"] == "CES0500000003"].set_index("date")["value"]
    assert wages[pd.Timestamp("2024-01-01")] == pytest.approx(34.55)
    assert wages[pd.Timestamp("2024-03-01")] == pytest.approx(34.85)


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
# ERS CSV Parsing Tests
# Function under test: transform.parse_ers_csv(data, category_map, start_year)
# ==========================================================

_ERS_RAW = {
    "rows": [
        {"Category": "All food",           "Year": "2024", "Annual": "2.1"},
        {"Category": "Food at home",        "Year": "2024", "Annual": "1.8"},
        {"Category": "Food away from home", "Year": "2024", "Annual": "4.2"},
        {"Category": "All food",            "Year": "2025", "Annual": "2.5"},
    ]
}


def test_parse_ers_csv_returns_dataframe():
    result = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    assert isinstance(result, pd.DataFrame)


def test_parse_ers_csv_expected_columns():
    result = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    assert list(result.columns) == ["series_id", "series_name", "date", "value", "source"]


def test_parse_ers_csv_maps_categories_to_series_ids():
    result = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    assert "ERS_ALL_FOOD" in result["series_id"].values
    assert "ERS_FOOD_HOME" in result["series_id"].values


def test_parse_ers_csv_drops_unmapped_categories():
    """Categories not in category_map must not appear in the output."""
    raw = {
        "rows": [
            {"Category": "All food",           "Year": "2024", "Annual": "2.1"},
            {"Category": "Exotic imported teas","Year": "2024", "Annual": "9.9"},
        ]
    }
    result = transform.parse_ers_csv(raw, ERS_CATEGORY_MAP_TEST, 2024)
    assert len(result) == 1
    assert result.iloc[0]["series_id"] == "ERS_ALL_FOOD"


def test_parse_ers_csv_date_is_january_first():
    """Year 2024 must produce 2024-01-01."""
    result = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    all_food = result[result["series_id"] == "ERS_ALL_FOOD"].sort_values("date")
    assert all_food.iloc[0]["date"] == pd.Timestamp("2024-01-01")


def test_parse_ers_csv_date_column_is_datetime():
    result = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    assert pd.api.types.is_datetime64_any_dtype(result["date"])


def test_parse_ers_csv_values_parsed_correctly():
    """The Annual column resolves as the value, mapped to the right series.
    _ERS_RAW holds All food 2.1 (2024) / 2.5 (2025), Food at home 1.8,
    Food away from home 4.2."""
    result = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    assert result["value"].dtype == "float64"
    all_food = result[result["series_id"] == "ERS_ALL_FOOD"].set_index("date")["value"]
    assert all_food[pd.Timestamp("2024-01-01")] == pytest.approx(2.1)
    assert all_food[pd.Timestamp("2025-01-01")] == pytest.approx(2.5)
    by_series = result.set_index("series_id")["value"]
    assert by_series["ERS_FOOD_HOME"] == pytest.approx(1.8)
    assert by_series["ERS_FOOD_AWAY"] == pytest.approx(4.2)


def test_parse_ers_csv_source_label():
    result = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    assert (result["source"] == "ERS").all()


def test_parse_ers_csv_filters_by_start_year():
    """Rows with Year < start_year must be excluded."""
    raw = {
        "rows": [
            {"Category": "All food", "Year": "2023", "Annual": "5.0"},
            {"Category": "All food", "Year": "2024", "Annual": "2.1"},
        ]
    }
    result = transform.parse_ers_csv(raw, ERS_CATEGORY_MAP_TEST, 2024)
    assert len(result) == 1
    assert result.iloc[0]["date"] == pd.Timestamp("2024-01-01")


def test_parse_ers_csv_empty_rows_returns_empty_dataframe():
    result = transform.parse_ers_csv({"rows": []}, ERS_CATEGORY_MAP_TEST, 2024)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
    assert list(result.columns) == ["series_id", "series_name", "date", "value", "source"]


def test_parse_ers_csv_missing_required_columns_returns_empty_dataframe():
    """If 'Year' or 'Category' columns are absent the function must return an empty DataFrame."""
    result = transform.parse_ers_csv({"rows": [{"Foo": "bar"}]}, ERS_CATEGORY_MAP_TEST, 2024)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_parse_ers_csv_sorted_oldest_first():
    result = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    dates = result["date"].tolist()
    assert dates == sorted(dates)



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


def test_parse_bls_missing_value_dash_becomes_nan():
    """BLS encodes missing values as '-'. Must become NaN, not raise."""
    data = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [{
                "seriesID": "CUUR0000SA0",
                "data": [
                    {"year": "2024", "period": "M01", "value": "312.0", "footnotes": [{}]},
                    {"year": "2024", "period": "M02", "value": "-",     "footnotes": [{}]},
                ],
            }]
        },
    }
    result = transform.parse_bls_batch(data, {"CPI_URBAN": "CUUR0000SA0"})
    feb_row = result[result["date"] == pd.Timestamp("2024-02-01")]
    assert pd.isna(feb_row["value"].iloc[0])    


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



def test_build_dim_series_includes_ers_rows():
    result = transform.build_dim_series(
        FRED_SERIES_MAP, BLS_SERIES_MAP, ers_series=ERS_SERIES_MAP_TEST
    )
    ers_rows = result[result["source"] == "ERS"]
    assert len(ers_rows) == len(ERS_SERIES_MAP_TEST)


def test_build_dim_series_without_ers_arg():
    """Calling without ers_series must return only FRED and BLS rows."""
    result = transform.build_dim_series(FRED_SERIES_MAP, BLS_SERIES_MAP)
    assert len(result) == len(FRED_SERIES_MAP) + len(BLS_SERIES_MAP)
    assert "ERS" not in result["source"].values


def test_combine_fact_tables_with_extra_frames(raw_fred_json, raw_bls_json):
    fred_df  = transform.parse_fred_observations(raw_fred_json, "UNRATE", "UNRATE")
    bls_df   = transform.parse_bls_batch(raw_bls_json, BLS_SERIES_MAP)
    extra_df = transform.parse_ers_csv(_ERS_RAW, ERS_CATEGORY_MAP_TEST, 2024)
    result   = transform.combine_fact_tables([fred_df], bls_df, extra_frames=[extra_df])
    assert len(result) == len(fred_df) + len(bls_df) + len(extra_df)
    assert "ERS" in result["source"].values

