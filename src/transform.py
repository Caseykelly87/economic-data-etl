import pandas as pd


def parse_fred_observations(data: dict, series_id: str, series_name: str) -> pd.DataFrame:
    """
    Parse a raw FRED API response dict into a normalised DataFrame.

    FRED encodes missing values as the string "." — these are coerced to NaN.
    Extra FRED metadata fields (realtime_start, realtime_end, etc.) are excluded.

    Parameters
    ----------
    data        : full FRED response dict (must contain an 'observations' key)
    series_id   : technical series ID,  e.g. "UNRATE"
    series_name : human-readable config key, e.g. "UNRATE"

    Returns
    -------
    DataFrame with columns: series_id, series_name, date (datetime64), value (float64), source
    """
    df = pd.DataFrame(data["observations"])[["date", "value"]]

    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")  # "." becomes NaN

    df["series_id"] = series_id
    df["series_name"] = series_name
    df["source"] = "FRED"

    return df[["series_id", "series_name", "date", "value", "source"]]


def parse_bls_batch(data: dict, series_map: dict) -> pd.DataFrame:
    """
    Parse a raw BLS API batch response dict into a normalised DataFrame.

    BLS returns observations most-recent-first; output is sorted oldest-first.
    The period field (e.g. "M01") is combined with year to produce a date
    representing the first day of that month.

    Parameters
    ----------
    data       : full BLS response dict (must contain Results.series)
    series_map : dict mapping human-readable name -> series_id (e.g. BLS_SERIES
                 from config), used to attach series_name to each row

    Returns
    -------
    DataFrame with columns: series_id, series_name, date (datetime64), value (float64), source
    Sorted oldest-first by date.
    """
    id_to_name = {v: k for k, v in series_map.items()}

    frames = []
    for series in data["Results"]["series"]:
        series_id = series["seriesID"]
        rows = [
            {
                "series_id": series_id,
                "series_name": id_to_name.get(series_id, series_id),
                "date": pd.Timestamp(year=int(obs["year"]), month=int(obs["period"][1:]), day=1),
                "value": pd.to_numeric(obs["value"], errors="coerce"),  # '-' or '.' → NaN
                "source": "BLS",
            }
            for obs in series["data"]
        ]
        frames.append(pd.DataFrame(rows))

    df = pd.concat(frames, ignore_index=True).sort_values("date").reset_index(drop=True)
    return df[["series_id", "series_name", "date", "value", "source"]]


def build_dim_series(
    fred_series: dict,
    bls_series: dict,
    ers_series: dict = None,
) -> pd.DataFrame:
    """
    Build a dimension table from the configured series mappings.

    Parameters
    ----------
    fred_series   : FRED_SERIES dict from config   (name -> series_id)
    bls_series    : BLS_SERIES dict from config    (name -> series_id)
    census_series : CENSUS_SERIES dict (optional)  (name -> series_id)
    ers_series    : ERS_SERIES dict (optional)     (name -> series_id)

    Returns
    -------
    DataFrame with columns: series_id, series_name, source
    One row per configured series (FRED, BLS, CENSUS, ERS).
    """
    rows = [
        {"series_id": sid, "series_name": name, "source": "FRED"}
        for name, sid in fred_series.items()
    ] + [
        {"series_id": sid, "series_name": name, "source": "BLS"}
        for name, sid in bls_series.items()
    ] + [
        {"series_id": sid, "series_name": name, "source": "ERS"}
        for name, sid in (ers_series or {}).items()
    ]
    return pd.DataFrame(rows, columns=["series_id", "series_name", "source"])



def combine_fact_tables(
    fred_frames: list,
    bls_frame: pd.DataFrame,
    extra_frames: list = None,
) -> pd.DataFrame:
    """
    Merge all per-series FRED DataFrames with the BLS batch DataFrame and any
    additional source DataFrames (e.g. Census MSRS, ERS forecasts).

    Parameters
    ----------
    fred_frames  : list of DataFrames, one per FRED series (output of parse_fred_observations)
    bls_frame    : single DataFrame for all BLS series (output of parse_bls_batch)
    extra_frames : optional list of additional source DataFrames (e.g. Census, ERS)

    Returns
    -------
    DataFrame with columns: series_id, series_name, date (datetime64), value (float64), source
    Sorted oldest-first by date.
    """
    all_frames = fred_frames + [bls_frame] + (extra_frames or [])
    return (
        pd.concat(all_frames, ignore_index=True)
        .sort_values("date")
        .reset_index(drop=True)
    )


def parse_ers_csv(data: dict, category_map: dict, start_year: int) -> pd.DataFrame:
    """
    Parse an ERS CPI Forecasts dict (rows stored as list of dicts) into a normalised DataFrame.

    Each row's 'Category' is mapped to a series_id via category_map; rows with
    unmapped categories are silently dropped.  The 'Year' column produces a date
    of January 1st of that year.  The value column is resolved in priority order:
    Forecast_Midpoint → Annual → Midpoint → first other numeric column.

    Parameters
    ----------
    data         : dict with a 'rows' key — list of row dicts as produced by
                   fetch_ers_price_outlook
    category_map : dict mapping raw CSV category strings to series_id strings
                   (e.g. ERS_CATEGORY_MAP from config)
    start_year   : only include rows where Year >= start_year

    Returns
    -------
    DataFrame with columns: series_id, series_name, date (datetime64), value (float64), source
    Sorted oldest-first by date.
    """
    rows = data.get("rows", [])
    if not rows:
        return pd.DataFrame(columns=["series_id", "series_name", "date", "value", "source"])

    df = pd.DataFrame(rows)

    if "Year" not in df.columns or "Category" not in df.columns:
        return pd.DataFrame(columns=["series_id", "series_name", "date", "value", "source"])

    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df = df[df["Year"] >= start_year].copy()

    value_col = None
    for preferred in ("Forecast_Midpoint", "Annual", "Midpoint"):
        if preferred in df.columns:
            value_col = preferred
            break
    if value_col is None:
        candidate_cols = [
            c for c in df.columns
            if c not in ("Year", "Category")
            and pd.to_numeric(df[c], errors="coerce").notna().any()
        ]
        value_col = candidate_cols[0] if candidate_cols else None

    if value_col is None:
        return pd.DataFrame(columns=["series_id", "series_name", "date", "value", "source"])

    df["series_id"] = df["Category"].map(category_map)
    df = df.dropna(subset=["series_id"])
    df["series_name"] = df["series_id"]
    df["date"] = pd.to_datetime(df["Year"].astype(int).astype(str) + "-01-01")
    df["value"] = pd.to_numeric(df[value_col], errors="coerce")
    df["source"] = "ERS"

    return (
        df[["series_id", "series_name", "date", "value", "source"]]
        .sort_values("date")
        .reset_index(drop=True)
    )

