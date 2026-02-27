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
                "value": float(obs["value"]),
                "source": "BLS",
            }
            for obs in series["data"]
        ]
        frames.append(pd.DataFrame(rows))

    df = pd.concat(frames, ignore_index=True).sort_values("date").reset_index(drop=True)
    return df[["series_id", "series_name", "date", "value", "source"]]


def build_dim_series(fred_series: dict, bls_series: dict) -> pd.DataFrame:
    """
    Build a dimension table from the configured series mappings.

    Parameters
    ----------
    fred_series : FRED_SERIES dict from config  (name -> series_id)
    bls_series  : BLS_SERIES dict from config   (name -> series_id)

    Returns
    -------
    DataFrame with columns: series_id, series_name, source
    One row per configured series (FRED rows first, then BLS).
    """
    rows = [
        {"series_id": sid, "series_name": name, "source": "FRED"}
        for name, sid in fred_series.items()
    ] + [
        {"series_id": sid, "series_name": name, "source": "BLS"}
        for name, sid in bls_series.items()
    ]
    return pd.DataFrame(rows, columns=["series_id", "series_name", "source"])