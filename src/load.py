import pandas as pd
from sqlalchemy import text


def ensure_tables_exist(engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_economic_observations (
                series_id   TEXT NOT NULL,
                series_name TEXT NOT NULL,
                date        TEXT NOT NULL,
                value       REAL,
                source      TEXT NOT NULL,
                PRIMARY KEY (series_id, date)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_series (
                series_id   TEXT PRIMARY KEY,
                series_name TEXT NOT NULL,
                source      TEXT NOT NULL
            )
        """))
        conn.commit()


def _nan_equal(a, b) -> bool:
    """True when both values are NaN, or both are numerically equal."""
    a_nan = pd.isna(a)
    b_nan = pd.isna(b)
    if a_nan and b_nan:
        return True
    if a_nan or b_nan:
        return False
    return abs(float(a) - float(b)) < 1e-9


def _to_date_str(value) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def upsert_observations(df: pd.DataFrame, engine) -> dict:
    """
    Upsert a fact DataFrame into fact_economic_observations.

    Primary key: (series_id, date).
    NaN values are stored as NULL.

    Returns
    -------
    dict with keys: inserted, updated, unchanged
    """
    stats = {"inserted": 0, "updated": 0, "unchanged": 0}

    with engine.connect() as conn:
        existing = pd.read_sql(
            "SELECT series_id, date, value FROM fact_economic_observations", conn
        )

    existing_map = {
        (row["series_id"], str(row["date"])[:10]): row["value"]
        for _, row in existing.iterrows()
    }

    to_insert = []
    to_update = []

    for _, row in df.iterrows():
        key = (row["series_id"], _to_date_str(row["date"]))
        if key not in existing_map:
            to_insert.append(row)
            stats["inserted"] += 1
        elif _nan_equal(row["value"], existing_map[key]):
            stats["unchanged"] += 1
        else:
            to_update.append(row)
            stats["updated"] += 1

    if to_insert:
        insert_df = pd.DataFrame(to_insert)
        insert_df["date"] = insert_df["date"].apply(_to_date_str)
        insert_df.to_sql(
            "fact_economic_observations", engine, if_exists="append", index=False
        )

    if to_update:
        with engine.connect() as conn:
            for row in to_update:
                conn.execute(
                    text("""
                        UPDATE fact_economic_observations
                        SET value = :value, series_name = :series_name, source = :source
                        WHERE series_id = :series_id AND date = :date
                    """),
                    {
                        "series_id": row["series_id"],
                        "date": _to_date_str(row["date"]),
                        "value": None if pd.isna(row["value"]) else float(row["value"]),
                        "series_name": row["series_name"],
                        "source": row["source"],
                    },
                )
            conn.commit()

    return stats


def upsert_dim_series(df: pd.DataFrame, engine) -> dict:
    """
    Upsert a dimension DataFrame into dim_series.

    Primary key: series_id. Existing rows are never overwritten.

    Returns
    -------
    dict with keys: inserted, unchanged
    """
    stats = {"inserted": 0, "unchanged": 0}

    with engine.connect() as conn:
        existing = pd.read_sql("SELECT series_id FROM dim_series", conn)

    existing_ids = set(existing["series_id"]) if not existing.empty else set()

    new_rows = df[~df["series_id"].isin(existing_ids)]
    stats["unchanged"] = len(df) - len(new_rows)

    if not new_rows.empty:
        new_rows.to_sql("dim_series", engine, if_exists="append", index=False)
        stats["inserted"] = len(new_rows)

    return stats