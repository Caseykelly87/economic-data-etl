import logging
from pathlib import Path
from weakref import WeakSet

import pandas as pd
from sqlalchemy import event, text
from sqlalchemy.engine import Engine


# Macro pipeline uses stdlib logging rather than structlog. The grocery
# pipeline (sim_ingest.py, sim_transform.py, detect_*.py) uses structlog
# directly; the asymmetry is intentional and documented in observability.py.
logger = logging.getLogger(__name__)


# Tracks SQLAlchemy engines that have had the SQLite raw-attach connect
# listener wired up. WeakSet so a disposed engine is garbage-collected
# without lingering in this registry.
_wired_engines: WeakSet[Engine] = WeakSet()


def _sqlite_raw_target(engine_url) -> str:
    """Resolve the file (or :memory:) to attach as the `raw` schema.

    In-memory engines attach another `:memory:` database — the schema lives
    for the engine's lifetime, which matches the test fixture pattern.
    File-based engines attach a sibling file named `<stem>_raw.db` next to
    the main database, so persistence semantics match the main file.
    """
    db = engine_url.database
    if db in (None, "", ":memory:"):
        return ":memory:"
    main_path = Path(db)
    return (main_path.parent / f"{main_path.stem}_raw.db").as_posix()


def _wire_sqlite_raw_attach(engine: Engine) -> None:
    """Register a connect listener that ATTACHes the raw database for SQLite.

    SQLite has no native schemas — `raw.<table>` is interpreted as a table
    in an attached database named `raw`. The listener fires on every new
    pooled connection so callers see the schema regardless of pool churn.
    Idempotent — tracked via the module-level _wired_engines WeakSet so
    repeated calls on the same engine instance are no-ops without
    attaching an arbitrary attribute to the third-party Engine class.
    """
    if engine in _wired_engines:
        return
    target = _sqlite_raw_target(engine.url)

    @event.listens_for(engine, "connect")
    def _attach_raw(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        try:
            cur.execute(f"ATTACH DATABASE '{target}' AS raw")
        finally:
            cur.close()

    _wired_engines.add(engine)


def ensure_tables_exist(engine) -> None:
    """Create the `raw` schema and the two raw tables if they don't exist.

    PostgreSQL: executes ``CREATE SCHEMA IF NOT EXISTS raw`` so the tables
    land in the layered-warehouse `raw` zone (raw → staging → marts).
    SQLite: attaches a sibling (or in-memory) database as `raw`, giving the
    same ``raw.<table>`` SQL syntax across dialects.

    Idempotent — safe to call repeatedly on the same engine.
    """
    dialect = engine.dialect.name

    if dialect == "sqlite":
        _wire_sqlite_raw_attach(engine)

    with engine.connect() as conn:
        if dialect == "postgresql":
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            logger.info(
                "raw schema ensured",
                extra={"source": "load", "dialect": dialect, "schema": "raw"},
            )
        elif dialect == "sqlite":
            # Safety net: if a connection from the pool predated the listener
            # (e.g. ensure_tables_exist is called after the engine was already
            # connected elsewhere), attach on the current connection too.
            attached = {row[1] for row in conn.execute(text("PRAGMA database_list"))}
            if "raw" not in attached:
                conn.execute(
                    text(f"ATTACH DATABASE '{_sqlite_raw_target(engine.url)}' AS raw")
                )
            logger.info(
                "raw schema attached",
                extra={
                    "source": "load",
                    "dialect": dialect,
                    "schema": "raw",
                    "target": _sqlite_raw_target(engine.url),
                },
            )

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.fact_economic_observations (
                series_id   TEXT NOT NULL,
                series_name TEXT NOT NULL,
                date        TEXT NOT NULL,
                value       DOUBLE PRECISION,
                source      TEXT NOT NULL,
                PRIMARY KEY (series_id, date)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.dim_series (
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
    Upsert a fact DataFrame into raw.fact_economic_observations.

    Primary key: (series_id, date).
    NaN values are stored as NULL.

    Returns
    -------
    dict with keys: inserted, updated, unchanged
    """
    stats = {"inserted": 0, "updated": 0, "unchanged": 0}

    # NOTE: loads the full table into memory for comparison.
    # Acceptable for small datasets; revisit if row counts grow large.
    with engine.connect() as conn:
        existing = pd.read_sql(
            "SELECT series_id, date, value FROM raw.fact_economic_observations", conn
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
            "fact_economic_observations",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
        )

    if to_update:
        with engine.connect() as conn:
            for row in to_update:
                conn.execute(
                    text("""
                        UPDATE raw.fact_economic_observations
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
    Upsert a dimension DataFrame into raw.dim_series.

    Primary key: series_id. Existing rows are never overwritten.
    Existing dim rows are never overwritten — series metadata is stable.

    Returns
    -------
    dict with keys: inserted, unchanged
    """
    stats = {"inserted": 0, "unchanged": 0}

    # NOTE: loads the full table into memory for comparison.
    # Acceptable for small datasets; revisit if row counts grow large.
    with engine.connect() as conn:
        existing = pd.read_sql("SELECT series_id FROM raw.dim_series", conn)

    existing_ids = set(existing["series_id"]) if not existing.empty else set()

    new_rows = df[~df["series_id"].isin(existing_ids)]
    stats["unchanged"] = len(df) - len(new_rows)

    if not new_rows.empty:
        new_rows.to_sql(
            "dim_series",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
        )
        stats["inserted"] = len(new_rows)

    return stats
