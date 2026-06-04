"""Staging and domain-mart build over the raw economic zone.

The raw landing tables (``raw.fact_economic_observations`` and
``raw.dim_series``) are produced by the load stage. This module completes the
layered warehouse: it conforms raw into a typed ``staging`` layer and projects
that staging layer into the domain marts the API serves
(``public_analytics.mart_*``).

Why staging is materialized rather than a view: SQLite — the dialect the test
suite runs on — cannot create a view in one attached database that references
a table in another ("view ... cannot reference objects in database raw"), so a
``staging`` view over ``raw`` is not portable. Materializing keeps one code
path that the SQLite suite exercises identically to PostgreSQL. Freshness is
not a concern because the staging and mart tables are fully rebuilt as a
deliberate, idempotent stage on every pipeline run; at this data volume
(low thousands of rows) the rebuild cost is negligible.

The marts are a full refresh (DELETE then INSERT-SELECT) rather than an
incremental upsert: they are deterministic projections of raw, so rebuilding
them from staging is both simpler and guaranteed correct, and it makes the
build idempotent with no duplicate-key risk on re-run.
"""

import logging

from sqlalchemy import bindparam, text

from src.config import MART_DOMAINS
from src.load import ensure_schemas

logger = logging.getLogger(__name__)


# Maps a domain key in MART_DOMAINS to its physical mart table name.
_DOMAIN_TABLE = {
    "inflation": "mart_inflation",
    "labor_market": "mart_labor_market",
    "gdp": "mart_gdp",
}

_STAGING_TABLE = "staging.stg_economic_observations"
_SUMMARY_TABLE = "public_analytics.mart_economic_summary"


_DDL_STAGING = """
    CREATE TABLE IF NOT EXISTS staging.stg_economic_observations (
        series_id        TEXT NOT NULL,
        series_name      TEXT NOT NULL,
        observation_date DATE NOT NULL,
        value            DOUBLE PRECISION,
        source           TEXT NOT NULL,
        PRIMARY KEY (series_id, observation_date)
    )
"""

# Domain marts share one shape; the column list and PK match the API's
# SQLAlchemy models (MartInflation / MartLaborMarket / MartGdp) exactly so the
# API can read them. source is nullable to match that contract.
_DDL_DOMAIN_MART = """
    CREATE TABLE IF NOT EXISTS public_analytics.{table} (
        series_id        TEXT NOT NULL,
        observation_date DATE NOT NULL,
        series_name      TEXT NOT NULL,
        value            DOUBLE PRECISION,
        source           TEXT,
        PRIMARY KEY (series_id, observation_date)
    )
"""

# Summary has its own shape: one row per series carrying the latest
# observation. Matches the API's MartEconomicSummary model exactly.
_DDL_SUMMARY = """
    CREATE TABLE IF NOT EXISTS public_analytics.mart_economic_summary (
        series_id    TEXT PRIMARY KEY,
        series_name  TEXT NOT NULL,
        source       TEXT,
        latest_date  DATE,
        latest_value DOUBLE PRECISION
    )
"""


def _observation_date_expr(dialect: str) -> str:
    """SQL expression that conforms raw.date (ISO text) to observation_date.

    PostgreSQL casts the ISO text to a real DATE. SQLite must NOT cast: its
    DATE affinity is NUMERIC, so ``CAST('2024-01-01' AS DATE)`` returns the
    integer 2024 and silently corrupts the date. The bare column keeps the
    ISO ``YYYY-MM-DD`` text, which the API's SQLAlchemy Date type parses back
    to a date the same way it already reads raw.date.
    """
    return "CAST(date AS DATE)" if dialect == "postgresql" else "date"


def _table_count(conn, table: str) -> int:
    return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()


def build_marts(engine) -> dict:
    """Build the staging layer and the four domain marts from raw.

    Reads ``raw.fact_economic_observations``, conforms it into
    ``staging.stg_economic_observations`` (typed observation_date), and
    projects that staging layer into the marts the API consumes:
    ``public_analytics.mart_inflation``, ``mart_labor_market``, ``mart_gdp``
    (domain subsets by series_id), and ``mart_economic_summary`` (latest
    observation for every series).

    Idempotent full refresh — safe to re-run; produces identical marts with
    no row duplication.

    Returns
    -------
    dict mapping each built table to its row count, e.g.
    ``{"staging": 1234, "mart_inflation": 400, ..., "mart_economic_summary": 23}``.
    """
    ensure_schemas(engine)
    dialect = engine.dialect.name
    obs_date = _observation_date_expr(dialect)

    stats: dict[str, int] = {}

    with engine.connect() as conn:
        # --- DDL (idempotent) ---
        conn.execute(text(_DDL_STAGING))
        for table in _DOMAIN_TABLE.values():
            conn.execute(text(_DDL_DOMAIN_MART.format(table=table)))
        conn.execute(text(_DDL_SUMMARY))

        # --- Staging: conform raw into a typed observation_date ---
        conn.execute(text("DELETE FROM staging.stg_economic_observations"))
        conn.execute(text(f"""
            INSERT INTO staging.stg_economic_observations
                (series_id, series_name, observation_date, value, source)
            SELECT series_id, series_name, {obs_date} AS observation_date, value, source
            FROM raw.fact_economic_observations
        """))
        stats["staging"] = _table_count(conn, _STAGING_TABLE)

        # --- Domain marts: project staging filtered to each domain ---
        for domain, table in _DOMAIN_TABLE.items():
            series_ids = MART_DOMAINS.get(domain, [])
            conn.execute(text(f"DELETE FROM public_analytics.{table}"))
            if series_ids:
                stmt = text(f"""
                    INSERT INTO public_analytics.{table}
                        (series_id, observation_date, series_name, value, source)
                    SELECT series_id, observation_date, series_name, value, source
                    FROM staging.stg_economic_observations
                    WHERE series_id IN :ids
                """).bindparams(bindparam("ids", expanding=True))
                conn.execute(stmt, {"ids": series_ids})
            stats[table] = _table_count(conn, f"public_analytics.{table}")

        # --- Summary: the latest observation for every series (no-loss) ---
        conn.execute(text("DELETE FROM public_analytics.mart_economic_summary"))
        conn.execute(text("""
            INSERT INTO public_analytics.mart_economic_summary
                (series_id, series_name, source, latest_date, latest_value)
            SELECT s.series_id, s.series_name, s.source,
                   s.observation_date, s.value
            FROM staging.stg_economic_observations AS s
            JOIN (
                SELECT series_id, MAX(observation_date) AS max_date
                FROM staging.stg_economic_observations
                GROUP BY series_id
            ) AS latest
              ON s.series_id = latest.series_id
             AND s.observation_date = latest.max_date
        """))
        stats["mart_economic_summary"] = _table_count(conn, _SUMMARY_TABLE)

        conn.commit()

    logger.info(
        "marts built",
        extra={"source": "marts", "dialect": dialect, "row_counts": str(stats)},
    )
    return stats
