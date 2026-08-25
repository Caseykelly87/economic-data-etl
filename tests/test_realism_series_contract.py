"""Contract between the macro series catalog and the sim engine's realism layer.

The sim engine's Stage 2 queries raw.fact_economic_observations by
series_name and uses the database only when every name in its realism set
is present. The macro pipeline is what populates that table, so the
catalog in src/config.py carries the supply side of that contract:
eleven series under exact names, all monthly (FRED and BLS sources).

The annual ERS food-price forecasts are the wrong granularity for the
realism multiplier math, which is why they live under ERS_FORECAST_*
names — a forecast series accidentally reclaiming a realism name would
silently feed annual percentages into the sim engine's index-level math.
"""

from __future__ import annotations

from src.config import BLS_SERIES, ERS_CATEGORY_MAP, FRED_SERIES

# The realism layer's series names, mirrored from the sim engine's
# REALISM_SERIES constant (knot_shore/realism.py). Update both together.
REALISM_SERIES_NAMES = {
    "SENTIMENT",
    "UNRATE",
    "AVG_WAGES",
    "ERS_ALL_FOOD",
    "ERS_FOOD_HOME",
    "ERS_FOOD_AWAY",
    "ERS_CEREALS",
    "ERS_MEATS",
    "ERS_DAIRY",
    "ERS_FRUITS_VEG",
    "ERS_BEVERAGES",
}


def test_macro_catalog_supplies_every_realism_series_name():
    supplied = set(FRED_SERIES) | set(BLS_SERIES)
    missing = REALISM_SERIES_NAMES - supplied
    assert not missing, (
        f"macro catalog no longer supplies realism series {sorted(missing)}; "
        "the sim engine's database mode needs all of them present"
    )


def test_forecast_series_names_stay_off_the_realism_names():
    forecast_names = set(ERS_CATEGORY_MAP.values())
    collisions = forecast_names & REALISM_SERIES_NAMES
    assert not collisions, (
        f"ERS forecast series {sorted(collisions)} collide with realism "
        "series names; annual percentages would mix into monthly index data"
    )


def test_realism_food_series_use_bls_monthly_cpi_ids():
    """The eight ERS_-named realism series must resolve to BLS CUUR* CPI
    indexes — monthly index levels, the granularity the multiplier math
    needs."""
    for name in REALISM_SERIES_NAMES:
        if not name.startswith("ERS_"):
            continue
        assert name in BLS_SERIES
        assert BLS_SERIES[name].startswith("CUUR"), (
            f"{name} maps to {BLS_SERIES[name]}, expected a CUUR* CPI index"
        )
