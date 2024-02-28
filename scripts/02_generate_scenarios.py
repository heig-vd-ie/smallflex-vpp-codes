"""
Generate scenarios script
"""
from federation.generate_scenarios import initialize_time_series
from schema.schema import (
    DischargeFlow,
    Irradiation,
    WindSpeed,
    Temperature,
    MarketPrice,
)


if __name__ == "__main__":
    DB_CACHE_FILE = r".cache/interim/case.db"
    results1 = initialize_time_series(DB_CACHE_FILE, DischargeFlow)
    results2 = initialize_time_series(DB_CACHE_FILE, MarketPrice)
    results3 = initialize_time_series(DB_CACHE_FILE, Irradiation)
    results4 = initialize_time_series(DB_CACHE_FILE, Temperature)
    results5 = initialize_time_series(DB_CACHE_FILE, WindSpeed)
