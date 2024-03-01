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
    scen_number0 = 7
    results1 = initialize_time_series(DB_CACHE_FILE, DischargeFlow, scen_number=scen_number0)
    results2 = initialize_time_series(DB_CACHE_FILE, Irradiation, scen_number=scen_number0)
    results3 = initialize_time_series(DB_CACHE_FILE, Temperature, scen_number=scen_number0)
    results4 = initialize_time_series(DB_CACHE_FILE, WindSpeed, scen_number=scen_number0)

    mc0 = {
        "DA-sym": {"market": "DA", "direction": "sym", "country": "CH", "source": "alpiq"},
        "IDA-sym": {"market": "IDA", "direction": "sym", "country": "DE", "source": "alpiq"},
        "FCR-cap-sym": {"market": "FCR-cap", "direction": "sym", "country": "CH", "source": "regelleistung"},
        "aFRR-act-pos": {"market": "aFRR-act", "direction": "pos", "country": "AT", "source": "apg"},
        "aFRR-act-neg": {"market": "aFRR-act", "direction": "neg", "country": "AT", "source": "apg"},
        "aFRR-cap-pos": {"market": "aFRR-cap", "direction": "pos", "country": "AT", "source": "apg"},
        "aFRR-cap-neg": {"market": "aFRR-cap", "direction": "neg", "country": "AT", "source": "apg"},
        "mFRR-act-pos": {"market": "mFRR-act", "direction": "pos", "country": "FR", "source": "rte"},
        "mFRR-act-neg": {"market": "mFRR-act", "direction": "neg", "country": "FR", "source": "rte"},
        "mFRR-cap-pos": {"market": "mFRR-cap", "direction": "pos", "country": "CH", "source": "swissgrid"},
        "mFRR-cap-neg": {"market": "mFRR-cap", "direction": "neg", "country": "CH", "source": "swissgrid"},
        "RR-act-pos": {"market": "RR-act", "direction": "pos", "country": "FR", "source": "rte"},
        "RR-act-neg": {"market": "RR-act", "direction": "neg", "country": "FR", "source": "rte"},
        }

    results5 = initialize_time_series(DB_CACHE_FILE, MarketPrice, scen_number=scen_number0, market_combinations=mc0)
