"""
Generate scenarios script
"""
import os
from federation.integrate_time_series import (
    generate_sql_tables_gries,
    generate_baseline_discharge_sql,
    generate_baseline_price_sql,
)
from federation.generate_scenarios import (
    query_time_series_data,
    fill_null_remove_outliers,
    generate_scenarios,
)
from federation.plot_time_series import plot_time_series, plot_scenarios
from schema.schema import (
    DischargeFlow,
    Irradiation,
    WindSpeed,
    Temperature,
    MarketPrice,
)

if __name__ == "__main__":
    DB_CACHE_FILE = r".cache/interim/time_series_schema.db"
    if not os.path.exists(DB_CACHE_FILE):
        db_cache_file_sql = os.path.join(f"sqlite:///{DB_CACHE_FILE}")
        generate_sql_tables_gries(restart_interim_data=True, write_sql=db_cache_file_sql)
        generate_baseline_discharge_sql(restart_interim_data=True, write_sql=db_cache_file_sql)
        generate_baseline_price_sql(restart_interim_data=True, write_sql=db_cache_file_sql)

    data = {
        "Irradiation": query_time_series_data(DB_CACHE_FILE, alt=2510, tables=[Irradiation])["Irradiation"],
        "WindSpeed": query_time_series_data(DB_CACHE_FILE, alt=2510, tables=[WindSpeed])["WindSpeed"],
        "Temperature": query_time_series_data(DB_CACHE_FILE, alt=2510, tables=[Temperature])["Temperature"],
        "DischargeFlow": query_time_series_data(DB_CACHE_FILE, river="Gletsch", tables=[DischargeFlow])["DischargeFlow"],
        "DA price": query_time_series_data(DB_CACHE_FILE, market="DA price", tables=[MarketPrice])["MarketPrice"],
        "Short price": query_time_series_data(DB_CACHE_FILE, market="Short price", tables=[MarketPrice])["MarketPrice"],
        "Long price": query_time_series_data(DB_CACHE_FILE, market="Long price", tables=[MarketPrice])["MarketPrice"],
        "FRR-pos": query_time_series_data(DB_CACHE_FILE, market="FRR-pos", tables=[MarketPrice])["MarketPrice"],
        "FRR-neg": query_time_series_data(DB_CACHE_FILE, market="FRR-neg", tables=[MarketPrice])["MarketPrice"],
        "RR-pos": query_time_series_data(DB_CACHE_FILE, market="RR-pos", tables=[MarketPrice])["MarketPrice"],
        "RR-neg": query_time_series_data(DB_CACHE_FILE, market="RR-neg", tables=[MarketPrice])["MarketPrice"],
        "FCR": query_time_series_data(DB_CACHE_FILE, market="FCR", tables=[MarketPrice])["MarketPrice"],
    }

    data_scenarios = {}
    for d in data:
        Z_SCORE = 4 if d in ["Irradiation", "Temperature"] else 10
        data[d] = fill_null_remove_outliers(data[d], d_time="1h", z_score=Z_SCORE)
        data_scenarios[d] = generate_scenarios(data[d])

    plot_time_series(data=data, fig_path=r".cache/figs/time_series_raw.html")
    plot_scenarios(data=data_scenarios, fig_path=r".cache/figs/time_series_scenarios.html")
