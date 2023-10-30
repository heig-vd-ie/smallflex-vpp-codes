from federation.integrate_time_series import generate_sql_tables_gries, generate_baseline_discharge_sql, generate_baseline_price_sql
from federation.generate_scenarios import query_time_series_data, fill_null_remove_outliers, generate_scenarios
from federation.plot_time_series import plot_time_series
from schema.schema import DischargeFlow, Irradiation, WindSpeed, Temperature, MarketPrice
import os

if __name__ == "__main__":
    db_cache_file = r".cache/interim/time_series_schema.db"
    if not os.path.exists(db_cache_file):
        db_cache_file_sql = os.path.join(f"sqlite:///", db_cache_file)
        generate_sql_tables_gries(restart_interim_data=True, write_sql=db_cache_file_sql)
        generate_baseline_discharge_sql(restart_interim_data=True, write_sql=db_cache_file_sql)
        generate_baseline_price_sql(restart_interim_data=True, write_sql=db_cache_file_sql)

    data = {
        # "Irradiation": query_time_series_data(db_cache_file, alt=2510, tables=[Irradiation])["Irradiation"],
        # "WindSpeed": query_time_series_data(db_cache_file, alt=2510, tables=[WindSpeed])["WindSpeed"],
        # "Temperature": query_time_series_data(db_cache_file, alt=2510, tables=[Temperature])["Temperature"],
        # "DischargeFlow": query_time_series_data(db_cache_file, river="Gletsch", tables=[DischargeFlow])["DischargeFlow"],
        "DA price": query_time_series_data(db_cache_file, market="DA price", tables=[MarketPrice])["MarketPrice"],
        "Short price": query_time_series_data(db_cache_file, market="Short price", tables=[MarketPrice])["MarketPrice"],
        "Long price": query_time_series_data(db_cache_file, market="Long price", tables=[MarketPrice])["MarketPrice"],
        "FRR-pos": query_time_series_data(db_cache_file, market="FRR-pos", tables=[MarketPrice])["MarketPrice"],
        "FRR-neg": query_time_series_data(db_cache_file, market="FRR-neg", tables=[MarketPrice])["MarketPrice"],
        "RR-pos": query_time_series_data(db_cache_file, market="RR-pos", tables=[MarketPrice])["MarketPrice"],
        "RR-neg": query_time_series_data(db_cache_file, market="RR-neg", tables=[MarketPrice])["MarketPrice"],
        "FCR": query_time_series_data(db_cache_file, market="FCR", tables=[MarketPrice])["MarketPrice"],
    }

    data_scenarios = {}
    for d in data.keys():
        z_score = 4 if d in ["Irradiation", "Temperature"] else 10
        data[d] = fill_null_remove_outliers(data[d], d_time="1h", z_score=z_score)
        data_scenarios[d] = generate_scenarios(data[d])

    plot_time_series(data=data, fig_path=r".cache/figs/time_series_raw.html")
    plot_time_series(data=data_scenarios, fig_path=r".cache/figs/time_series_scenarios.html")
