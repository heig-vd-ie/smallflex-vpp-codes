from federation.integrate_time_series import generate_sql_tables_gries, generate_baseline_discharge_sql
from federation.generate_scenarios import query_time_series_data, fill_null_remove_outliers, plot_meteo_data
import os

if __name__ == "__main__":
    db_cache_file = r".cache/interim/time_series_schema.db"
    if not os.path.exists(db_cache_file):
        db_cache_file_sql = os.path.join(f"sqlite:///", db_cache_file)
        generate_sql_tables_gries(restart_interim_data=True, write_sql=db_cache_file_sql)
        generate_baseline_discharge_sql(restart_interim_data=True, write_sql=db_cache_file_sql)
    data = query_time_series_data(db_cache_file, alt=2510, river="Gletsch")
    data = fill_null_remove_outliers(data, d_time="1h", z_score=4)
    fig = plot_meteo_data(fig_path=r".cache/figs/time_series_raw.html", data=data)
