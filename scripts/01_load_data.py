"""
Load data script
"""
import os
from federation.integrate_time_series_2 import (
    generate_sql_tables_gries,
    generate_baseline_discharge_sql,
    generate_baseline_price_sql,
)


if __name__ == "__main__":
    DB_CACHE_FILE = r".cache/interim/time_series_schema.db"
    db_cache_file_sql = os.path.join(f"sqlite:///{DB_CACHE_FILE}")
    generate_sql_tables_gries(restart_interim_data=True, write_sql=db_cache_file_sql)
    generate_baseline_discharge_sql(restart_interim_data=True, write_sql=db_cache_file_sql)
    generate_baseline_price_sql(restart_interim_data=True, write_sql=db_cache_file_sql)
