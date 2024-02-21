from federation.generate_scenarios import query_time_series_data
from federation.plot_time_series import plot_time_series
from schema.schema import (
    Irradiation,
    MarketPrice,
    Temperature,
    WindSpeed,
    DischargeFlow,
)
import streamlit as st

if __name__ == "__main__":
    DB_CACHE_FILE = r".cache/interim/time_series_schema.db"
   
    tables = ["DischargeFlow", "Irradiation", "WindSpeed", "Temperature"]

    tabs = st.tabs(tables)
    for i, table in enumerate(tables):
        with tabs[i]:
            all_raw_data = {
                table: 
                query_time_series_data(db_cache_file=DB_CACHE_FILE, river="Gletsch", tables=[DischargeFlow])["DischargeFlow"] if table == "DischargeFlow" else
                query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[Irradiation])["Irradiation"] if table == "Irradiation" else
                query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[WindSpeed])["WindSpeed"] if table == "WindSpeed" else
                query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[Temperature])["Temperature"] if table == "Temperature" else
                None
                }
            figs = plot_time_series(all_raw_data)
            st.plotly_chart(figs[0], use_container_width=True, theme="streamlit")
