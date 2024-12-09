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
   
    all_raw_data = {
        "Irradiation": query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[Irradiation])["Irradiation"],
        "WindSpeed": query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[WindSpeed])["WindSpeed"],
        "Temperature": query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[Temperature])["Temperature"],
        "DischargeFlow": query_time_series_data(db_cache_file=DB_CACHE_FILE, river="Gletsch", tables=[DischargeFlow])["DischargeFlow"],
        "DA price": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="DA price", tables=[MarketPrice])["MarketPrice"],
        "Short price": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="Short price", tables=[MarketPrice])["MarketPrice"],
        "Long price": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="Long price", tables=[MarketPrice])["MarketPrice"],
        "FRR-pos": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="FRR-pos", tables=[MarketPrice])["MarketPrice"],
        "FRR-neg": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="FRR-neg", tables=[MarketPrice])["MarketPrice"],
        "RR-pos": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="RR-pos", tables=[MarketPrice])["MarketPrice"],
        "RR-neg": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="RR-neg", tables=[MarketPrice])["MarketPrice"],
        "FCR": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="FCR", tables=[MarketPrice])["MarketPrice"]
    }

    figs = plot_time_series(all_raw_data, fig_path=".cache/figs/time_series_raw.html")
    tabs = st.tabs(all_raw_data.keys())
    for i, fig in enumerate(figs):
        with tabs[i]:
            st.plotly_chart(fig, theme="streamlit")
    