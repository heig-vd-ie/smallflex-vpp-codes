from federation.generate_scenarios import query_time_series_data
from federation.plot_time_series import plot_scenarios
from schema.schema import (
    MarketPriceNorm,
    IrradiationNorm,
    WindSpeedNorm,
    TemperatureNorm,
    DischargeFlowNorm
)
import streamlit as st

if __name__ == "__main__":
    DB_CACHE_FILE = r".cache/interim/case.db"
    
    scenario_data = {
        "Irradiation": query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[IrradiationNorm])["IrradiationNorm"],
        "WindSpeed": query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[WindSpeedNorm])["WindSpeedNorm"],
        "Temperature": query_time_series_data(db_cache_file=DB_CACHE_FILE, alt=2000, tables=[TemperatureNorm])["TemperatureNorm"],
        "DischargeFlow": query_time_series_data(db_cache_file=DB_CACHE_FILE, river="Gletsch", tables=[DischargeFlowNorm])["DischargeFlowNorm"],
        "DA price": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="DA price", tables=[MarketPriceNorm])["MarketPriceNorm"],
        "Short price": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="Short price", tables=[MarketPriceNorm])["MarketPriceNorm"],
        "Long price": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="Long price", tables=[MarketPriceNorm])["MarketPriceNorm"],
        "FRR-pos": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="FRR-pos", tables=[MarketPriceNorm])["MarketPriceNorm"],
        "FRR-neg": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="FRR-neg", tables=[MarketPriceNorm])["MarketPriceNorm"],
        "RR-pos": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="RR-pos", tables=[MarketPriceNorm])["MarketPriceNorm"],
        "RR-neg": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="RR-neg", tables=[MarketPriceNorm])["MarketPriceNorm"],
        "FCR": query_time_series_data(db_cache_file=DB_CACHE_FILE, market="FCR", tables=[MarketPriceNorm])["MarketPriceNorm"]
    }

    figs = plot_scenarios(scenario_data, fig_path=".cache/figs/time_series_scenarios.html")
    tabs = st.tabs(scenario_data.keys())
    for i, fig in enumerate(figs):
        with tabs[i]:
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)
    