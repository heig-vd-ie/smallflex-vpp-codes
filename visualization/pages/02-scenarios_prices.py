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
    
    scenario_data = {}
    for m in ["DA-sym", "IDA-sym", "FCR-cap-sym", "aFRR-act-pos", "aFRR-act-neg", "aFRR-cap-pos", "aFRR-cap-neg",
              "mFRR-act-pos", "mFRR-act-neg", "mFRR-cap-pos", "mFRR-cap-neg", "RR-act-pos", "RR-act-neg"]:
        scenario_data[m] = query_time_series_data(db_cache_file=DB_CACHE_FILE, market=m, tables=[MarketPriceNorm])["MarketPriceNorm"]

    figs = plot_scenarios(scenario_data, fig_path=".cache/figs/time_series_scenarios.html")
    tabs = st.tabs(scenario_data.keys())
    for i, fig in enumerate(figs):
        with tabs[i]:
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)
    