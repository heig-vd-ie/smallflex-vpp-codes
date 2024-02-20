from federation.generate_scenarios import query_time_series_data
from federation.plot_time_series import plot_time_series
from schema.schema import (
    MarketPrice,
)
from sqlalchemy import create_engine
import polars as pl
import streamlit as st


if __name__ == "__main__":
    DB_CACHE_FILE = r".cache/interim/time_series_schema.db"

    engine = create_engine(f"sqlite+pysqlite:///{DB_CACHE_FILE}", echo=False)
    con = engine.connect()
    markets = pl.read_database(query="""SELECT DISTINCT {f1}.market FROM {f1}""".format(f1=MarketPrice.__tablename__), connection=con)["market"].to_list()

    tabs = st.tabs(markets)
    for i, market in enumerate(markets):
        with tabs[i]:
            all_raw_data = {
                market: query_time_series_data(db_cache_file=DB_CACHE_FILE, market=market, tables=[MarketPrice])["MarketPrice"]
            }
            figs = plot_time_series(all_raw_data)
            st.plotly_chart(figs[0], use_container_width=True, theme="streamlit")
    