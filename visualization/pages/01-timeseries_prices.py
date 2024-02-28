from federation.generate_scenarios import query_time_series_data
from federation.plot_time_series import plot_time_series
from schema.schema import (
    MarketPrice,
)
from sqlalchemy import create_engine
import polars as pl
import streamlit as st


if __name__ == "__main__":
    DB_CACHE_FILE = r".cache/interim/case.db"

    engine = create_engine(f"sqlite+pysqlite:///{DB_CACHE_FILE}", echo=False)
    con = engine.connect()
    markets = pl.read_database(query="""SELECT DISTINCT {f1}.market, {f1}.direction FROM {f1}""".format(f1=MarketPrice.__tablename__), connection=con).with_columns(pl.concat_str(["market", "direction"], separator="-").alias("name"))
    page_md = markets.select(["market", "direction"]).to_dicts()
    names = markets["name"].to_list()

    tabs1 = st.tabs(names)
    for i, market in enumerate(names):
        with tabs1[i]:
            countries = pl.read_database(query="""SELECT DISTINCT {f1}.market, {f1}.direction, {f1}.country FROM {f1} where ({f1}.market == \"{f2}\" and {f1}.direction == \"{f3}\")""".format(f1=MarketPrice.__tablename__, f2=page_md[i]["market"], f3=page_md[i]["direction"]), connection=con)["country"].to_list()
            tabs2 = st.tabs(countries)
            for j, country in enumerate(countries):
                with tabs2[j]:
                    sources = pl.read_database(query="""SELECT DISTINCT {f1}.market, {f1}.direction, {f1}.country, {f1}.source FROM {f1} where ({f1}.market == \"{f2}\" and {f1}.direction == \"{f3}\" and {f1}.country == \"{f4}\")""".format(f1=MarketPrice.__tablename__, f2=page_md[i]["market"], f3=page_md[i]["direction"], f4=country), connection=con)["source"].to_list()
                    tabs3 = st.tabs(sources)
                    for k, source in enumerate(sources):
                        with tabs3[k]:
                            all_raw_data = {market: pl.read_database(query="""SELECT {f1}.* FROM {f1} where ({f1}.market == \"{f2}\" and {f1}.direction == \"{f3}\" and {f1}.country == \"{f4}\" and {f1}.source == \"{f5}\")""".format(f1=MarketPrice.__tablename__, f2=page_md[i]["market"], f3=page_md[i]["direction"], f4=country, f5=source), connection=con)}
                            figs = plot_time_series(all_raw_data)
                            st.plotly_chart(figs[0], use_container_width=True, theme="streamlit")
        