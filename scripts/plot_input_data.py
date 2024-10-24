
import json

import polars as pl
from polars import col as c

from data_federation.input_model import SmallflexInputSchema
from plotly.subplots import make_subplots
import streamlit as st
from datetime import date
import plotly.graph_objs as go
from utility.general_function import pl_to_dict
from config import settings
import plotly.express as px
from itertools import product


if __name__=="__main__":
    output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES))
    small_flex_input_schema: SmallflexInputSchema = SmallflexInputSchema()\
        .duckdb_to_schema(file_path=output_file_names["duckdb_input"])
    
    colors = px.colors.qualitative.Plotly
    market_price_measurement = small_flex_input_schema.market_price_measurement\
    .filter(~((c("timestamp").dt.month() == 2) & (c("timestamp").dt.day() == 29)))\
    .with_columns(
        c("timestamp").dt.year().alias("year"),
        c("timestamp").dt.to_string(format="%m-%d %H:%M").alias("date_str"),
    )
    market_list = sorted(market_price_measurement["market"].unique().to_list())
    
    market_tabs = st.tabs(market_list)
    
    for market_tabs_idx, market in enumerate(market_list):
        with market_tabs[market_tabs_idx]:
            time_step = "1h" if market in ["DA", "IDA"] else "4h"
            date_mapping = pl_to_dict(
                pl.DataFrame(pl.datetime_range(
                    start=date(2022, 1, 1), end=date(2023, 1, 1), interval=time_step,
                    eager=True, closed="left").alias("timestamp")
                ).with_row_index("index")\
                .filter(~((c("timestamp").dt.month() == 2) & (c("timestamp").dt.day() == 29)))\
                .with_columns(
                    c("timestamp").dt.to_string(format="%m-%d %H:%M").alias("date_str"),
                )["date_str", "index"])

            actual_market = market_price_measurement.filter(c("market") == "mFRR-cap").with_columns(
                c("date_str").replace_strict(date_mapping, default=None).alias("index"),
            )
            country_list = actual_market["country"].unique().to_list()
            country_tabs = st.tabs(country_list)
            for country_tab_idx, country in enumerate(country_list):
                trace_list: list = []
                data_by_country = actual_market.filter(c("country")==country)
                year_list = sorted(data_by_country["year"].unique().to_list())
                direction_list = actual_market["direction"].unique()
                
                fig = make_subplots(rows=len(direction_list), cols = 1, subplot_titles=direction_list, shared_xaxes=True)
                for i, values in enumerate(product(year_list, direction_list)):
                    year = values[0]
                    direction = values[1]
                    fig_index = i % len(direction_list) + 1
                    
                    color_index= i//2
                    plot_data = data_by_country.filter(c("year") == year).filter(c("direction") == direction).sort("index")
                    fig.add_trace(
                        go.Scatter(
                            x=plot_data["index"].to_list(), y=plot_data["avg"].to_list(),
                            mode='lines', name=str(year), legendgroup=str(year),  line=dict(color=colors[color_index]),
                            showlegend=fig_index==1),
                        row=fig_index, col=1
                        )

                fig.update_layout(xaxis=dict(nticks=11))
                print(market_tabs_idx, country_tab_idx)
                with country_tabs[country_tab_idx]:
                    st.plotly_chart(fig, theme="streamlit", use_container_width=True)