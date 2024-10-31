
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
import tqdm

from data_display.input_data_plots import (
    plot_market_prices, plot_discharge_flow, plot_power, plot_basin_height, plot_basin_height_volume_table,
    plot_continuous_performance_table, plot_discrete_performance_table
)

COLORS = px.colors.qualitative.Plotly

@st.cache_data
def generate_input_data_display() -> dict:
    output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES))
    small_flex_input_schema: SmallflexInputSchema = SmallflexInputSchema()\
        .duckdb_to_schema(file_path=output_file_names["duckdb_input"])
    plot_dict: dict = {}
    plot_dict["Market price"] = generate_market_price_display(data=small_flex_input_schema.market_price_measurement)
    plot_dict["Power plants"] = generate_power_plants_display(small_flex_input_schema=small_flex_input_schema)
    plot_dict["Measurements"] = generate_measurement_display(small_flex_input_schema=small_flex_input_schema)
    
    return plot_dict
    
def generate_market_price_display(data: pl.DataFrame) -> dict:
    
    plot_dict: dict = dict()
    
    data = times_series_preprocessing(data)

    for market in tqdm.tqdm(
        sorted(data["market"].unique().to_list()), desc="Generate market plots", ncols=150):
        
        plot_dict[market]= {}
        time_step = "1h" if market in ["DA", "IDA"] else "4h"
        
        date_mapping = generate_date_mapping(time_step=time_step)

        actual_data = data.filter(c("market") == market).with_columns(
            c("date_str").replace_strict(date_mapping, default=None).alias("index"),
        )
        for country in actual_data["country"].unique().to_list():
            fig = plot_market_prices(data = actual_data.filter(c("country")==country))
            plot_dict[market][country] = fig
    return plot_dict


def generate_power_plants_display(small_flex_input_schema: SmallflexInputSchema) -> dict:
    plot_dict: dict = dict()
    plot_dict["basin height per volume"] = plot_basin_height_volume_table(small_flex_input_schema=small_flex_input_schema)
    plot_dict["Hydro power performance table"] = plot_power_performance_table(small_flex_input_schema=small_flex_input_schema)
    
    return plot_dict

def generate_measurement_display(small_flex_input_schema: SmallflexInputSchema) -> dict:
    plot_dict: dict = {}
    plot_dict["Discharge flow"] = generate_discharge_flow_display(data=small_flex_input_schema.discharge_flow_measurement)
    plot_dict["Basin height"] = generate_basins_display(small_flex_input_schema=small_flex_input_schema)
    plot_dict["Power production"] = generate_power_measurement(small_flex_input_schema=small_flex_input_schema)
    return plot_dict

def generate_power_measurement(small_flex_input_schema: SmallflexInputSchema):
    
    plot_dict: dict = dict()
    name_mapping = pl_to_dict(small_flex_input_schema.wind_power_plant[["uuid", "name"]])
    name_mapping.update(pl_to_dict(small_flex_input_schema.hydro_power_plant[["uuid", "name"]]))

    data = small_flex_input_schema.power_production_measurement.with_columns(
        c("power_plant_fk").replace_strict(name_mapping, default=None).alias("name")
    )
    
    
    data = times_series_preprocessing(data)
    date_mapping = generate_date_mapping(time_step="15m")
    for name in tqdm.tqdm(
            sorted(data["name"].unique().to_list()), desc="Generate discharge_flow plots", ncols=150):
        
        actual_data = data.filter(c("name") == name).with_columns(
                c("date_str").replace_strict(date_mapping, default=None).alias("index"),
            )
        plot_dict[name] = plot_power(data = actual_data)
    return plot_dict

def generate_discharge_flow_display(data: pl.DataFrame) -> dict:
    plot_dict: dict = dict()
    data = times_series_preprocessing(data)
    date_mapping = generate_date_mapping(time_step="1h")
    for river in tqdm.tqdm(
            sorted(data["river"].unique().to_list()), desc="Generate discharge_flow plots", ncols=150):
        
        actual_data = data.filter(c("river") == river).with_columns(
                c("date_str").replace_strict(date_mapping, default=None).alias("index"),
            )
        plot_dict[river] = plot_discharge_flow(data = actual_data)
    return plot_dict



def plot_power_performance_table(small_flex_input_schema: SmallflexInputSchema) -> dict:
    plot_dict: dict = dict()
    hydro_power_performance_table: pl.DataFrame = small_flex_input_schema.hydro_power_performance_table
    power_plant_fk = hydro_power_performance_table["power_plant_fk"].unique().to_list()
    
    for metadata in small_flex_input_schema.hydro_power_plant.filter(c("uuid").is_in(power_plant_fk)).to_dicts():
        data = hydro_power_performance_table.filter(c("power_plant_fk")==metadata["uuid"])
        if metadata["control"] == "discrete":
            plot_dict[metadata["name"]] = plot_discrete_performance_table(data)
        elif metadata["control"] == "continuous":
            plot_dict[metadata["name"]] = plot_continuous_performance_table(data)
        
    return plot_dict

def generate_basins_display(small_flex_input_schema: SmallflexInputSchema) -> dict:
    plot_dict: dict = dict()
    name_mapping = pl_to_dict(small_flex_input_schema.water_basin[["uuid", "name"]])

    data = small_flex_input_schema.basin_height_measurement.with_columns(
        c("timestamp").dt.to_string(format="%Y-%m-%d %H:%M").alias("date_str"),
        c("timestamp").dt.year().alias("year"),
        c("water_basin_fk").replace_strict(name_mapping, default=None).alias("name")    
    )
    for name in tqdm.tqdm(
            sorted(data["name"].unique().to_list()), desc="Generate discharge_flow plots", ncols=150):
        actual_data = data.filter(c("name") == name)
        plot_dict[name] = plot_basin_height(data = actual_data)
    return plot_dict
    
def generate_date_mapping(time_step: str)-> dict:
    date_mapping = pl_to_dict(
        pl.DataFrame(pl.datetime_range(
            start=date(2022, 1, 1), end=date(2023, 1, 1), interval=time_step,
            eager=True, closed="left").alias("timestamp")
        ).with_row_index("index")\
        .filter(~((c("timestamp").dt.month() == 2) & (c("timestamp").dt.day() == 29)))\
        .with_columns(
            c("timestamp").dt.to_string(format="%m-%d %H:%M").alias("date_str"),
        )["date_str", "index"])
    return date_mapping

def times_series_preprocessing(data: pl.DataFrame) -> pl.DataFrame:
    return (
        data
        .filter(~((c("timestamp").dt.month() == 2) & (c("timestamp").dt.day() == 29)))\
        .with_columns(
            c("timestamp").dt.year().alias("year"),
            c("timestamp").dt.to_string(format="%y-%m-%d %Hh").alias("displayed_date"),
            c("timestamp").dt.to_string(format="%m-%d %H:%M").alias("date_str"),
        )
    )


if __name__=="__main__":
    st.set_page_config(page_title="SmallFlex input data", page_icon="📊", layout="wide")
    plot_dict = generate_input_data_display()
    # Create two columns, one for the "tabs" and one for the content
    
    st.title("SmallFlex input data")
    main_tabs = st.columns([1, 7])

    # Simulate tabs on the left by creating buttons or radio options
    with main_tabs[0]:
        st.header("Data Type")
        first_level_tabs = st.radio("Data Type", ["Market price", "Power plants", "Measurements"], index=0, label_visibility="collapsed")
        
    with main_tabs[1]:
        if first_level_tabs == "Market price":
            st.header("Market prices")
            market_plot_dict = plot_dict["Market price"]
            market_tabs = st.tabs(list(market_plot_dict.keys()))
            for market_tabs_idx, market in enumerate(market_plot_dict.keys()):
                with market_tabs[market_tabs_idx]:
                    st.write("Countries")
                    country_tabs = st.tabs(list(market_plot_dict[market].keys()))
                    for country_tab_idx, country in enumerate(market_plot_dict[market].keys()):
                        with country_tabs[country_tab_idx]:
                            key = "market_price_"  +str(market_tabs_idx) + "_" + str(country_tab_idx)
                            st.plotly_chart(market_plot_dict[market][country], theme="streamlit", key = key)
                            
    with main_tabs[1]:
        if first_level_tabs == "Power plants":
            st.header("Power plants")
        
            power_plants_tabs = st.tabs(list(plot_dict["Power plants"].keys()))
            for power_plants_tabs_idx, power_plants in enumerate(plot_dict["Power plants"].keys()):
                with power_plants_tabs[power_plants_tabs_idx]:
                    if isinstance(plot_dict["Power plants"][power_plants], dict):
                        power_plants_sub_tabs = st.tabs(list(plot_dict["Power plants"][power_plants].keys()))
                        for sub_tab_idx, sub_power_plants in enumerate(plot_dict["Power plants"][power_plants].keys()):
                            with power_plants_sub_tabs[sub_tab_idx]:
                                key = "power_plants"  +str(power_plants_tabs_idx) + "_" + str(sub_tab_idx)
                                st.plotly_chart(plot_dict["Power plants"][power_plants][sub_power_plants], theme="streamlit", key = key)
                    else:
                        key = "power_plant"  +str(power_plants_tabs_idx) + "_0" 
                        st.plotly_chart(plot_dict["Power plants"][power_plants], theme="streamlit", key = key)
    
    with main_tabs[1]:
        if first_level_tabs == "Measurements":
            st.header("Measurements")
            meas_plot_dict = plot_dict["Measurements"]
            meas_tabs = st.tabs(list(meas_plot_dict.keys()))
            for meas_tabs_idx, meas in enumerate(meas_plot_dict.keys()):
                with meas_tabs[meas_tabs_idx]:
                    if isinstance(meas_plot_dict[meas], dict):
                        meas_sub_tabs = st.tabs(list(meas_plot_dict[meas].keys()))
                        for sub_tab_idx, sub_meas in enumerate(meas_plot_dict[meas].keys()):
                            with meas_sub_tabs[sub_tab_idx]:
                                key = "measurements"  +str(meas_tabs_idx) + "_" + str(sub_tab_idx)
                                st.plotly_chart(meas_plot_dict[meas][sub_meas], theme="streamlit", key = key)
                    else:
                        key = "measurements"  +str(meas_tabs_idx) + "_0" 
                        st.plotly_chart(meas_plot_dict[meas], theme="streamlit", key = key)
                            

