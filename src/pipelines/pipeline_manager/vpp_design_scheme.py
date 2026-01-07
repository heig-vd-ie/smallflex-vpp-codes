import os

import json
import numpy as np
import polars as pl
from polars  import col as c
from polars import selectors as cs
import pyomo.environ as pyo
from datetime import timedelta, datetime
from itertools import product
from tqdm.auto import tqdm
from typing import Optional, Literal

from general_function import pl_to_dict, build_non_existing_dirs, dict_to_duckdb

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig
from pipelines.model_manager.stochastic_first_stage import StochasticFirstStage
from pipelines.model_manager.stochastic_second_stage import StochasticSecondStage

from timeseries_preparation.read_custom_file_timeseries import read_and_validate_custom_file

from data_display.baseline_plots import plot_scenario_results, plot_second_stage_result, plot_first_stage_result

from pipelines.pipeline_manager.first_stage_stochastic_pipeline import first_stage_stochastic_pipeline
from pipelines.pipeline_manager.second_stage_stochastic_pipeline import second_stage_stochastic_pipeline


from utility.data_preprocessing import (
    print_pl
)

from config import settings


HYDROPOWER_MASK = {
    "DT": c("name").is_in(["Aegina discrete turbine"]),
    "DTP": c("name").is_in(["Aegina discrete turbine", "Aegina pump"]),
    "CTP": c("name").is_in(["Aegina continuous turbine", "Aegina pump"]),
}
BATTERY_SIZE = {
    "0MW": {"rated_power": 0, "capacity": 0},
    "1MW_2MWh": {"rated_power": 1, "capacity": 2},
    "2MW_4MWh": {"rated_power": 2, "capacity": 4},
    "5MW_10MWh": {"rated_power": 5, "capacity": 10},
    "1MW_4MWh": {"rated_power": 1, "capacity": 4},
    "2MW_8MWh": {"rated_power": 2, "capacity": 8},
    "5MW_20MWh": {"rated_power": 5, "capacity": 20},
}

MARKET = ["DA", "FRC", "Imbalance"]
HYDROPOWER_MASK_LIST: list[Literal["DT", "DTP", "CTP"]] =["DT", "DTP", "CTP", "CTP", "CTP"]
MARKET_LIST: list[Literal['DA', 'FCR', 'Imbalance']]  = ['DA', 'DA', 'DA', 'FCR', 'Imbalance']

def set_config(data_config: DataConfig, market: list[Literal['DA', 'FCR', 'Imbalance']], battery_size) -> DataConfig:
    if market == "FCR":
        data_config.with_ancillary = True
        data_config.hydro_participation_to_imbalance = False
        data_config.battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
        data_config.battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
        data_config.imbalance_battery_rated_power = 0
        data_config.imbalance_battery_capacity = 0
    elif market == "Imbalance":
        data_config.with_ancillary = False
        data_config.hydro_participation_to_imbalance = True
        data_config.battery_rated_power = 0
        data_config.battery_capacity = 0
        data_config.imbalance_battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
        data_config.imbalance_battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
    elif market == "DA":
        data_config.with_ancillary = False
        data_config.hydro_participation_to_imbalance = False
        data_config.battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
        data_config.battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
        data_config.imbalance_battery_rated_power = 0
        data_config.imbalance_battery_capacity = 0
    else: 
        raise ValueError(f"Market {market} not recognized.")
    return data_config
    

def vpp_design_scheme(market_price_file_name: str | None = None, design_name: str="vpp_design_scheme") -> None:

    if market_price_file_name is not None:
        custom_market_prices: pl.DataFrame = read_and_validate_custom_file(file_name=market_price_file_name)
    else:
        custom_market_prices = None

    smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(file_path=settings.input_files.duckdb_input)

    data_config: DataConfig = DataConfig(
        nb_scenarios=200,
        total_scenarios_synthesized=smallflex_input_schema.discharge_volume_synthesized["scenario"].max(), # type: ignore
    )

    output_folder = f"{settings.output_files.output}/{design_name}"
    plot_folder = f"{settings.output_files.results_plot}/{design_name}"
    build_non_existing_dirs(output_folder)
    build_non_existing_dirs(plot_folder)
    
    # First stage: compute basin volume expectation for different hydropower masks#####################################
    basin_volume_expectation_dict: dict[Literal["DT", "DTP", "CTP"], pl.DataFrame] = {}
    pbar = tqdm(set(HYDROPOWER_MASK_LIST), position=0, leave=True)
    for hydro in pbar:
        pbar.set_description(f"First stage optimization problem with {hydro} hydropower plant")
        _, basin_volume_expectation, _ = first_stage_stochastic_pipeline(
                data_config=data_config,
                smallflex_input_schema=smallflex_input_schema,
                hydro_power_mask=HYDROPOWER_MASK[hydro],
                plot_result=False,
                custom_market_prices=custom_market_prices
            )
        basin_volume_expectation_dict[hydro] = basin_volume_expectation
        
    
    results_data = {}
    income_list: list = []
    scenario_list = list(map(
        lambda x: list(x[0]) + [x[1]], 
        list(product(*[list(zip(HYDROPOWER_MASK_LIST, MARKET_LIST)), list(BATTERY_SIZE.keys())]))
        ))

    pbar = tqdm(scenario_list, desc=f"Optimization", position=0)
    for hydro, market, battery_size in pbar:
        pbar.set_description(f"Optimization {hydro} hydro and {market} market with {battery_size}")
        scenario_name = "_".join([hydro, market, battery_size])
        
        data_config: DataConfig = set_config(data_config=data_config, market=market, battery_size=battery_size)
            
        
        second_stage_optimization_results, adjusted_income, fig = second_stage_stochastic_pipeline(
                data_config=data_config,
                smallflex_input_schema=smallflex_input_schema,
                basin_volume_expectation=basin_volume_expectation,
                hydro_power_mask=HYDROPOWER_MASK[hydro],
                plot_result=True,
                custom_market_prices=custom_market_prices
                )

        income_list.append((hydro + " " + market, battery_size, adjusted_income/1e3))
        
        if fig is not None:
            fig.write_html(f"{plot_folder}/{scenario_name}_results.html")

        results_data[scenario_name] = second_stage_optimization_results
        
        second_stage_optimization_results\
            .with_columns(c("timestamp").dt.to_string(format="%Y-%m-%d %H:%M:%S").alias("timestamp"))\
            .write_csv(f"{output_folder}/{scenario_name}_results.csv")
        
    results_data["adjusted_income"] = pl.DataFrame(
        income_list, schema=["market", "battery_size", "adjusted_income"], orient="row"
    ).pivot(on="market", index="battery_size", values="adjusted_income")

    results_data["adjusted_income"].write_csv(f"{output_folder}/summarized_income_results.csv")

    print_pl(results_data["adjusted_income"], float_precision=0)
        
    dict_to_duckdb(results_data, f"{output_folder}/results.duckdb")