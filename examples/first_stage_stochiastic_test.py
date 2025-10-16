# %%
import os
import json
import numpy as np
import polars as pl
from polars  import col as c
from polars import selectors as cs
import pyomo.environ as pyo
from datetime import timedelta

from general_function import pl_to_dict, build_non_existing_dirs
from numpy_function import clipped_cumsum

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import StochasticConfig, DeterministicConfig
from pipelines.model_manager.stochastic_first_stage import StochasticFirstStage
from pipelines.model_manager.deterministic_second_stage_2 import DeterministicSecondStage

from pipelines.result_manager import extract_first_stage_optimization_results, extract_basin_volume_expectation, extract_second_stage_optimization_results

from timeseries_preparation.first_stage_stochiastic_data import process_first_stage_timeseries_data
from timeseries_preparation.deterministic_data import process_timeseries_data

from data_display.baseline_plots import plot_scenario_results, plot_second_stage_result

from utility.data_preprocessing import (
    generate_basin_volume_table,
    clean_hydro_power_performance_table,
    split_timestamps_per_sim,
    generate_hydro_power_state,
    generate_first_stage_basin_state_table,
    generate_clean_timeseries,
    generate_datetime_index,
    generate_clean_timeseries_scenarios,
)
from utility.data_preprocessing import (
    split_timestamps_per_sim,
    extract_result_table,
    pivot_result_table,
)
from data_display.baseline_plots import plot_result

from config import settings

os.chdir(os.getcwd().replace("/src", ""))
os.environ["GRB_LICENSE_FILE"] = os.environ["HOME"] + "/gurobi_license/gurobi.lic"
# %%

HYDROPOWER_MASK = [
    c("name").is_in(["Aegina discrete turbine"]),
    c("name").is_in(["Aegina discrete turbine", "Aegina pump"]),
    c("name").is_in(["Aegina continuous turbine", "Aegina pump"]),
    c("name").is_in(["Aegina continuous turbine"])
]

YEAR_LIST = [
    2023
]

PV_POWER_MASK = (c("sub_basin") == "Greisse_4") & (c("start_height") == 2050)
WIND_POWER_MASK = (c("sub_basin") == "Greisse_3") & (c("start_height") == 3050)

BATTERY_SIZE = {
    "no_battery": {"rated_power": 0, "capacity": 0},
    "battery_1_MW_2MWh": {"rated_power": 1, "capacity": 2},
    "battery_2_MW_4MWh": {"rated_power": 2, "capacity": 4},
    "battery_5_MW_10MWh": {"rated_power": 5, "capacity": 10},
    "battery_1_MW_4MWh": {"rated_power": 1, "capacity": 4},
    "battery_2_MW_8MWh": {"rated_power": 2, "capacity": 8},
    "battery_5_MW_20MWh": {"rated_power": 5, "capacity": 20},
}

# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES)) # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(file_path=file_names["duckdb_input"])

data_config: StochasticConfig = StochasticConfig(
    first_stage_timestep=timedelta(days=1),
    second_stage_sim_horizon=timedelta(days=4),
    solver_name="gurobi", 
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75
)
data_config_2: DeterministicConfig = DeterministicConfig(
    first_stage_timestep=timedelta(days=1),
    second_stage_sim_horizon=timedelta(days=1),
    solver_name="gurobi", 
    year = 2022,
    battery_capacity=0,
    bound_penalty_factor=0.3
)
# %%
stochastic_first_stage: StochasticFirstStage = StochasticFirstStage(
    data_config=data_config,
    smallflex_input_schema=smallflex_input_schema,
    hydro_power_mask=HYDROPOWER_MASK[2],
)

timeseries=process_first_stage_timeseries_data(
    smallflex_input_schema=smallflex_input_schema,
    data_config=data_config,
    scenario_list=stochastic_first_stage.scenario_list,
    water_basin_mapping=pl_to_dict(stochastic_first_stage.water_basin["uuid", "B"]),
)
stochastic_first_stage.set_timeseries(timeseries=timeseries)

stochastic_first_stage.solve_model()

optimization_results = extract_first_stage_optimization_results(
    model_instance=stochastic_first_stage.model_instance,
    timeseries=stochastic_first_stage.timeseries
)

fig = plot_scenario_results(
    optimization_results=optimization_results, 
    water_basin=stochastic_first_stage.upstream_water_basin,
)

plot_folder = file_names["results_plot"] + "/linear_stochastic"

build_non_existing_dirs(plot_folder)
fig.write_html(f"{plot_folder}/first_stage_results.html")
# %%
basin_volume_expectation = extract_basin_volume_expectation(
    model_instance=stochastic_first_stage.model_instance,
    optimization_results=optimization_results,
    water_basin=stochastic_first_stage.upstream_water_basin,
    data_config=data_config
)
basin_volume_expectation.write_parquet(".cache/output/basin_volume_expectation.parquet")

basin_volume_expectation = pl.read_parquet(".cache/output/basin_volume_expectation.parquet")

deterministic_second_stage: DeterministicSecondStage = DeterministicSecondStage(
    data_config=data_config_2,
    smallflex_input_schema=smallflex_input_schema,
    basin_volume_expectation=basin_volume_expectation,
    hydro_power_mask=HYDROPOWER_MASK[2],
)

timeseries = process_timeseries_data(
    smallflex_input_schema=smallflex_input_schema,
    data_config=data_config_2,
    basin_index_mapping=pl_to_dict(deterministic_second_stage.water_basin["uuid", "B"]),
    pv_power_mask=PV_POWER_MASK,
    wind_power_mask=WIND_POWER_MASK,
    
)

deterministic_second_stage.set_timeseries(timeseries=timeseries)

deterministic_second_stage.solve_every_models()

second_stage_optimization_results = (
        extract_second_stage_optimization_results(
            model_instances=deterministic_second_stage.model_instances,
            timeseries=deterministic_second_stage.timeseries,
        )
    )

second_stage_optimization_results.write_parquet(".cache/output/second_stage_optimization_results.parquet")
second_stage_optimization_results = pl.read_parquet(".cache/output/second_stage_optimization_results.parquet")

fig = plot_second_stage_result(
    results=second_stage_optimization_results,
    water_basin=deterministic_second_stage.water_basin,
    market_price_quantiles=deterministic_second_stage.market_price_quantiles,
    basin_volume_expectation=deterministic_second_stage.basin_volume_expectation,
    with_battery=data_config_2.battery_capacity > 0,
)
fig.write_html(f".cache/plot/final_results.html")
