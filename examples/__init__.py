import os
os.chdir(os.getcwd() + "/src")
import json
import numpy as np
import polars as pl
from polars  import col as c
from polars import selectors as cs
import pyomo.environ as pyo
from datetime import timedelta
from itertools import product
from tqdm.auto import tqdm
from typing import Optional

from general_function import pl_to_dict, build_non_existing_dirs, dict_to_duckdb
from numpy_function import clipped_cumsum

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig
from pipelines.model_manager.stochastic_first_stage import StochasticFirstStage
from pipelines.model_manager.deterministic_second_stage_old import DeterministicSecondStage
from pipelines.model_manager.stochastic_second_stage import StochasticSecondStage

from pipelines.result_manager import (
    extract_first_stage_optimization_results, 
    extract_basin_volume_expectation, 
    extract_second_stage_optimization_results, 
    extract_third_stage_optimization_results)

from timeseries_preparation.first_stage_stochastic_data import process_first_stage_timeseries_data
from timeseries_preparation.deterministic_data import process_timeseries_data
from timeseries_preparation.second_stage_stochastic_data import process_second_stage_timeseries_stochastic_data

from data_display.baseline_plots import plot_scenario_results, plot_second_stage_result

from pipelines.piplines_manager.first_stage_stochastic_pipeline import first_stage_stochastic_pipeline
from pipelines.piplines_manager.second_stage_deterministic_pipeline import second_stage_deterministic_pipeline


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

from config import settings

PV_POWER_MASK = (c("sub_basin") == "Greisse_4") & (c("start_height") == 2050)
WIND_POWER_MASK = (c("sub_basin") == "Greisse_3") & (c("start_height") == 3050)

HYDROPOWER_MASK = {
    "discrete_turbine": c("name").is_in(["Aegina discrete turbine"]),
    "discrete_turbine_pump": c("name").is_in(["Aegina discrete turbine", "Aegina pump"]),
    "continuous_turbine_pump": c("name").is_in(["Aegina continuous turbine", "Aegina pump"]),
}
BATTERY_SIZE = {
    "no_battery": {"rated_power": 0, "capacity": 0},
    "battery_1_MW_2MWh": {"rated_power": 1, "capacity": 2},
    "battery_2_MW_4MWh": {"rated_power": 2, "capacity": 4},
    "battery_5_MW_10MWh": {"rated_power": 5, "capacity": 10},
    "battery_1_MW_4MWh": {"rated_power": 1, "capacity": 4},
    "battery_2_MW_8MWh": {"rated_power": 2, "capacity": 8},
    "battery_5_MW_20MWh": {"rated_power": 5, "capacity": 20},
}

def print_pl(data: pl.DataFrame, float_precision: Optional[int]= None) -> None:
    with pl.Config(
        set_tbl_rows=10000,
        set_tbl_cols=500,
        set_tbl_width_chars=50000,
        set_thousands_separator="'",
        set_float_precision=float_precision,
        set_tbl_hide_column_data_types=True,
    
    ):
        print(data)


os.chdir(os.getcwd().replace("/src", ""))
os.environ["GRB_LICENSE_FILE"] = os.environ["HOME"] + "/gurobi_license/gurobi.lic"