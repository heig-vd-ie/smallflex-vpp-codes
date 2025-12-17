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
from typing import Optional

from general_function import pl_to_dict, build_non_existing_dirs, dict_to_duckdb
from numpy_function import clipped_cumsum

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig
from pipelines.model_manager.deterministic_first_stage import DeterministicFirstStage
from pipelines.model_manager.stochastic_first_stage import StochasticFirstStage
from pipelines.model_manager.deterministic_second_stage import DeterministicSecondStage
from pipelines.model_manager.stochastic_second_stage import StochasticSecondStage

from pipelines.result_manager import (
    extract_basin_volume,
    extract_basin_volume_expectation,
    extract_first_stage_optimization_results, 
    extract_second_stage_optimization_results, 
    extract_third_stage_optimization_results)

from timeseries_preparation.first_stage_stochastic_data import process_first_stage_timeseries_data
from timeseries_preparation.deterministic_data import process_timeseries_data
from timeseries_preparation.second_stage_stochastic_data import process_second_stage_timeseries_stochastic_data

from data_display.baseline_plots import plot_scenario_results, plot_second_stage_result, plot_first_stage_result

from pipelines.pipeline_manager.first_stage_deterministic_pipeline import first_stage_deterministic_pipeline
from pipelines.pipeline_manager.first_stage_stochastic_pipeline import first_stage_stochastic_pipeline
from pipelines.pipeline_manager.second_stage_deterministic_pipeline import second_stage_deterministic_pipeline
from pipelines.pipeline_manager.second_stage_stochastic_pipeline import second_stage_stochastic_pipeline


from utility.data_preprocessing import (
    generate_basin_volume_table,
    clean_hydro_power_performance_table,
    split_timestamps_per_sim,
    generate_hydro_power_state,
    generate_first_stage_basin_state_table,
    generate_clean_timeseries,
    generate_datetime_index,
    generate_clean_timeseries_scenarios,
    split_timestamps_per_sim,
    extract_result_table,
    pivot_result_table,
    print_pl
)
from data_display.baseline_plots import *
from config import settings

PV_POWER_MASK = (c("sub_basin") == "Greisse_4") & (c("start_height") == 2050)
WIND_POWER_MASK = (c("sub_basin") == "Greisse_3") & (c("start_height") == 3050)

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

IMBALANCE_PARTICIPATION = {
    "with_hydro": True,
    "without_hydro": False,
}

MARKET = ["DA", "FRC", "Imbalance"]
