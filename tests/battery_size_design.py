#%%

import os
import json
import plotly.express as px
import plotly.graph_objects as go

import polars as pl
from polars import col as c
from datetime import timedelta

from general_function import build_non_existing_dirs, pl_to_dict, dict_to_duckdb

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DeterministicConfig
from pipelines.data_manager.deterministic_data_manager import DeterministicDataManager
from pipelines.result_manager import PipelineResultManager
from pipelines.model_manager.deterministic_first_stage import DeterministicFirstStage
from pipelines.model_manager.deterministic_second_stage import DeterministicSecondStage
from data_display.baseline_plots import plot_result
from itertools import product
from config import settings

os.chdir(os.getcwd().replace("/src", ""))
os.environ["GRB_LICENSE_FILE"] = os.environ["HOME"] + "/gurobi_license/gurobi.lic"

# %%
YEAR = 2023

PV_POWER_MASK = (c("sub_basin") == "Greisse_4") & (c("start_height") == 2050)
WIND_POWER_MASK = (c("sub_basin") == "Greisse_3") & (c("start_height") == 3050)

HYDROPOWER_MASK = {
    "discrete_turbine": c("name").is_in(["Aegina discrete turbine"]),
    "discrete_turbine_pump": c("name").is_in(["Aegina discrete turbine", "Aegina pump"]),
    "continuous_turbine_pump": c("name").is_in(["Aegina continuous turbine", "Aegina pump"]),
}
BATTERY_SIZE = {
    # "no_battery": {"rated_power": 0, "capacity": 0},
    "battery_1_MW_2MWh": {"rated_power": 1, "capacity": 2},
    "battery_2_MW_4MWh": {"rated_power": 2, "capacity": 4},
    "battery_5_MW_10MWh": {"rated_power": 5, "capacity": 10},
    "battery_10_MW_20MWh": {"rated_power": 10, "capacity": 20},
    "battery_1_MW_4MWh": {"rated_power": 1, "capacity": 4},
    "battery_2_MW_8MWh": {"rated_power": 2, "capacity": 8},
    "battery_5_MW_20MWh": {"rated_power": 5, "capacity": 20},
    "battery_10_MW_40MWh": {"rated_power": 10, "capacity": 40},
}
# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)
# %%

results_data = {}

for hydro_power_mask, battery_size in product(*[HYDROPOWER_MASK.keys(), BATTERY_SIZE.keys()]):
    pipeline_config: DeterministicConfig = DeterministicConfig(
        first_stage_timestep=timedelta(days=2),
        second_stage_sim_horizon=timedelta(days=4),
        year=YEAR, nb_state_dict={0: 3},
        second_stage_quantile=0.15,
        battery_efficiency=0.95, 
        battery_rated_power=BATTERY_SIZE[battery_size]["rated_power"], 
        battery_capacity=BATTERY_SIZE[battery_size]["capacity"]
    )
    pipeline_data_manager: DeterministicDataManager = DeterministicDataManager(
        smallflex_input_schema=smallflex_input_schema,
        pipeline_config=pipeline_config,
        hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
        pv_power_mask=PV_POWER_MASK,
        wind_power_mask=WIND_POWER_MASK,
        )
    result_manager: PipelineResultManager = PipelineResultManager()
    
    deterministic_first_stage: DeterministicFirstStage = DeterministicFirstStage(
        pipeline_data_manager=pipeline_data_manager
    )
    deterministic_first_stage.solve_model()
    powered_volume_quota: pl.DataFrame = result_manager.extract_powered_volume_quota(
        model_instance=deterministic_first_stage.model_instance,
        first_stage_nb_timestamp=deterministic_first_stage.first_stage_nb_timestamp,
    )

    deterministic_second_stage: DeterministicSecondStage = DeterministicSecondStage(
        pipeline_data_manager=pipeline_data_manager,
        powered_volume_quota=powered_volume_quota,
    )
    deterministic_second_stage.solve_every_models()
    # first_stage_optimization_results = (
    #     result_manager.extract_first_stage_optimization_results(
    #         model_instance=deterministic_first_stage.model_instance,
    #         timestep_index=deterministic_first_stage.first_stage_timestep_index,
    #     )
    # )
    second_stage_optimization_results, powered_volume_overage, powered_volume_shortage = (
        result_manager.extract_second_stage_optimization_results(
            model_instances=deterministic_second_stage.model_instances,
            timestep_index=deterministic_second_stage.second_stage_timestep_index,
            nb_timestamp_per_ancillary=pipeline_config.nb_timestamp_per_ancillary,
            with_battery=True
        )
    )
    results_data["_".join([hydro_power_mask, battery_size])] = second_stage_optimization_results

    
dict_to_duckdb(results_data, ".cache/output/results.duckdb")