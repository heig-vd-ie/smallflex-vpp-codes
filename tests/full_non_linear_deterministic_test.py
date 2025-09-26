# %%
import os
import json

import polars as pl
from polars import col as c
from datetime import timedelta

from general_function import build_non_existing_dirs, pl_to_dict

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DeterministicConfig
from pipelines.data_manager.deterministic_data_manager import DeterministicDataManager
from pipelines.result_manager import PipelineResultManager
from pipelines.model_manager.deterministic_first_stage import DeterministicFirstStage
from pipelines.model_manager.deterministic_second_stage import DeterministicSecondStage
from data_display.baseline_plots import plot_result


from config import settings

os.chdir(os.getcwd().replace("/src", ""))
os.environ["GRB_LICENSE_FILE"] = os.environ["HOME"] + "/gurobi_license/gurobi.lic"

# %%
YEAR = 2020
HYDROPOWER_MASK = [
    c("name").is_in(["Aegina discrete turbine"]),
    c("name").is_in(["Aegina discrete turbine", "Aegina pump"]),
    c("name").is_in(["Aegina continuous turbine", "Aegina pump"]),
    c("name").is_in(["Aegina continuous turbine"]),
]
PV_POWER_MASK = (c("sub_basin") == "Greisse_4") & (c("start_height") == 2050)
WIND_POWER_MASK = (c("sub_basin") == "Greisse_3") & (c("start_height") == 3050)
# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)

pipeline_config: DeterministicConfig = DeterministicConfig(
    first_stage_timestep=timedelta(days=2),
    second_stage_sim_horizon=timedelta(days=4),
    year=YEAR, nb_state_dict={0: 3}
)
pipeline_data_manager: DeterministicDataManager = DeterministicDataManager(
    smallflex_input_schema=smallflex_input_schema,
    pipeline_config=pipeline_config,
    hydro_power_mask=HYDROPOWER_MASK[2],
    pv_power_mask=PV_POWER_MASK,
    wind_power_mask=WIND_POWER_MASK
)
result_manager: PipelineResultManager = PipelineResultManager()

# %%
deterministic_first_stage: DeterministicFirstStage = DeterministicFirstStage(
    pipeline_data_manager=pipeline_data_manager
)
deterministic_first_stage.solve_model()
powered_volume_quota: pl.DataFrame = result_manager.extract_powered_volume_quota(
    model_instance=deterministic_first_stage.model_instance,
    first_stage_nb_timestamp=deterministic_first_stage.first_stage_nb_timestamp,
)


# %%
deterministic_second_stage: DeterministicSecondStage = DeterministicSecondStage(
    pipeline_data_manager=pipeline_data_manager,
    powered_volume_quota=powered_volume_quota,
)
deterministic_second_stage.solve_every_models()

# %%

first_stage_optimization_results = (
    result_manager.extract_first_stage_optimization_results(
        model_instance=deterministic_first_stage.model_instance,
        timestep_index=deterministic_first_stage.first_stage_timestep_index,
    )
)

second_stage_optimization_results, powered_volume_overage, powered_volume_shortage = (
    result_manager.extract_second_stage_optimization_results(
        model_instances=deterministic_second_stage.model_instances,
        timestep_index=deterministic_second_stage.second_stage_timestep_index,
        nb_timestamp_per_ancillary=pipeline_config.nb_timestamp_per_ancillary
    )
)

# %%
plot_folder = file_names["results_plot"] + "/full_deterministic"
max_volume_mapping = pl_to_dict(deterministic_first_stage.water_basin["B", "volume_max"])
start_volume_mapping = pl_to_dict(deterministic_first_stage.water_basin["B", "start_volume"])

build_non_existing_dirs(plot_folder)

fig = plot_result(
    results=first_stage_optimization_results,
    max_volume_mapping=max_volume_mapping,
    start_volume_mapping=start_volume_mapping,
)
fig.write_html(f"{plot_folder}/first_stage_results.html")

fig = plot_result(
    results=second_stage_optimization_results,    
    max_volume_mapping=max_volume_mapping,
    start_volume_mapping=start_volume_mapping)
fig.write_html(f"{plot_folder}/second_stage_results.html")
