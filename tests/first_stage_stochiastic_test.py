# %%
import os
import json

import polars as pl
from polars  import col as c

from datetime import timedelta

from general_function import pl_to_dict, build_non_existing_dirs

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import StochasticConfig
from pipelines.data_manager.stochastic_data_manager import StochasticDataManager
from pipelines.result_manager import PipelineResultManager
from pipelines.model_manager.stochastic_first_stage import StochasticFirstStage

from data_display.baseline_plots import plot_scenario_results


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


file_names: dict[str, str] = json.load(open(settings.FILE_NAMES)) # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(file_path=file_names["duckdb_input"])

pipeline_config: StochasticConfig = StochasticConfig(
    first_stage_timestep=timedelta(days=1),
    second_stage_sim_horizon=timedelta(days=4),
    solver_name="gurobi", 
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=1
)

pipeline_data_manager: StochasticDataManager = StochasticDataManager(
    smallflex_input_schema=smallflex_input_schema,
    pipeline_config=pipeline_config,
    hydro_power_mask=HYDROPOWER_MASK[2]
)
result_manager:PipelineResultManager = PipelineResultManager(is_stochastic=True)
# %%


stochastic_first_stage: StochasticFirstStage = StochasticFirstStage(
    pipeline_data_manager=pipeline_data_manager
)
stochastic_first_stage.solve_model()

optimization_results = result_manager.extract_first_stage_optimization_results(
    model_instance=stochastic_first_stage.model_instance, 
    first_stage_timestep_index=pipeline_data_manager.first_stage_timestep_index)

# %%
max_volume_mapping = pl_to_dict(stochastic_first_stage.water_basin["B", "volume_max"])
fig = plot_scenario_results(optimization_results=optimization_results, max_volume_mapping=max_volume_mapping)

plot_folder = file_names["results_plot"] + "/linear_stochastic"

build_non_existing_dirs(plot_folder)
fig.write_html(f"{plot_folder}/first_stage_results.html")