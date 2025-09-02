# %%
import os
import json

import polars as pl
from polars  import col as c

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import PipelineConfig
from pipelines.data_manager import PipelineDataManager
from pipelines.result_manager import PipelineResultManager
from pipelines.model_manager.baseline_first_stage import BaselineFirstStage
from pipelines.model_manager.baseline_second_stage import BaselineSecondStage

from utility.pyomo_preprocessing import ( 
    extract_optimization_results, pivot_result_table)

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

pipeline_config: PipelineConfig = PipelineConfig(year=2020, second_stage_quantile=0.15)
pipeline_data_manager: PipelineDataManager = PipelineDataManager(
    smallflex_input_schema=smallflex_input_schema,
    pipeline_config=pipeline_config,
    hydro_power_mask=HYDROPOWER_MASK[2]
)
result_manager:PipelineResultManager = PipelineResultManager(pipeline_data_manager=pipeline_data_manager)

# %%
baseline_first_stage: BaselineFirstStage = BaselineFirstStage(pipeline_data_manager=pipeline_data_manager)
baseline_first_stage.solve_model()
powered_volume_quota: pl.DataFrame = result_manager.extract_powered_volume_quota(model_instance=baseline_first_stage.model_instance)


# %%
baseline_second_stage: BaselineSecondStage = BaselineSecondStage(
    pipeline_data_manager=pipeline_data_manager,
    powered_volume_quota=powered_volume_quota
    )
baseline_second_stage.solve_every_models()

# %%

first_stage_optimization_results = result_manager.extract_optimization_results(
    model_instance=baseline_first_stage.model_instance, is_first_stage=True)


second_stage_optimization_results, powered_volume_overage, powered_volume_shortage = result_manager.extract_second_stage_optimization_results(
    model_instances=baseline_second_stage.model_instances)
