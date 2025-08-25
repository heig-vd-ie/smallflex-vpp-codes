from pipelines.data_configs import PipelineConfig
from pipelines.data_manager import PipelineDataManager
import os
from multiprocessing import get_context
from smallflex_data_schema import SmallflexInputSchema
from typing import Union
import json
from datetime import timedelta
import shutil
import polars as pl
from polars  import col as c

from general_function import build_non_existing_dirs, generate_log, dict_to_duckdb

from config import settings

from data_display.baseline_plots import plot_first_stage_result, plot_second_stage_result

from baseline_model.optimization_results_processing import combine_second_stage_results
from baseline_model.baseline_input import BaseLineInput
from baseline_model.first_stage.first_stage_pipeline import BaselineFirstStage
from baseline_model.second_stage.second_stage_pipeline import BaselineSecondStage

os.chdir(os.getcwd().replace("/src", ""))
os.environ["GRB_LICENSE_FILE"] = os.environ["HOME"] + "/gurobi_license/gurobi.lic"


HYDROPOWER_MASK = [
    c("name").is_in(["Aegina discrete turbine"]),
    c("name").is_in(["Aegina discrete turbine", "Aegina pump"]),
    c("name").is_in(["Aegina discrete turbine", "Aegina continuous turbine", "Aegina pump"])
]


file_names: dict[str, str] = json.load(open(settings.FILE_NAMES)) # type: ignore
smallflex_input_schema=SmallflexInputSchema().duckdb_to_schema(file_path=file_names["duckdb_input"])

pipeline_config = PipelineConfig(year=2020)
pipeline_data_manager = PipelineDataManager(
    smallflex_input_schema=smallflex_input_schema,
    pipeline_config=pipeline_config,
    hydro_power_mask=HYDROPOWER_MASK[2]
)