import pyomo.environ as pyo
import json
from datetime import timedelta
import polars as pl
from polars import selectors as cs
from polars  import col as c
import os 
import numpy as np
import shutil
import math
import tqdm
from datetime import timedelta, datetime, timezone
# from optimization_model.optimizaztion_pipeline import first_stage_pipeline
from typing_extensions import Optional
from data_display.input_data_plots import plot_basin_height_volume_table

from data_display.baseline_plots import *
from utility.pyomo_preprocessing import extract_optimization_results, linear_interpolation_for_bound, arange_float, linear_interpolation_using_cols 
from data_federation.input_model import SmallflexInputSchema
from utility.pyomo_preprocessing import *
from config import settings
from utility.general_function import pl_to_dict, pl_to_dict_with_tuple, build_non_existing_dirs, generate_log, dict_to_duckdb
from pyomo_models.input_data_preprocessing import (
    generate_baseline_index, generate_clean_timeseries, generate_water_flow_factor, generate_basin_volume_table,
    clean_hydro_power_performance_table, generate_hydro_power_state, split_timestamps_per_sim, generate_second_stage_state
)
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.graph_objs as go
from plotly.graph_objects import Figure

from plotly.subplots import make_subplots

from pyomo_models.baseline.baseline_input import BaseLineInput
from pyomo_models.baseline.first_stage.first_stage_pipeline import BaselineFirstStage
from pyomo_models.baseline.second_stage.second_stage_pipeline import BaselineSecondStage


YEAR = 2020

SIMULATION_SETTING = {
    "1": {"global_price": False, "quantile": 0, "buffer": 0.2},
    "2": {"global_price": False, "quantile": 15, "buffer": 0.2},
    "3": {"global_price": False, "quantile": 15, "buffer": 0.3},
    "4": {"global_price": False, "quantile": 25, "buffer": 0.2},
    "5": {"global_price": True, "quantile": 15, "buffer": 0.3},
    "6": {"global_price": True, "quantile": 25, "buffer": 0.3},
    "7": {"global_price": True, "quantile": 35, "buffer": 0.3}
}
output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES))


if __name__=="__main__":
    baseline_folder = output_file_names["baseline"]
    plot_folder = f"{baseline_folder}/plots"
    if os.path.exists(baseline_folder):
        shutil.rmtree(baseline_folder)
    build_non_existing_dirs(plot_folder)
    
    sim_results: dict[str, pl.DataFrame] = dict()
    sim_summary: list = []
    
    
    baseline_input = BaseLineInput(
        input_schema_file_name=output_file_names["duckdb_input"],
        real_timestep=timedelta(hours=1),
        year=YEAR,
        hydro_power_mask = c("name").is_in(["Aegina hydro"])
    )
    baseline_first_stage = BaselineFirstStage(baseline_input, timestep=timedelta(days=1))
    baseline_first_stage.solve_model() 
    
    sim_results["first_stage"] = baseline_first_stage.simulation_results
    
    sim_summary.append(["first_stage", baseline_first_stage.simulation_results["income"].sum()])
    print("first_stage", round(baseline_first_stage.simulation_results["income"].sum()/1e6, 3))
    
    fig = plot_first_stage_result(
            simulation_results=baseline_first_stage.simulation_results, time_divider=7
        )
        
    fig.write_html(f"{plot_folder}/first_stage.html")
        
    
    for name, sim_setting in SIMULATION_SETTING.items():
        baseline_second_stage = BaselineSecondStage(
            input_instance=baseline_input, 
            first_stage=baseline_first_stage, 
            timestep=timedelta(days=5), 
            powered_volume_enabled=True, 
            **sim_setting
        )
        baseline_second_stage.solve_model()

        sim_results[name] = baseline_second_stage.simulation_results
        sim_summary.append([name, baseline_second_stage.simulation_results["income"].sum()])
        
        print(name, round(baseline_second_stage.simulation_results["income"].sum()/1e6, 3))
        
        fig = plot_second_stage_result(
            simulation_results=baseline_second_stage.simulation_results, time_divider=7*24
        )
        
        fig.write_html(f"{plot_folder}/second_stage_{name}.html")
    
    sim_results["summary"] = pl.DataFrame(sim_summary, schema=["name", "income"])
    
    dict_to_duckdb(data=sim_results, file_path= f"{baseline_folder}/baseline.duckdb")