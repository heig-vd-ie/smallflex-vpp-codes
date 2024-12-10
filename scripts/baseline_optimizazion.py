import os
import json
from datetime import timedelta
import polars as pl

from polars  import col as c
import shutil

from datetime import timedelta
from data_display.baseline_plots import plot_first_stage_result, plot_second_stage_result
from config import settings
from utility.general_function import build_non_existing_dirs, generate_log, dict_to_duckdb

from pyomo_models.baseline.baseline_input import BaseLineInput
from pyomo_models.baseline.first_stage.first_stage_pipeline import BaselineFirstStage
from pyomo_models.baseline.second_stage.second_stage_pipeline import BaselineSecondStage


YEARS = [2020, 2021, 2022, 2023]

TURBINE_FACTORS = {0.75, 0.90, 1.0}
SIMULATION_SETTING = {
    "1": {"quantile": 0, "buffer": 0.2, "powered_volume_enabled": True, "with_penalty": True},
    "2": {"quantile": 15, "buffer": 0.3, "powered_volume_enabled": True, "with_penalty": True},
    "3": {"quantile": 15, "buffer": 0.3, "powered_volume_enabled": False, "with_penalty": True},
    "4": {"quantile": 0, "buffer": 0.2, "powered_volume_enabled": True, "with_penalty": False},
}
output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES))

log = generate_log(name=__name__)

if __name__=="__main__":
    baseline_folder = output_file_names["baseline"]
    if os.path.exists(baseline_folder):
        shutil.rmtree(baseline_folder)
    for year in YEARS:
        year_folder = f"{baseline_folder}/year_{year}"
        for turbine_factor in TURBINE_FACTORS:
            test_folder = f"{year_folder}/turbine_factor_{turbine_factor}"
            plot_folder = f"{test_folder}/plots"
            build_non_existing_dirs(plot_folder)
            
            sim_results: dict[str, pl.DataFrame] = dict()
            sim_summary: list = []

            baseline_input = BaseLineInput(
                input_schema_file_name=output_file_names["duckdb_input"],
                real_timestep=timedelta(hours=1),
                year=year,
                hydro_power_mask = c("name").is_in(["Aegina hydro"])
            )
            baseline_first_stage = BaselineFirstStage(baseline_input, timestep=timedelta(days=1), turbine_factor=turbine_factor)
            baseline_first_stage.solve_model() 
            
            sim_results["first_stage"] = baseline_first_stage.simulation_results
            
            sim_summary.append(["first_stage", baseline_first_stage.simulation_results["income"].sum()])
            value = round(baseline_first_stage.simulation_results["income"].sum()/1e6, 3)
            print(f"{year} year, {turbine_factor} turbine_factor and first_stage : {value}")
            
            fig = plot_first_stage_result(
                    simulation_results=baseline_first_stage.simulation_results, time_divider=7
                )
                
            fig.write_html(f"{plot_folder}/first_stage.html")
                
            
            for name, sim_setting in SIMULATION_SETTING.items():
                baseline_second_stage = BaselineSecondStage(
                    input_instance=baseline_input, 
                    first_stage=baseline_first_stage, 
                    timestep=timedelta(days=2), 
                    powered_volume_enabled=True, 
                    **sim_setting
                )
                baseline_second_stage.solve_model()

                sim_results[f"second_stage_{name}"] = baseline_second_stage.simulation_results
                sim_summary.append([name, baseline_second_stage.simulation_results["income"].sum()])
                
                value = round(baseline_second_stage.simulation_results["income"].sum()/1e6, 3)
                print(f"{year} year, {turbine_factor} turbine_factor and second stage {name}: {value}")
                
                fig = plot_second_stage_result(
                    simulation_results=baseline_second_stage.simulation_results, time_divider=7*24
                )
                
                fig.write_html(f"{plot_folder}/second_stage_{name}.html")
            
            sim_results["summary"] = pl.DataFrame(sim_summary, schema=["name", "income"])
            
            dict_to_duckdb(data=sim_results, file_path= f"{test_folder}/optimization_results.duckdb")