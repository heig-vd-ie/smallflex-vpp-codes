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

TURBINE_FACTORS = {0.7, 0.8, 0.9}
SIMULATION_SETTING = {
    "1": {"quantile": 0, "buffer": 0.2, "powered_volume_enabled": True, "with_penalty": True},
    "2": {"quantile": 0.15, "buffer": 0.3, "powered_volume_enabled": True, "with_penalty": True},
    "3": {"quantile": 0.15, "buffer": 0.3, "powered_volume_enabled": False, "with_penalty": True},
    # "4": {"quantile": 0, "buffer": 0.2, "powered_volume_enabled": True, "with_penalty": False},
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
            log_book_final: pl.DataFrame = pl.DataFrame()
            build_non_existing_dirs(plot_folder)
            
            sim_results: dict[str, pl.DataFrame] = dict()
            sim_summary: list = []

            baseline_input: BaseLineInput = BaseLineInput(
                input_schema_file_name=output_file_names["duckdb_input"],
                real_timestep=timedelta(hours=1),
                year=year,
                hydro_power_mask = c("name").is_in(["Aegina hydro"]),
                volume_factor=1e-6
            )
            
            baseline_first_stage: BaselineFirstStage = BaselineFirstStage(
                baseline_input, timestep=timedelta(days=1), turbine_factor=turbine_factor
            )
            
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
                baseline_second_stage: BaselineSecondStage = BaselineSecondStage(
                    input_instance=baseline_input, 
                    first_stage=baseline_first_stage, 
                    timestep=timedelta(days=4), 
                    time_limit=20,
                    **sim_setting
                )
                baseline_second_stage.solve_model()

                sim_results[f"second_stage_{name}"] = baseline_second_stage.simulation_results
                sim_summary.append([name, baseline_second_stage.simulation_results["income"].sum()])
                
                value = round(baseline_second_stage.simulation_results["income"].sum()/1e6, 3)
                print(f"Results for {year} year, {turbine_factor} turbine_factor and second stage {name}\n")
                print(f"Total income: {value}")    
                log_book = baseline_second_stage.log_book
                if not log_book.is_empty():
                    print(f"Non optimal solutions:\n{log_book.to_pandas().to_string()}")   
                    
                    log_book_final = pl.concat([
                        log_book_final, 
                        log_book.with_columns(pl.lit(name).alias("sim_name"))
                    ], how="diagonal_relaxed")    
                
                fig = plot_second_stage_result(
                    simulation_results=baseline_second_stage.simulation_results, time_divider=7*24
                )
                
                fig.write_html(f"{plot_folder}/second_stage_{name}.html")
            
            sim_results["summary"] = pl.DataFrame(sim_summary, schema=["name", "income"], orient="row")
            sim_results["log_book"] = log_book_final
            
            dict_to_duckdb(data=sim_results, file_path= f"{test_folder}/optimization_results.duckdb")