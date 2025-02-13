import os
import json
from datetime import timedelta
import polars as pl
from typing import Union
from multiprocessing import get_context

from polars  import col as c
import shutil

from datetime import timedelta
from data_display.baseline_plots import plot_first_stage_result, plot_second_stage_result
from config import settings
from general_function import build_non_existing_dirs, generate_log, dict_to_duckdb

from pyomo_models.baseline.baseline_input import BaseLineInput
from pyomo_models.baseline.first_stage.first_stage_pipeline import BaselineFirstStage
from pyomo_models.baseline.second_stage.second_stage_pipeline import BaselineSecondStage


PARALLEL = False
YEARS = [2020, 2021, 2022, 2023]

TURBINE_FACTORS = {0.75, 0.85, 0.95}
YEARS = [2020]
TURBINE_FACTORS = {0.75}

SIMULATION_SETTING = [
    {"quantile": 0, "buffer": 0.2, "powered_volume_enabled": True},
    {"quantile": 0.15, "buffer": 0.3, "powered_volume_enabled": True},
    {"quantile": 0.25, "buffer": 0.3, "powered_volume_enabled": False},
    {"quantile": 0.15, "buffer": 0.3, "powered_volume_enabled": True, "global_price": True},
    {"quantile": 0.25, "buffer": 0.3, "powered_volume_enabled": False, "global_price": True},
]
REAL_TIMESTEP = timedelta(hours=1)
FIRST_STAGE_TIMESTEP = timedelta(days=1)
SECOND_STAGE_TIME_SIM = timedelta(days=4)
TIME_LIMIT = 20 # in seconds
VOLUME_FACTOR = 1e-6

output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES))

log = generate_log(name=__name__)

def solve_second_stage_model(
    second_stage: BaselineSecondStage
    ):
    try:
        second_stage.solve_model()
    except Exception as e:
        raise e
    return second_stage
    

if __name__=="__main__":
    baseline_folder = output_file_names["baseline"]
    if os.path.exists(baseline_folder):
        shutil.rmtree(baseline_folder)
    for year in YEARS:
        plot_folder = f"{baseline_folder}/plots_year_{year}"
        
        income_result_list: list[dict] = []
        log_book_final: pl.DataFrame = pl.DataFrame()
        for turbine_factor in TURBINE_FACTORS:
            turbine_factor_str = str(turbine_factor).replace(".", "_")
            
            income_result: dict = {} 
            income_result["turbine_factor"] = turbine_factor
            
            build_non_existing_dirs(plot_folder)
            
            sim_results: dict[str, pl.DataFrame] = dict()
            sim_summary: list = []

            baseline_input: BaseLineInput = BaseLineInput(
                input_schema_file_name=output_file_names["duckdb_input"],
                real_timestep=REAL_TIMESTEP,
                year=year,
                max_alpha_error=2,
                hydro_power_mask = c("name").is_in(["Aegina hydro"]),
                volume_factor=VOLUME_FACTOR
            )
            first_stage: BaselineFirstStage = BaselineFirstStage(
                input_instance=baseline_input,
                timestep=FIRST_STAGE_TIMESTEP,
                turbine_factor=turbine_factor
            )
            first_stage.solve_model()
            
            sim_results["first_stage"] = first_stage.simulation_results
            income_result["first_stage"] = round(first_stage.simulation_results["income"].sum()/1e6, 3)

            fig = plot_first_stage_result(
                    simulation_results=first_stage.simulation_results, time_divider=7
                )
                
            fig.write_html(f"{plot_folder}/{turbine_factor}_turbine_factor_first_stage.html")
            
            optimization_inputs: list[list[Union[BaselineSecondStage, str]]] = []
            income_results: list[dict] = []
            
            inputs_list = []
            for model_nb, sim_setting in enumerate(SIMULATION_SETTING):
                second_stage: BaselineSecondStage = BaselineSecondStage(
                    input_instance=baseline_input, 
                    first_stage=first_stage, 
                    timestep=timedelta(days=4), 
                    time_limit=TIME_LIMIT,
                    model_nb=model_nb,
                    is_parallel=PARALLEL,
                    **sim_setting
                )
                inputs_list.append([second_stage])
            if PARALLEL:
                with get_context("spawn").Pool(processes=len(inputs_list)) as pool:
                    results = pool.starmap(solve_second_stage_model, inputs_list)
            else:
                results = []
                for model_inputs in inputs_list:
                    results.append(solve_second_stage_model(*model_inputs))
                
            for model_id, second_stage in enumerate(results):
                
                fig_path = f"{plot_folder}/{turbine_factor_str}_turbine_factor_model_{model_nb}.html"    
                second_stage.finalizes_results_processing()
                
                sim_results[f"turbine_factor_{turbine_factor_str}_model_{model_id}"] = second_stage.simulation_results
                income_result[f"model_{model_id}"] = round(second_stage.simulation_results["income"].sum()/1e6, 3)
                fig = plot_second_stage_result(
                    simulation_results=second_stage.simulation_results, time_divider=7*24
                )
                fig.write_html(fig_path)
                
                log_book_final = pl.concat([
                    log_book_final, 
                    second_stage.log_book.with_columns(pl.lit(model_id).alias("sim_name"))
                ], how="diagonal_relaxed")    
                
            income_result_list.append(income_result) 
            
        sim_results["income_result"] = pl.from_dicts(income_result_list)
        log.info(f"Income results for {year} year:\n{sim_results["income_result"]}")
        
        if not log_book_final.is_empty():

            sim_results["log_book"] = log_book_final
        print(sim_results)
        
        dict_to_duckdb(data=sim_results, file_path= f"{baseline_folder}/{year}_year_results.duckdb")