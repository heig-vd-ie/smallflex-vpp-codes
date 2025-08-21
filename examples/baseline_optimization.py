import os
from multiprocessing import get_context

from typing import Union
import json
from datetime import timedelta
import shutil
import polars as pl
from polars import col as c

from general_function import build_non_existing_dirs, generate_log, dict_to_duckdb

from config import settings

from data_display.baseline_plots import (
    plot_first_stage_result,
    plot_second_stage_result,
)

from baseline_model.optimization_results_processing import combine_second_stage_results
from baseline_model.baseline_input import BaseLineInput
from baseline_model.first_stage.first_stage_pipeline import BaselineFirstStage
from baseline_model.second_stage.second_stage_pipeline import BaselineSecondStage


PARALLEL = False
YEARS = [2020, 2021, 2022, 2023]


SIMULATION_SETTING = {"quantile": 0, "buffer": 0.2, "powered_volume_enabled": True}


HYDROPOWER_MASK = [
    c("name").is_in(["Aegina discrete turbine"]),
    c("name").is_in(["Aegina discrete turbine", "Aegina pump"]),
    c("name").is_in(
        ["Aegina discrete turbine", "Aegina continuous turbine", "Aegina pump"]
    ),
]


TURBINE_FACTORS = 0.75
# SIMULATION_SETTING = [{"quantile": 0.15, "buffer": 0.3, "powered_volume_enabled": True}]

REAL_TIMESTEP = timedelta(hours=1)
FIRST_STAGE_TIMESTEP = timedelta(days=1)
SECOND_STAGE_TIME_SIM = timedelta(days=4)
TIME_LIMIT = 20  # in seconds
VOLUME_FACTOR = 1e-6

log = generate_log(name=__name__)


def solve_second_stage_model(second_stage: BaselineSecondStage):
    try:
        second_stage.solve_model()
    except Exception as e:
        raise e
    return second_stage


if __name__ == "__main__":

    output_file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore

    baseline_folder = output_file_names["baseline"]
    if os.path.exists(baseline_folder):
        shutil.rmtree(baseline_folder)
    for year in YEARS:
        plot_folder = f"{baseline_folder}/plots_year_{year}"

        income_result_list: list[dict] = []
        log_book_final: pl.DataFrame = pl.DataFrame()
        for idx, hydro_power_mask in enumerate(HYDROPOWER_MASK):

            income_result: dict = {}
            # income_result[idx] = turbine_factor

            build_non_existing_dirs(plot_folder)

            sim_results: dict[str, pl.DataFrame] = dict()
            sim_summary: list = []

            baseline_input: BaseLineInput = BaseLineInput(
                input_schema_file_name=output_file_names["duckdb_input"],
                real_timestep=REAL_TIMESTEP,
                year=year,
                max_alpha_error=2,
                hydro_power_mask=hydro_power_mask,
                volume_factor=VOLUME_FACTOR,
            )
            first_stage: BaselineFirstStage = BaselineFirstStage(
                nb_state=5,
                input_instance=baseline_input,
                timestep=FIRST_STAGE_TIMESTEP,
                max_turbined_volume_factor=TURBINE_FACTORS,
            )
            first_stage.solve_model()

            sim_results["first_stage"] = first_stage.optimization_results

            fig = plot_first_stage_result(
                simulation_results=first_stage.optimization_results, time_divider=7
            )

            fig.write_html(f"{plot_folder}/{idx}_mask_first_stage.html")

            optimization_inputs: list[list[Union[BaselineSecondStage, str]]] = []
            income_results: list[dict] = []

            inputs_list = []

            second_stage: BaselineSecondStage = BaselineSecondStage(
                input_instance=baseline_input,
                first_stage=first_stage,
                timestep=timedelta(days=4),
                time_limit=TIME_LIMIT,
                model_nb=idx,
                nb_state=4,
                is_parallel=PARALLEL,
                **SIMULATION_SETTING,
            )
            second_stage.solve_model()

            fig_path = f"{plot_folder}/{idx}_mask.html"

            optimization_summary, combined_results = combine_second_stage_results(
                optimization_results=second_stage.optimization_results,
                powered_volume=second_stage.powered_volume,
                market_price=second_stage.market_price,
                index=second_stage.index,
                flow_to_vol_factor=second_stage.real_timestep.total_seconds()
                * second_stage.volume_factor,
            )

            sim_results[f"model_{idx}_mask"] = combined_results
            income_result[f"model_{idx}_mask"] = round(
                combined_results["income"].sum() / 1e6, 3
            )
            fig = plot_second_stage_result(
                simulation_results=combined_results, time_divider=7 * 24
            )
            fig.write_html(fig_path)

            log_book_final = pl.concat(
                [
                    log_book_final,
                    second_stage.log_book.with_columns(pl.lit(idx).alias("sim_name")),
                ],
                how="diagonal_relaxed",
            )

            income_result_list.append(income_result)

        sim_results["income_result"] = pl.from_dicts(income_result_list)
        # log.info(f"Income results for {year} year:\n{sim_results["income_result"]}")

        if not log_book_final.is_empty():

            sim_results["log_book"] = log_book_final

        dict_to_duckdb(
            data=sim_results, file_path=f"{baseline_folder}/{year}_year_results.duckdb"
        )
