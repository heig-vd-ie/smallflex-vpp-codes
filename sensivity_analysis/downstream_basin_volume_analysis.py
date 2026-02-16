import os

import json
import numpy as np
import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo
from datetime import timedelta, datetime
from itertools import product
from tqdm.auto import tqdm
from typing import Optional, Literal

from general_function import pl_to_dict, build_non_existing_dirs, dict_to_duckdb

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig
from pipelines.model_manager.stochastic_first_stage import StochasticFirstStage
from pipelines.model_manager.stochastic_second_stage import StochasticSecondStage

from timeseries_preparation.read_custom_file_timeseries import (
    read_and_validate_custom_file,
)

from data_display.baseline_plots import (
    plot_scenario_results,
    plot_second_stage_result,
    plot_first_stage_result,
)

from pipelines.pipeline_manager.first_stage_stochastic_pipeline import (
    first_stage_stochastic_pipeline,
)
from pipelines.pipeline_manager.second_stage_stochastic_pipeline import (
    second_stage_stochastic_pipeline,
)

from utility.data_preprocessing import print_pl

from config import settings


def downstream_basin_volume_analysis(
    design_name: str = "downstream_basin_volume",
    basin_volume_size_list: list[float] = [5e4, 1e5, 2e5, 5e5, 1e6, 2e6, 5e6],
    hydropower_mask = c("name").is_in(["Aegina continuous turbine", "Aegina pump"])
    ):
    # Load smallflex input schema and set data config
    smallflex_input_schema: (
        SmallflexInputSchema
    ) = SmallflexInputSchema().duckdb_to_schema(
        file_path=settings.input_files.duckdb_input
    )
    data_config: DataConfig = DataConfig(
        nb_scenarios=200,
        total_scenarios_synthesized=smallflex_input_schema.discharge_volume_synthesized["scenario"].max(),  # type: ignore
    )

    # Create output directories
    output_folder = f"{settings.output_files.output}/{design_name}"
    plot_folder = f"{settings.output_files.results_plot}/{design_name}"
    build_non_existing_dirs(output_folder)
    build_non_existing_dirs(plot_folder)
    
    results_data: dict[str, pl.DataFrame] = {}
    income_list: list = []
    
    for basin_volume_size in tqdm(basin_volume_size_list, position=0, desc="Basin volume size sensitivity analysis"):
        
        water_basin = smallflex_input_schema.water_basin.with_columns(
            pl.when(c("name") == "Aegina downstream basin")
            .then(pl.lit(basin_volume_size))
            .otherwise(c("volume_max"))
            .alias("volume_max")
        )

        smallflex_input_schema = smallflex_input_schema.replace_table(**{"water_basin": water_basin})
        
        _, basin_volume_expectation, _ = first_stage_stochastic_pipeline(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydropower_mask,
            plot_result=False,
            custom_market_prices=None,
        )
        
        second_stage_optimization_results, adjusted_income, fig = (
            second_stage_stochastic_pipeline(
                data_config=data_config,
                smallflex_input_schema=smallflex_input_schema,
                basin_volume_expectation=basin_volume_expectation,
                hydro_power_mask=hydropower_mask,
                plot_result=True,
                custom_market_prices=None,
            )
        )

        income_list.append((basin_volume_size, adjusted_income / 1e3))
        
        if fig is not None:
            fig.write_html(f"{plot_folder}/{basin_volume_size}_results.html")
            
        results_data[f"basin_volume_size_{basin_volume_size}"] = second_stage_optimization_results
    results_data["adjusted_income"] = pl.DataFrame(
    income_list, schema=["basin_volume_size", "adjusted_income"], orient="row"
    )
    min_income = results_data["adjusted_income"]["adjusted_income"].min()
    results_data["adjusted_income"] = results_data["adjusted_income"].with_columns(
        (100*c("adjusted_income")/min_income).alias("relative_income")
    )
    print_pl(results_data["adjusted_income"], float_precision=2)

    dict_to_duckdb(results_data, f"{output_folder}/results.duckdb")

    
if __name__ == "__main__":
    downstream_basin_volume_analysis()