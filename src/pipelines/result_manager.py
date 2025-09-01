from traitlets import Int
from pipelines.baseline_model import first_stage
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
from pipelines.model_manager import baseline_first_stage, baseline_second_stage
from utility.pyomo_preprocessing import (
    extract_optimization_results, pivot_result_table, remove_suffix, generate_clean_timeseries, generate_datetime_index)
from utility.input_data_preprocessing import (
    generate_hydro_power_state, split_timestamps_per_sim
)

import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo

from general_function import pl_to_dict, generate_log

from utility.pyomo_preprocessing import (
    join_pyomo_variables, extract_optimization_results, pivot_result_table, remove_suffix)

from pipelines.model_manager.baseline_first_stage import BaselineFirstStage

class PipelineResultManager(PipelineDataManager):
    """
    A class to manage the results of a pipeline, including data processing and visualization.
    """

    def __init__(
        self,
        pipeline_data_manager: PipelineDataManager
    ):
        # Retrieve attributes from pipeline_data_manager
        for key, value in vars(pipeline_data_manager).items():
            setattr(self, key, value)
        self.first_optimization_results: pl.DataFrame
    
    
    def extract_first_stage_optimization_results(
        self, model_instance: pyo.ConcreteModel, is_first_stage: int) -> pl.DataFrame:

            
        market_price = extract_optimization_results(
            model_instance=model_instance, var_name="market_price"
        )
        ancillary_market_price = extract_optimization_results(
            model_instance=model_instance, var_name="ancillary_market_price"
        )

        water_basin_index = self.water_basin

        flow_to_vol_factor = 3600 * self.volume_factor
        
        if is_first_stage:
            nb_hours_mapping = pl_to_dict(extract_optimization_results(
                    model_instance=model_instance, var_name="nb_hours"
            )[["T", "nb_hours"]])
        else:
            nb_hours_mapping = {}

        volume_max_mapping: dict[str, float] = pl_to_dict(water_basin_index["B", "volume_max"])

        basin_volume = extract_optimization_results(
                model_instance=model_instance, var_name="basin_volume"
            ).with_columns(
                (c("basin_volume") / c("B").replace_strict(volume_max_mapping, default=None)).alias("basin_volume")
            )

        basin_volume = pivot_result_table(
            df = basin_volume, on="B", index=["T"], 
            values="basin_volume")

        

        powered_volume = extract_optimization_results(
                model_instance=model_instance, var_name="flow"
            ).with_columns(
                (
                    c("flow") * flow_to_vol_factor * c("T").replace_strict(nb_hours_mapping, default=1)
                ).alias("powered_volume")
            )
            
        powered_volume = pivot_result_table(
            df = powered_volume, on="H", index=["T"], 
            values="powered_volume")
            

        hydro_power = extract_optimization_results(
                model_instance=model_instance, var_name="hydro_power"
            )

        hydro_power = pivot_result_table(
            df = hydro_power, on="H", index=["T"], 
            values="hydro_power")

        ancillary_power = extract_optimization_results(
                model_instance=model_instance, var_name="ancillary_power"
            )

        ancillary_power = pivot_result_table(
            df = ancillary_power, on="CH", index=["T" if is_first_stage else "F"], 
            values="ancillary_power")
        
        if not is_first_stage:
            ancillary_power = ancillary_power.with_columns(
                pl.all().exclude("F").map_elements(
                    lambda x: [x] * self.nb_timestamp_per_ancillary, return_dtype=pl.List(pl.Float64)
                )
            ).explode(pl.all().exclude("F")).with_row_index(name="T").drop("F")
            ancillary_market_price = ancillary_market_price.with_columns(
                pl.all().exclude("F").map_elements(
                    lambda x: [x] * self.nb_timestamp_per_ancillary, return_dtype=pl.List(pl.Float64)
                )
            ).explode(pl.all().exclude("F")).with_row_index(name="T").drop("F")

        self.first_stage_optimization_results: pl.DataFrame =(
            market_price
                .join(basin_volume, on = "T", how="inner")
                .join(ancillary_market_price, on = "T", how="inner")
                .join(powered_volume, on = "T", how="inner")
                .join(hydro_power, on = "T", how="inner")
                .join(ancillary_power, on = "T", how="inner")
                .with_columns(
                    (
                    pl.sum_horizontal(cs.starts_with("hydro_power")) *
                    c("T").replace_strict(nb_hours_mapping, default=1) * c("market_price") +
                    pl.sum_horizontal(cs.starts_with("ancillary_power")) *
                    c("T").replace_strict(nb_hours_mapping, default=1) * c("market_price")
                    ).alias("income")
                )
        )
        return self.first_stage_optimization_results
    
    def extract_powered_volume_quota(self, model_instance: pyo.ConcreteModel) -> pl.DataFrame:
    
        flow_to_vol_factor = 3600 * self.volume_factor

        nb_hours_mapping = pl_to_dict(extract_optimization_results(
                        model_instance=model_instance, var_name="nb_hours"
                    )[["T", "nb_hours"]])

        powered_volume_quota = extract_optimization_results(
                model_instance=model_instance, var_name="flow"
            ).with_columns(
                (
                    c("flow") * flow_to_vol_factor * c("T").replace_strict(nb_hours_mapping, default=1)
                ).alias("powered_volume")
            ).drop("flow")
            
            
        powered_volume_quota = split_timestamps_per_sim(
            data=powered_volume_quota, divisors=self.first_stage_nb_timestamp
            ).group_by("sim_idx", "H", maintain_order=True).agg(c("powered_volume").sum())
        return powered_volume_quota
