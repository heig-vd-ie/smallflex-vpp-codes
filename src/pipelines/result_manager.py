import polars as pl
from polars import col as c
from typing import Tuple
from polars import selectors as cs
import pyomo.environ as pyo

from general_function import pl_to_dict


from pipelines.model_manager import deterministic_second_stage
from utility.data_preprocessing import (
    split_timestamps_per_sim,
    extract_result_table,
    pivot_result_table,
)


class PipelineResultManager():
    """
    A class to manage the results of a pipeline, including data processing and visualization.
    """

    def __init__(self, is_stochastic: bool = False) -> None  :

        self.col_list =  ["T", "Ω"] if is_stochastic else ["T"]

        self.first_optimization_results: pl.DataFrame
        self.second_optimization_results: pl.DataFrame


    def extract_optimization_results(
        self, model_instance: pyo.ConcreteModel, 
        is_first_stage: int, 
        nb_timestamp_per_ancillary: int = 1,
        with_battery: bool = False
    ) -> pl.DataFrame:

        market_price = extract_result_table(
            model_instance=model_instance, var_name="market_price"
        )
        ancillary_market_price = extract_result_table(
            model_instance=model_instance, var_name="ancillary_market_price"
        )

        flow_to_vol_factor = 3600

        if is_first_stage:
            nb_hours_mapping = pl_to_dict(
                extract_result_table(
                    model_instance=model_instance, var_name="nb_hours"
                )[["T", "nb_hours"]]
            )
        else:
            nb_hours_mapping = {}

        basin_volume = extract_result_table(
            model_instance=model_instance, var_name="basin_volume"
        )

        basin_volume = pivot_result_table(
            df=basin_volume, on="B", index=self.col_list, values="basin_volume"
        )
        
        discharge_volume = extract_result_table(
            model_instance=model_instance, var_name="discharge_volume"
        )

        discharge_volume = pivot_result_table(
                df=discharge_volume, on="B", index=self.col_list, values="discharge_volume"
            )
        
        spilled_volume = extract_result_table(
            model_instance=model_instance, var_name="spilled_volume"
        )

        spilled_volume = pivot_result_table(
                df=spilled_volume, on="B", index=self.col_list, values="spilled_volume"
            )

        powered_volume = extract_result_table(
            model_instance=model_instance, var_name="flow"
        ).with_columns(
            (
                c("flow")
                * flow_to_vol_factor
                * c("T").replace_strict(nb_hours_mapping, default=1)
            ).alias("powered_volume")
        )

        powered_volume = pivot_result_table(
            df=powered_volume, on="H", index=["T"], values="powered_volume"
        )

        hydro_power = extract_result_table(
            model_instance=model_instance, var_name="hydro_power"
        )

        hydro_power = pivot_result_table(
            df=hydro_power, on="H", index=["T"], values="hydro_power"
        )

        hydro_ancillary_reserve = extract_result_table(
            model_instance=model_instance, var_name="hydro_ancillary_reserve"
        )
        
        if with_battery:
            battery_charging_power = extract_result_table(
                model_instance=model_instance, var_name="battery_charging_power"
            ).with_columns(
                c("battery_charging_power") * -1
            ) 

            battery_discharging_power = extract_result_table(
                model_instance=model_instance, var_name="battery_discharging_power"
            )

            battery_soc = extract_result_table(
                model_instance=model_instance, var_name="battery_soc"
            )

            battery_ancillary_reserve = extract_result_table(
                model_instance=model_instance, var_name="battery_ancillary_reserve"
            )

        if not is_first_stage:
            hydro_ancillary_reserve = (
                hydro_ancillary_reserve.with_columns(
                    pl.all()
                    .exclude("F")
                    .map_elements(
                        lambda x: [x] * nb_timestamp_per_ancillary,
                        return_dtype=pl.List(pl.Float64),
                    )
                )
                .explode(pl.all().exclude("F")) # type: ignore
                .with_row_index(name="T")
                .drop("F")
            )  
            ancillary_market_price = (
                ancillary_market_price.with_columns(
                    pl.all()
                    .exclude("F")
                    .map_elements(
                        lambda x: [x] * nb_timestamp_per_ancillary,
                        return_dtype=pl.List(pl.Float64),
                    )
                )
                .explode(pl.all().exclude("F")) # type: ignore
                .with_row_index(name="T")
                .drop("F")
            )  # type: ignore
            if with_battery:
                battery_ancillary_reserve = (
                    battery_ancillary_reserve.with_columns(
                        pl.all()
                        .exclude("F")
                        .map_elements(
                            lambda x: [x] * nb_timestamp_per_ancillary,
                            return_dtype=pl.List(pl.Float64),
                        )
                    )
                    .explode(pl.all().exclude("F")) # type: ignore
                    .with_row_index(name="T")
                    .drop("F")
                )  # type: ignore

        optimization_results: pl.DataFrame = (
            market_price
            .join(ancillary_market_price, on=self.col_list, how="inner")
            .join(discharge_volume, on=self.col_list, how="inner")
            .join(basin_volume, on=self.col_list, how="inner")
            .join(spilled_volume, on=self.col_list, how="inner")
            .join(powered_volume, on="T", how="inner")
            .join(hydro_power, on="T", how="inner")
            .join(hydro_ancillary_reserve, on="T", how="inner")
        )
        if with_battery:
            optimization_results = (
                optimization_results
                .join(battery_charging_power, on="T", how="inner")
                .join(battery_discharging_power, on="T", how="inner")
                .join(battery_soc, on="T", how="inner")
                .join(battery_ancillary_reserve, on="T", how="inner")
            )
            
        optimization_results = optimization_results.with_columns(
            (pl.sum_horizontal([
                cs.starts_with("battery").and_(cs.ends_with("power")), 
                cs.starts_with("hydro_power")]) * c("market_price")).alias("da_income"),
            (pl.sum_horizontal(cs.ends_with("ancillary_reserve")) * c("ancillary_market_price")/ 4).alias("ancillary_income")
        ).sum()

    
        return optimization_results

    def extract_first_stage_optimization_results(
        self, model_instance: pyo.ConcreteModel, timestep_index: pl.DataFrame
    ) -> pl.DataFrame:

        optimization_results = self.extract_optimization_results(
            model_instance=model_instance, is_first_stage=True
        )
        
        optimization_results = optimization_results.join(
            timestep_index["T", "timestamp"], on= "T", how="left")
        
        return optimization_results

    def extract_second_stage_optimization_results(
        self, 
        model_instances: dict[int, pyo.ConcreteModel], 
        timestep_index: pl.DataFrame,
        nb_timestamp_per_ancillary: int = 1,
        with_battery: bool = False
    ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:

        optimization_results: pl.DataFrame = pl.DataFrame()
        powered_volume_overage: pl.DataFrame = pl.DataFrame()
        powered_volume_shortage: pl.DataFrame = pl.DataFrame()
        for key, model_instance in model_instances.items():
            optimization_results = pl.concat(
                [
                    optimization_results,
                    self.extract_optimization_results(
                        model_instance=model_instance, is_first_stage=False, 
                        nb_timestamp_per_ancillary=nb_timestamp_per_ancillary,
                        with_battery=with_battery
                    ).with_columns(pl.lit(key).alias("sim_idx")),
                ],
                how="diagonal_relaxed",
            )
            powered_volume_overage = pl.concat(
                [
                    powered_volume_overage,
                    extract_result_table(
                        model_instance, "powered_volume_overage"
                    ).with_columns(pl.lit(key).alias("sim_idx")),
                ],
                how="diagonal_relaxed",
            )

            powered_volume_shortage = pl.concat(
                [
                    powered_volume_shortage,
                    extract_result_table(
                        model_instance, "powered_volume_shortage"
                    ).with_columns(pl.lit(key).alias("sim_idx")),
                ],
                how="diagonal_relaxed",
            )
            
        optimization_results = optimization_results.join(
            timestep_index[["T", "sim_idx", "timestamp"]], on=["T", "sim_idx"], how="left"
        )

        powered_volume_overage = powered_volume_overage.pivot(
            on="H", values="powered_volume_overage", index="sim_idx"
        )
        powered_volume_shortage = powered_volume_shortage.pivot(
            on="H", values="powered_volume_shortage", index="sim_idx"
        )
        self.second_optimization_results = optimization_results

        return optimization_results, powered_volume_overage, powered_volume_shortage

    def extract_powered_volume_quota(
        self, model_instance: pyo.ConcreteModel, first_stage_nb_timestamp: int
    ) -> pl.DataFrame:

        nb_hours_mapping = pl_to_dict(
            extract_result_table(model_instance=model_instance, var_name="nb_hours")[
                ["T", "nb_hours"]
            ]
        )

        powered_volume_quota = (
            extract_result_table(model_instance=model_instance, var_name="flow")
            .with_columns(
                (
                    c("flow")
                    * 3600
                    * c("T").replace_strict(nb_hours_mapping, default=1)
                ).alias("powered_volume")
            )
            .drop("flow")
        )

        powered_volume_quota = (
            split_timestamps_per_sim(
                data=powered_volume_quota, divisors=first_stage_nb_timestamp
            )
            .group_by("sim_idx", "H", maintain_order=True)
            .agg(c("powered_volume").sum())
        )
        return powered_volume_quota
