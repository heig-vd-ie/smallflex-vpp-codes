import polars as pl
from polars import col as c
from typing import Tuple
from polars import selectors as cs
import pyomo.environ as pyo

from general_function import pl_to_dict

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
        optimization_results: pl.DataFrame
    ) -> pl.DataFrame:
        
        for attribute in ["market_price", "basin_volume", "discharge_volume", "spilled_volume"]:
            if hasattr(model_instance, attribute):
                data = extract_result_table(
                    model_instance=model_instance, var_name=attribute
                )
                if attribute != "market_price":
                    data = pivot_result_table(
                        df=data, on="B", index=self.col_list, values=attribute
                    )
                
                optimization_results = optimization_results.join(data, on=self.col_list, how="left")
        for attribute in ["flow", "hydro_power"]:
            if hasattr(model_instance, attribute):
                data = extract_result_table(
                    model_instance=model_instance, var_name=attribute
                )
                data = pivot_result_table(
                    df=data, on="H", index=["T"], values=attribute
                )
                
                optimization_results = optimization_results.join(data, on=["T"], how="left")
                
        for attribute in [
            "ancillary_market_price", "hydro_ancillary_reserve",
            "battery_charging_power", "battery_discharging_power", "battery_soc", "battery_ancillary_reserve"
            "pv_power", "wind_power"
            ]:
            if hasattr(model_instance, attribute):
                data = extract_result_table(
                    model_instance=model_instance, var_name=attribute
                )
                
                if attribute == "battery_charging_power":
                    data = data.with_columns(
                        c("battery_charging_power") * -1
                    )
                col = "Flow" if ("ancillary" in attribute) and not (is_first_stage) else "T"
                optimization_results = optimization_results.join(data, on=col, how="left")
        print(optimization_results)
        optimization_results = optimization_results.with_columns(
            (pl.sum_horizontal([
                cs.contains("pv_power"),
                cs.contains("wind_power"),
                cs.starts_with("battery").and_(cs.ends_with("power")), 
                cs.starts_with("hydro_power")]
            ) * c("market_price")).alias("da_income"),
            # (pl.sum_horizontal(cs.ends_with("ancillary_reserve")) * c("ancillary_market_price")).alias("ancillary_income")
        )
    
    
        return optimization_results

    def extract_first_stage_optimization_results(
        self, model_instance: pyo.ConcreteModel, timeseries: pl.DataFrame
    ) -> pl.DataFrame:

        optimization_results = timeseries.select("timestamp", *self.col_list)
        optimization_results = self.extract_optimization_results(
            model_instance=model_instance, is_first_stage=True, 
            optimization_results=optimization_results
        )
        
        
        return optimization_results

    # def extract_second_stage_optimization_results(
    #     self, 
    #     model_instances: dict[int, pyo.ConcreteModel],  
    #     timestep_index: pl.DataFrame,
    #     nb_timestamp_per_ancillary: int = 1,
    #     with_battery: bool = False
    # ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:

    #     optimization_results: pl.DataFrame = pl.DataFrame()
    #     powered_volume_overage: pl.DataFrame = pl.DataFrame()
    #     powered_volume_shortage: pl.DataFrame = pl.DataFrame()
    #     for key, model_instance in model_instances.items():
    #         optimization_results = pl.concat(
    #             [
    #                 optimization_results,
    #                 self.extract_optimization_results(
    #                     model_instance=model_instance, is_first_stage=False, 
    #                     nb_timestamp_per_ancillary=nb_timestamp_per_ancillary,
    #                     with_battery=with_battery
    #                 ).with_columns(pl.lit(key).alias("sim_idx")),
    #             ],
    #             how="diagonal_relaxed",
    #         )
    #         powered_volume_overage = pl.concat(
    #             [
    #                 powered_volume_overage,
    #                 extract_result_table(
    #                     model_instance, "powered_volume_overage"
    #                 ).with_columns(pl.lit(key).alias("sim_idx")),
    #             ],
    #             how="diagonal_relaxed",
    #         )

    #         powered_volume_shortage = pl.concat(
    #             [
    #                 powered_volume_shortage,
    #                 extract_result_table(
    #                     model_instance, "powered_volume_shortage"
    #                 ).with_columns(pl.lit(key).alias("sim_idx")),
    #             ],
    #             how="diagonal_relaxed",
    #         )
            
    #     optimization_results = optimization_results.join(
    #         timestep_index[["T", "sim_idx", "timestamp"]], on=["T", "sim_idx"], how="left"
    #     )

    #     powered_volume_overage = powered_volume_overage.pivot(
    #         on="H", values="powered_volume_overage", index="sim_idx"
    #     )
    #     powered_volume_shortage = powered_volume_shortage.pivot(
    #         on="H", values="powered_volume_shortage", index="sim_idx"
    #     )
    #     self.second_optimization_results = optimization_results

    #     return optimization_results, powered_volume_overage, powered_volume_shortage

    # def extract_powered_volume_quota(
    #     self, model_instance: pyo.ConcreteModel, first_stage_nb_timestamp: int
    # ) -> pl.DataFrame:

    #     nb_hours_mapping = pl_to_dict(
    #         extract_result_table(model_instance=model_instance, var_name="nb_hours")[
    #             ["T", "nb_hours"]
    #         ]
    #     )

    #     powered_volume_quota = (
    #         extract_result_table(model_instance=model_instance, var_name="flow")
    #         .with_columns(
    #             (
    #                 c("flow")
    #                 * 3600
    #                 * c("T").replace_strict(nb_hours_mapping, default=1)
    #             ).alias("powered_volume")
    #         )
    #         .drop("flow")
    #     )

    #     powered_volume_quota = (
    #         split_timestamps_per_sim(
    #             data=powered_volume_quota, divisors=first_stage_nb_timestamp
    #         )
    #         .group_by("sim_idx", "H", maintain_order=True)
    #         .agg(c("powered_volume").sum())
    #     )
    #     return powered_volume_quota
