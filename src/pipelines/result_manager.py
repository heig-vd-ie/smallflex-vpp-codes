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





def extract_optimization_results(
    model_instance: pyo.ConcreteModel,
    optimization_results: pl.DataFrame,
) -> pl.DataFrame:
    attribute_list = [
        "market_price", "ancillary_market_price", "basin_volume", "discharge_volume", 
        "spilled_volume", "flow", "hydro_power", "hydro_ancillary_reserve",
        "battery_charging_power", "battery_discharging_power", "battery_soc", 
        "battery_ancillary_reserve", "pv_power", "wind_power"
    ]
    for attribute in attribute_list:
        if hasattr(model_instance, attribute):
            data = extract_result_table(
                model_instance=model_instance, var_name=attribute
            )
            
            col_list = ["F"] if "F" in data.columns else ["T"]
            
            if "Ω" in data.columns:
                col_list.append("Ω")

            if attribute in ["basin_volume", "discharge_volume", "spilled_volume"]:
                data = pivot_result_table(
                        df=data, on="B", index=col_list, values=attribute
                    )
            elif attribute in ["flow", "hydro_power"]:
                data = pivot_result_table(
                    df=data, on="H", index=["T"], values=attribute
                )
            elif attribute == "battery_charging_power":
                data = data.with_columns(
                    c("battery_charging_power") * -1
                )
            
            optimization_results = optimization_results.join(data, on=col_list, how="left")

    optimization_results = optimization_results.with_columns(
        (pl.sum_horizontal([
            cs.contains("pv_power"),
            cs.contains("wind_power"),
            cs.starts_with("battery").and_(cs.ends_with("power")), 
            cs.starts_with("hydro_power")]
        ) * c("market_price")).alias("da_income"),
    )
    if optimization_results.select(cs.contains("ancillary_reserve")).shape[1] > 0:
        optimization_results = optimization_results.with_columns(
            (pl.sum_horizontal(cs.contains("ancillary_reserve")) * c("ancillary_market_price")).alias("ancillary_income")
        )

    return optimization_results

def extract_first_stage_optimization_results(
    model_instance: pyo.ConcreteModel, timeseries: pl.DataFrame,
) -> pl.DataFrame:
    optimization_results = timeseries = timeseries.select("timestamp", "T", cs.matches(r"^Ω$"))
    optimization_results = extract_optimization_results(
        model_instance=model_instance,
        optimization_results=optimization_results
    )
    return optimization_results

def extract_second_stage_optimization_results(
    
    model_instances: dict[int, pyo.ConcreteModel],
    timeseries: pl.DataFrame,

) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:

    timeseries = timeseries.select(
        "timestamp",
        cs.matches(r"^sim_idx$"),
        cs.matches(r"^T$"),
        cs.matches(r"^F$"),
        cs.matches(r"^Ω$")
    )

    optimization_results: pl.DataFrame = pl.DataFrame()
    powered_volume_overage: pl.DataFrame = pl.DataFrame()
    powered_volume_shortage: pl.DataFrame = pl.DataFrame()
    for key, model_instance in model_instances.items():
        optimization_results = pl.concat(
            [
                optimization_results,
                extract_optimization_results(
                    model_instance=model_instance,
                    optimization_results=timeseries.filter(c("sim_idx") == key),
                )
            ], how="diagonal_relaxed",
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
        

    powered_volume_overage = powered_volume_overage.pivot(
        on="H", values="powered_volume_overage", index="sim_idx"
    )
    powered_volume_shortage = powered_volume_shortage.pivot(
        on="H", values="powered_volume_shortage", index="sim_idx"
    )

    return optimization_results, powered_volume_overage, powered_volume_shortage

def extract_powered_volume_quota(
    model_instance: pyo.ConcreteModel, first_stage_nb_timestamp: int
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
