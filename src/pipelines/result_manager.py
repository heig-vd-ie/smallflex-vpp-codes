import polars as pl
from polars import col as c
from typing import Tuple, Union
from polars import selectors as cs
import pyomo.environ as pyo
from numpy_function import clipped_cumsum
from general_function import pl_to_dict
from pipelines.data_configs import DataConfig


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


def extract_basin_volume_expectation(
    optimization_results: pl.DataFrame,
    water_basin: pl.DataFrame,
    data_config: DataConfig
) -> pl.DataFrame:

    max_volume_mapping = pl_to_dict(water_basin["B", "volume_max"])
    min_volume_mapping = pl_to_dict(water_basin["B", "volume_min"])
    start_volume_mapping = pl_to_dict(water_basin["B", "start_volume"])
    basin_idx = water_basin["B"].to_list()

    optimization_results = optimization_results.sort(["Ω", "T"]).with_columns(
            (
                (
                    c(f"spilled_volume_{col}").shift(1)
                    + c(f"basin_volume_{col}").diff().over("Ω")
                ).fill_null(start_volume_mapping[col])
                / (max_volume_mapping[col] - min_volume_mapping[col])
                * 100
            ).alias(f"basin_volume_{col}")
            for col in basin_idx
    )
    basin_volume = optimization_results["T", "Ω"]

    for col in basin_idx:
        basin_volume_df = optimization_results.pivot(
            on="Ω", values=f"basin_volume_{col}", index="T"
        )
        
        basin_volume_df = (
            pl.DataFrame(
                clipped_cumsum(basin_volume_df.drop("T").to_numpy(), xmin=0, xmax=100),
                schema=basin_volume_df.drop("T").columns,
            )
            .with_columns(basin_volume_df["T"])
            .unpivot(variable_name="Ω", value_name=f"basin_volume_{col}", index="T")
            .with_columns(c("Ω").cast(pl.UInt32))
        )
        basin_volume = basin_volume.join(
            basin_volume_df, on=["T", "Ω"], how="left"
        )

    col_list = ["median", "lower_quantile", "upper_quantile"]

    stat_volume: pl.DataFrame = basin_volume\
        .unpivot(
            variable_name="B", value_name="basin_volume", 
            on=cs.starts_with("basin_volume_"), index=["T", "Ω"]
        ).with_columns(
            c("B").str.replace("basin_volume_", "").cast(pl.Int32)
        ).group_by("T", "B").agg(
            c("basin_volume").median().alias("median"),
            c("basin_volume").quantile(data_config.lower_quantile).alias("lower_quantile"),
            c("basin_volume").quantile(0.9).alias("upper_quantile")
        ).sort(["B", "T"])\
        .with_columns(
            c("lower_quantile").clip(upper_bound=c("median") - data_config.min_quantile_diff).clip(lower_bound=0),
            c("upper_quantile").clip(lower_bound=c("median") + data_config.min_quantile_diff).clip(upper_bound=100)
        )
        
    mean_stat_volume = stat_volume.with_columns(
            c(col_list).rolling_mean(window_size=7).shift(-3).over("B")
        )

    mean_stat_volume = mean_stat_volume.join(
        stat_volume.filter(c("T").is_in([stat_volume["T"].max(), stat_volume["T"].min()])),
        on=["T", "B"],
        how="left",
        suffix="_raw"
    ).select(
        "T", "B", 
        *[
            pl.coalesce(cs.starts_with(col)).interpolate().alias(col) 
            for col in col_list
        ]
    ).with_columns(
        c(col_list) * 
        (c("B").replace_strict(max_volume_mapping) - c("B").replace_strict(min_volume_mapping)) + 
        c("B").replace_strict(min_volume_mapping)
    )
    return mean_stat_volume

