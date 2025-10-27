import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo
from numpy_function import clipped_cumsum
from general_function import pl_to_dict
from pipelines.data_configs import DataConfig
from tqdm.auto import tqdm

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
        "total_power_forecast", "basin_volume", "discharge_volume", 
        "spilled_volume", "flow", "hydro_power_forecast", "hydro_power", "hydro_ancillary_reserve",
        "battery_charging_power", "battery_discharging_power", "battery_soc", 
        "battery_ancillary_reserve", "pv_power","pv_power_measured", "wind_power", "wind_power_measured"
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
            elif attribute in ["flow", "hydro_power", "hydro_power_forecast"]:
                data = pivot_result_table(
                    df=data, on="H", index=["T"], values=attribute
                )
            elif attribute == "battery_charging_power":
                data = data.with_columns(
                    c("battery_charging_power") * -1
                )
            
            optimization_results = optimization_results.join(data, on=col_list, how="left")

    return optimization_results

def extract_first_stage_optimization_results(
    model_instance: pyo.ConcreteModel, timeseries: pl.DataFrame,
) -> pl.DataFrame:
    optimization_results = timeseries.select(
        "timestamp", "T", 
        cs.matches(r"^Ω$"), 
        cs.contains("market_price")
    )
    optimization_results = extract_optimization_results(
        model_instance=model_instance,
        optimization_results=optimization_results
    )
    return optimization_results

def extract_second_stage_optimization_results(
    
    model_instances: dict[int, pyo.ConcreteModel],
    timeseries: pl.DataFrame,
    data_config: DataConfig

) ->  tuple[pl.DataFrame, float]:

    timeseries = timeseries.select(
        "timestamp",
        cs.matches(r"^sim_idx$"),
        cs.matches(r"^T$"),
        cs.matches(r"^F$"),
        cs.matches(r"^Ω$"),
        cs.contains("market_price"),
        cs.contains("imbalance")
    )
    optimization_results: pl.DataFrame = pl.DataFrame()

    for key, model_instance in tqdm(model_instances.items(), desc="Extracting second stage results", leave=False):
        optimization_results = pl.concat(
            [
                optimization_results,
                extract_optimization_results(
                    model_instance=model_instance,
                    optimization_results=timeseries.filter(c("sim_idx") == key),
                )
            ], how="diagonal_relaxed",
        )

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
            (pl.sum_horizontal(cs.contains("ancillary_reserve")) * c("ancillary_market_price") / data_config.nb_timestamp_per_ancillary).alias("ancillary_income")
        )

    income = optimization_results.select(cs.contains("income")).to_numpy().sum()
    market_price_upper_quantile = optimization_results["market_price"].quantile(data_config.market_price_upper_quantile)
    market_price_lower_quantile = optimization_results["market_price"].quantile(data_config.market_price_lower_quantile) # type: ignore

    rated_alpha = extract_result_table(list(model_instances.values())[-1], "rated_alpha")
    end_basin_volume = extract_result_table(list(model_instances.values())[-1], "end_basin_volume")
    start_basin_volume = extract_result_table(list(model_instances.values())[0], "start_basin_volume")
    basin_volume_range = extract_result_table(list(model_instances.values())[0], "basin_volume_range")
    end_volume_penalty = start_basin_volume\
        .join(end_basin_volume, on="B", how="inner")\
        .join(rated_alpha, left_on="B", right_on="UP_B", how="inner")\
        .join(basin_volume_range, on="B", how="inner")\
        .with_columns(
            (c("end_basin_volume") - c("start_basin_volume")).alias("end_basin_volume_diff")
        ).with_columns(
            (
                c("end_basin_volume_diff") * c("rated_alpha") * c("basin_volume_range")/ 3600 *
                pl.when(c("end_basin_volume_diff") > 0)
                .then(market_price_lower_quantile)
                .otherwise(market_price_upper_quantile)
            ).alias("end_volume_penalty")
        )["end_volume_penalty"].to_numpy().sum()
    print(f"End volume penalty: {end_volume_penalty:.2f} EUR")
    adjusted_income = income + end_volume_penalty

    return optimization_results, adjusted_income

def extract_third_stage_optimization_results(
    
    model_instances: dict[int, pyo.ConcreteModel],
    timeseries: pl.DataFrame,
    data_config: DataConfig

) ->  tuple[pl.DataFrame, float, float]:

    timeseries = timeseries.select(
        "timestamp",
        cs.matches(r"^sim_idx$"),
        cs.matches(r"^T$"),
        cs.matches(r"^F$"),
        cs.matches(r"^Ω$"),
        cs.contains("market_price"),
        cs.contains("imbalance")
    )
    optimization_results: pl.DataFrame = pl.DataFrame()
    
    for key, model_instance in tqdm(model_instances.items(), desc="Extracting third stage results", leave=False):
        optimization_results = pl.concat(
            [
                optimization_results,
                extract_optimization_results(
                    model_instance=model_instance,
                    optimization_results=timeseries.filter(c("sim_idx") == key),
                )
            ], how="diagonal_relaxed",
        )

    optimization_results = (
        optimization_results
        .with_columns(
            pl.sum_horizontal(cs.contains("power").and_(~cs.contains("forecast"))).alias("total_power_real"),
        ).with_columns(
            (c("total_power_real") - c("total_power_forecast")).alias("power_difference"),
            (c("total_power_forecast") * c("market_price")).alias("da_income")
        ).with_columns(
            pl.when(c("power_difference") > 0)
            .then(c("power_difference") * c("long_imbalance"))
            .otherwise(c("power_difference") * c("short_imbalance")).alias("imbalance_penalty")
        )
    )

    income = optimization_results.select(cs.contains("income")).to_numpy().sum()
    imbalance_penalty = optimization_results["imbalance_penalty"].sum()

    market_price_upper_quantile = optimization_results["market_price"].quantile(data_config.market_price_upper_quantile)
    market_price_lower_quantile = optimization_results["market_price"].quantile(data_config.market_price_lower_quantile)

    rated_alpha = extract_result_table(list(model_instances.values())[-1], "rated_alpha")
    end_basin_volume = extract_result_table(list(model_instances.values())[-1], "end_basin_volume")
    start_basin_volume = extract_result_table(list(model_instances.values())[0], "start_basin_volume")
    basin_volume_range = extract_result_table(list(model_instances.values())[0], "basin_volume_range")
    end_volume_penalty = start_basin_volume\
        .join(end_basin_volume, on="B", how="inner")\
        .join(rated_alpha, left_on="B", right_on="UP_B", how="inner")\
        .join(basin_volume_range, on="B", how="inner")\
        .with_columns(
            (c("end_basin_volume") - c("start_basin_volume")).alias("end_basin_volume_diff")
        ).with_columns(
            (
                c("end_basin_volume_diff") * c("rated_alpha") * c("basin_volume_range")/ 3600 *
                pl.when(c("end_basin_volume_diff") > 0)
                .then(market_price_lower_quantile)
                .otherwise(market_price_upper_quantile)
            ).alias("end_volume_penalty")
        )["end_volume_penalty"].to_numpy().sum()

    adjusted_income = income + end_volume_penalty + imbalance_penalty
    
    return optimization_results, adjusted_income, imbalance_penalty




def extract_basin_volume_expectation(
    model_instance: pyo.ConcreteModel,
    optimization_results: pl.DataFrame,
    water_basin: pl.DataFrame,
    data_config: DataConfig
) -> pl.DataFrame:


    volume_range = pl_to_dict(water_basin["B", "volume_range"])
    start_volume_mapping = pl_to_dict(water_basin["B", "start_volume"])
    basin_idx = water_basin["B"].to_list()
    
    basin_volume_raw = optimization_results.select(
        "T", "Ω", cs.contains("basin_volume_"), cs.contains("spilled_volume_")
        )

    end_basin_volume = extract_result_table(
            model_instance=model_instance, var_name="end_basin_volume"
        ).with_columns(
            pl.lit(optimization_results["T"].max() + 1).alias("T"), # type: ignore
            ("basin_volume_" + c("B").cast(pl.Utf8)).alias("B")
        ).pivot(
            index=["T", "Ω"],
            on="B",
            values="end_basin_volume"
        )
    basin_volume_raw = pl.concat([basin_volume_raw, end_basin_volume], how="diagonal_relaxed")

    basin_volume_raw = basin_volume_raw.sort(["Ω", "T"]).select(
            "Ω", "T",
            *[(
                (
                    (c(f"spilled_volume_{col}")/volume_range[col]).shift(1) + 
                    c(f"basin_volume_{col}").diff().over("Ω")
                ).fill_null(start_volume_mapping[col])
                ).alias(f"basin_volume_{col}")
                for col in basin_idx
            ]
    )

    if basin_volume_raw.shape[1] == 3:
        basin_name = basin_volume_raw.drop(["Ω", "T"]).columns[0]
        basin_volume_raw = basin_volume_raw.pivot(
            on="Ω", values=cs.starts_with("basin_volume"), index="T",
        ).select(
            "T",
            pl.all().exclude("T").name.prefix(basin_name + "_")
        )
    else:
        basin_volume_raw = basin_volume_raw.pivot(
            on="Ω", values=cs.starts_with("basin_volume"), index="T",
        )

    cleaned_basin_volume = (
            pl.DataFrame(
                clipped_cumsum(basin_volume_raw.drop("T").to_numpy(), xmin=0, xmax=1),
                schema=basin_volume_raw.drop("T").columns,
            ).with_columns(basin_volume_raw["T"])
            .unpivot(variable_name="BΩ", value_name=f"basin_volume", index="T")
            .with_columns(
                c("BΩ").str.split("_")
                .list.slice(2).cast(pl.List(pl.UInt32))
                .list.to_struct(fields=["B", "Ω"])
            ).unnest("BΩ")
    )
    
    quantile_columns = [
        expr
        for i, (quantile, quantile_min) in 
        enumerate(zip(data_config.basin_volume_quantile, data_config.basin_volume_quantile_min))
        for expr in (
            c("basin_volume").quantile(0.5 - quantile)
                .clip(upper_bound=c("basin_volume").mean() - quantile_min)
                .clip(lower_bound=0)
                .alias(f"lower_quantile_{i}"),
            c("basin_volume").quantile(0.5 + quantile)
                .clip(lower_bound=c("basin_volume").mean() + quantile_min)
                .clip(upper_bound=100)
                .alias(f"upper_quantile_{i}")
        )
    ]

    stat_volume: pl.DataFrame = cleaned_basin_volume.group_by("T", "B").agg(
            c("basin_volume").mean().alias("mean"), *quantile_columns
        ).sort(["B", "T"])

    col_list = stat_volume.drop(["T", "B"]).columns
        
    mean_stat_volume = stat_volume.with_columns(
            c(col_list).rolling_mean(window_size=7).shift(-3).over("B")
        )
    # build missing values at beginning and end
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
    )

    mean_stat_volume = mean_stat_volume.\
        with_columns(
            c("mean").diff().shift(-1).alias("diff_volume")
        )
    return mean_stat_volume

