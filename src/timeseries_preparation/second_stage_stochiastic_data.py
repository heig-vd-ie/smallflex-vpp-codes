from typing import Optional
import polars as pl
import polars.selectors as cs
from datetime import timedelta
from polars import col as c

from smallflex_data_schema import SmallflexInputSchema

from utility.data_preprocessing import (
    generate_basin_volume_table,
    clean_hydro_power_performance_table,
    split_timestamps_per_sim,
    generate_hydro_power_state,
    generate_first_stage_basin_state_table,
    generate_clean_timeseries,
    generate_datetime_index,
)

from general_function import pl_to_dict
from pipelines.data_manager import DataManager


def process_second_stage_stochastic_data(
    smallflex_input_schema: SmallflexInputSchema,
    data_manager: DataManager,
) -> pl.DataFrame:
    """Process the second stage stochastic data.

    Args:
        smallflex_input_schema (SmallflexInputSchema): The input data schema.
        data_manager (DataManager): The data manager instance.
        wind_power_mask (Optional[pl.Expr], optional): The mask to identify wind power plants. Defaults to None.
        pv_power_mask (Optional[pl.Expr], optional): The mask to identify PV power plants. Defaults to None.
        hydro_power_mask (Optional[pl.Expr], optional): The mask to identify hydro power plants. Defaults to None.

    Returns:
        pl.DataFrame: The processed second stage stochastic data.
    """

    input_timeseries = pl.DataFrame()

    forecast: dict[str, pl.DataFrame] = {}
    measurement: dict[str, pl.DataFrame] = {}

    forecast["irradiation"] = pl.DataFrame(
        smallflex_input_schema.weather_forecast.filter(c("type") == "irradiation")
    )
    forecast["wind"] = pl.DataFrame(
        smallflex_input_schema.weather_forecast.filter(c("type") == "wind")
    )
    forecast["discharge_volume_0"] = pl.DataFrame(
        smallflex_input_schema.discharge_flow_forecast.filter(
            c("location") == "Griessee"
        )
    )

    measurement["irradiation"] = pl.DataFrame(
        smallflex_input_schema.weather_measurement["timestamp", "irradiation"]
    )
    measurement["wind"] = pl.DataFrame(
        smallflex_input_schema.weather_measurement["timestamp", "wind"]
    )
    measurement["discharge_volume_0"] = pl.DataFrame(
        smallflex_input_schema.discharge_flow_measurement.filter(
            c("location") == "Griessee"
        ).select(["timestamp", c("value").alias("discharge_volume_0")])
    )

    for key, df in forecast.items():
        df = (
            df.with_columns(cs.starts_with("rm").list.slice(0, 37))
            .explode(cs.starts_with("rm"))
            .with_columns(
                c("timestamp")
                + pl.duration(hours=1 + c("timestamp").cum_count().over("timestamp"))
            )
            .filter(c("timestamp").is_first_distinct())
            .select(
                c("timestamp"),
                pl.mean_horizontal(cs.starts_with("rm")).alias(f"{key}_mean_forecast"),
            )
        )
        df = (
            df.join(measurement[key], on="timestamp", how="inner", coalesce=True)
            .sort("timestamp")
            .upsample(every=timedelta(hours=1), time_column="timestamp")
            .fill_null(0)
        )
        if input_timeseries.is_empty():
            input_timeseries = df
        else:
            input_timeseries = input_timeseries.join(df, on="timestamp", how="inner")

    pv_max = (
        input_timeseries["irradiation", "irradiation_mean_forecast"].to_numpy().max()
    )
    input_timeseries = (
        input_timeseries.with_columns(
            (c(f"irradiation{col}") / pv_max * data_manager.pv_power_rated_power).alias(
                f"pv_power{col}"
            )
            for col in ("", "_mean_forecast")
        )
        .with_columns(
            pl.when(
                c(f"wind{col}").is_between(
                    data_manager.wind_speed_cut_in, data_manager.wind_speed_cut_off
                )
            )
            .then(
                c(f"wind{col}").pow(3)
                / (data_manager.wind_speed_cut_off**3)
                * data_manager.wind_turbine_rated_power
            )
            .otherwise(0)
            .alias(f"wind_power{col}")
            for col in ("", "_mean_forecast")
        )
        .with_columns(
            (
                cs.starts_with("discharge_volume_")
                * 2.5
                * data_manager.second_stage_timestep.total_seconds()
            )  # discharge volume in one hour
        )
    ).filter(c("timestamp").is_first_distinct())

    start_date = input_timeseries.filter(c("timestamp").dt.hour() == 0)[
        "timestamp"
    ].min()
    end_date = start_date + timedelta(days=365)  # type: ignore
    input_timeseries = input_timeseries.filter(
        c("timestamp").is_between(start_date, end_date, closed="left")
    )

    input_timeseries = input_timeseries.with_columns(
        pl.when(c("timestamp").dt.year() == 2024)
        .then(c("timestamp") + pl.duration(days=365))
        .otherwise(c("timestamp"))
    ).sort("timestamp")

    market_price: pl.DataFrame = smallflex_input_schema.market_price_measurement.filter(
        c("country") == data_manager.market_country
    ).filter(c("market") == data_manager.market)

    ancillary_market_price: pl.DataFrame = (
        smallflex_input_schema.market_price_measurement.filter(
            c("country") == data_manager.market_country
        )
        .filter(c("market") == data_manager.ancillary_market)
        .filter(c("source") == data_manager.market_source)
        .sort("timestamp")
    )
    imbalance_price: pl.DataFrame = (
        smallflex_input_schema.market_price_measurement.filter(
            c("market") == "imbalance"
        )
        .with_columns(
            c("direction").replace_strict(
                {"neg": "short_imbalance", "pos": "long_imbalance"}
            )
        )
        .pivot(on="direction", values="avg", index="timestamp")
        .sort("timestamp")
        .with_columns(
            (c("timestamp") + pl.duration(days=2 * 366)).alias("timestamp"),
        )
    )

    market_price = market_price.select(
        c("timestamp") + pl.duration(days=2 * 366), c("avg").alias("market_price")
    )
    ancillary_market_price = ancillary_market_price.select(
        c("timestamp") + pl.duration(days=2 * 366),
        c("avg").alias("ancillary_market_price"),
    )

    input_timeseries = (
        input_timeseries.join(market_price, on="timestamp", how="left")
        .join(ancillary_market_price, on="timestamp", how="left")
        .join(imbalance_price, on="timestamp", how="left")
        .with_row_index(name="T")
        .with_columns(
            c(
                "market_price",
                "ancillary_market_price",
                "short_imbalance",
                "long_imbalance",
            )
            .forward_fill()
            .backward_fill()
        )
    )

    return input_timeseries
