from typing import Optional
import polars as pl
import polars.selectors as cs
from datetime import timedelta
from polars import col as c

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig


def process_second_stage_timeseries_stochastic_data(
    smallflex_input_schema: SmallflexInputSchema,
    data_config: DataConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Process the second stage stochastic data.

    Args:
        smallflex_input_schema (SmallflexInputSchema): The input data schema.
        data_config (DataManager): The data manager instance.
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
        )
        
        days = df.filter(c(key).is_null()).with_columns(c("timestamp").dt.date())["timestamp"].unique()
        df = df.filter(~c("timestamp").dt.date().is_in(days.to_list()))

        df = df.with_columns(
            pl.datetime_range(
                start=df["timestamp"][0], 
                end=df["timestamp"][-1],
                interval=timedelta(hours=1),
                time_zone="UTC", eager=True
            ).slice(0, df.height).alias("timestamp")
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
            (c(f"irradiation{col}") / pv_max * data_config.pv_power_rated_power).alias(
                f"pv_power{col}"
            )
            for col in ("", "_mean_forecast")
        )
        .with_columns(
            pl.when(
                c(f"wind{col}").is_between(
                    data_config.wind_speed_cut_in, data_config.wind_speed_cut_off
                )
            )
            .then(
                c(f"wind{col}").pow(3)
                / (data_config.wind_speed_cut_off**3)
                * data_config.wind_turbine_rated_power
            )
            .otherwise(0)
            .alias(f"wind_power{col}")
            for col in ("", "_mean_forecast")
        )
        .with_columns(
            (
                cs.starts_with("discharge_volume_")
                * 2.5
                * data_config.second_stage_timestep.total_seconds() # discharge volume in one hour
            )  
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
        c("country") == data_config.market_country
    ).filter(c("market") == data_config.market)\
    .select(c("timestamp"), c("avg").alias("market_price"))\
    .with_columns(
        pl.col("market_price").rolling_quantile(
            quantile=data_config.market_price_lower_quantile, 
            window_size=data_config.market_price_window_size * 24).alias("market_price_lower_quantile"),
        pl.col("market_price").rolling_quantile(
            quantile=data_config.market_price_upper_quantile,
            window_size=data_config.market_price_window_size  * 24).alias("market_price_upper_quantile"),
    )

    imbalance_price: pl.DataFrame = (
        smallflex_input_schema.market_price_measurement.filter(
            c("market") == "imbalance"
        )
        .with_columns(
            c("direction").replace_strict(
                {"neg": "short_imbalance", "pos": "long_imbalance"}
            )
        ).filter(pl.struct(["timestamp", "direction"]).is_first_distinct())
        .pivot(on="direction", values="avg", index="timestamp")
        .sort("timestamp")
        .with_columns(
            (c("timestamp") + pl.duration(days=1 * 365)).alias("timestamp"),
        )
    )

    market_price = market_price.select(
        c("timestamp") + pl.duration(days=1 * 365), 
        "market_price", "market_price_lower_quantile", "market_price_upper_quantile"
    )
    ancillary_market_price: pl.DataFrame = (
            smallflex_input_schema.market_price_measurement.filter(
                c("country") == data_config.market_country
            )
            .filter(c("market") == data_config.ancillary_market)
            .filter(c("source") == data_config.market_source)
            .sort("timestamp")
        ).select(
            c("timestamp") + pl.duration(days=1 * 365),
            (c(data_config.fcr_value)).alias("ancillary_market_price")
        )

    input_timeseries = (
        input_timeseries
        .join(market_price, on="timestamp", how="left")
        .join(ancillary_market_price, on="timestamp", how="left")
        .join(imbalance_price, on="timestamp", how="left")
        .with_columns(
            c(
                "market_price",
                "short_imbalance",
                "long_imbalance",
                "ancillary_market_price"
            )
            .forward_fill()
            .backward_fill()
        )
    ).unique("timestamp")
    timeseries_forecast = input_timeseries.select(
        "timestamp",
        cs.ends_with("_mean_forecast").name.map(lambda c: c.removesuffix("_mean_forecast").lower()),
        cs.contains("market_price")
    )

    timeseries_measurement = input_timeseries.select(
        ~cs.ends_with("_mean_forecast")
    )
    return timeseries_forecast, timeseries_measurement
