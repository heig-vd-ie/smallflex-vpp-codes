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
    custom_market_prices: Optional[pl.DataFrame] = None,
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

    weather_forecast = process_weather_forecast_data(
        smallflex_input_schema=smallflex_input_schema,
        data_config=data_config
    )
    
    if custom_market_prices is None:
        market_prices: pl.DataFrame = process_market_prices_data(
            smallflex_input_schema=smallflex_input_schema,
            data_config=data_config
        )
    else:
        diff_short = 1.23
        diff_long = 1.37

        lower_quantile = custom_market_prices["da"].quantile(quantile=data_config.market_price_lower_quantile)
        upper_quantile = custom_market_prices["da"].quantile(quantile=data_config.market_price_upper_quantile)

        market_prices: pl.DataFrame = custom_market_prices.select(
            "timestamp",
            (c("timestamp") - c("timestamp").dt.truncate("1y")).dt.total_hours().alias("hour_of_year"),
            c("da").alias("market_price"),
            pl.lit(lower_quantile).alias("market_price_lower_quantile"),
            pl.lit(upper_quantile).alias("market_price_upper_quantile"),
            c("fcr").alias("ancillary_market_price"),
        ).slice(0, 365*24)
        if "short_imbalance" not in custom_market_prices.columns:
            market_prices = market_prices.with_columns(   
                (c("market_price") + c("market_price").abs()*diff_short).alias("short_imbalance"),
            )
        if "long_imbalance" not in custom_market_prices.columns:
            market_prices = market_prices.with_columns( 
                (c("market_price") - c("market_price").abs()*diff_long).alias("long_imbalance")
            )
        
    input_timeseries = market_prices.join(weather_forecast.drop("timestamp"), on="hour_of_year", how="left")

    
    timeseries_forecast = input_timeseries.select(
        "timestamp",
        cs.ends_with("_mean_forecast").name.map(lambda c: c.removesuffix("_mean_forecast").lower()),
        cs.contains("market_price")
    )

    timeseries_measurement = input_timeseries.select(
        ~cs.ends_with("_mean_forecast")
    )
    return timeseries_forecast, timeseries_measurement


def process_weather_forecast_data(
    smallflex_input_schema: SmallflexInputSchema,
    data_config: DataConfig,
) -> pl.DataFrame:
    
    weather_forecast = pl.DataFrame()

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
        
        if weather_forecast.is_empty():
            weather_forecast = df
        else:
            weather_forecast = weather_forecast.join(df, on="timestamp", how="inner")
        

    pv_max = (
        weather_forecast["irradiation", "irradiation_mean_forecast"].to_numpy().max()
    )
    weather_forecast = (
        weather_forecast.with_columns(
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
                * 2.5 # 2.5 to get the same volume as the historical measurements
                * data_config.second_stage_timestep.total_seconds() # discharge volume in one hour
            )  
        )
    ).filter(c("timestamp").is_first_distinct())


    weather_forecast = weather_forecast.with_columns(
        (c("timestamp") - c("timestamp").dt.truncate("1y")).dt.total_hours().alias("hour_of_year")
    ).sort("hour_of_year").unique("hour_of_year").slice(0, 365*24)\
    .with_columns(
        (pl.datetime(year=2026, month=1, day=1, time_zone="UTC") + pl.duration(hours=c("hour_of_year"))).alias("timestamp")
    )


    return weather_forecast


def process_market_prices_data(
    smallflex_input_schema: SmallflexInputSchema,
    data_config: DataConfig,
) -> pl.DataFrame:

    da_market_price: pl.DataFrame = smallflex_input_schema.market_price_measurement.filter(
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
    
    # da_market_price = da_market_price.select(
    #     "market_price", "market_price_lower_quantile", "market_price_upper_quantile"
    # )

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
    )

    ancillary_market_price: pl.DataFrame = (
            smallflex_input_schema.market_price_measurement.filter(
                c("country") == data_config.market_country
            )
            .filter(c("market") == data_config.ancillary_market)
            .filter(c("source") == data_config.market_source)
            .sort("timestamp")
        ).select(
            "timestamp",
            (c(data_config.fcr_value)).alias("ancillary_market_price")
        )
    
    market_prices = da_market_price\
        .join(imbalance_price, how="left", on="timestamp")\
        .join(ancillary_market_price, how="left", on="timestamp")
        
    market_prices = market_prices.sort("timestamp").filter(c("timestamp").dt.year()== 2024).with_columns(
        (c("timestamp") - c("timestamp").dt.truncate("1y")).dt.total_hours().alias("hour_of_year")
    ).slice(0, 365*24)
    
    return market_prices