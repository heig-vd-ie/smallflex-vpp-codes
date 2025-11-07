from typing import Optional
import polars as pl
from polars import col as c

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig

def process_timeseries_data(
    smallflex_input_schema: SmallflexInputSchema,
    data_config: DataConfig,
    basin_index_mapping: dict[str, int],
    pv_power_mask: Optional[pl.Expr] = None,
    wind_power_mask: Optional[pl.Expr] = None,
    fcr_factor: float = 1,
) -> pl.DataFrame:
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
    if pv_power_mask is None:
        pv_power_mask = pl.lit(False)
    if wind_power_mask is None:
        wind_power_mask = pl.lit(False)

    discharge_volume: pl.DataFrame = (
        (
            smallflex_input_schema.discharge_flow_historical.with_columns(
                c("basin_fk")
                .replace_strict(basin_index_mapping, default=None)
                .alias("B")
            )
            .drop_nulls("B")
            .drop_nulls(subset="basin_fk")
            .with_columns(
                (c("value") * data_config.second_stage_timestep.total_seconds()).alias(
                    "discharge_volume"
                )
            )
        )
        .pivot(index="timestamp", on="B", values="discharge_volume")
        .select(
            "timestamp", pl.all().exclude("timestamp").name.prefix("discharge_volume_")
        )
    )

    market_price: pl.DataFrame = (
        smallflex_input_schema.market_price_measurement.filter(
            c("country") == data_config.market_country
        ).filter(c("market") == data_config.market)
        .filter(c("timestamp").is_first_distinct())
    ).select("timestamp", c("avg").alias("market_price"))

    ancillary_market_price: pl.DataFrame = (
        smallflex_input_schema.market_price_measurement.filter(
            c("country") == data_config.market_country
        )
        .filter(c("market") == data_config.ancillary_market)
        .filter(c("source") == data_config.market_source)
        .sort("timestamp")
    ).select("timestamp", (c("avg") * fcr_factor).alias("ancillary_market_price"))

    pv_production: pl.DataFrame = smallflex_input_schema.weather_historical.filter(
        pv_power_mask
    )

    pv_production = pv_production.with_columns(
        (
            c("irradiation")
            / pv_production["irradiation"].max()
            * data_config.pv_power_rated_power
        ).alias("pv_power")
    )["timestamp", "pv_power"]

    wind_production: pl.DataFrame = smallflex_input_schema.weather_historical.filter(
        wind_power_mask
    )

    wind_production = wind_production.select(
        c("timestamp"),
        pl.when(
            c("wind").is_between(
                data_config.wind_speed_cut_in, data_config.wind_speed_cut_off
            )
        )
        .then(
            c("wind").pow(3)
            / (data_config.wind_speed_cut_off**3)
            * data_config.wind_turbine_rated_power
        )
        .otherwise(0)
        .alias("wind_power"),
    )

    input_timeseries = (
        discharge_volume.join(pl.DataFrame(market_price), on="timestamp", how="left")
        .join(pl.DataFrame(ancillary_market_price), on="timestamp", how="left")
        .join(pl.DataFrame(pv_production), on="timestamp", how="left")
        .join(pl.DataFrame(wind_production), on="timestamp", how="left")
        .with_columns(pl.all().forward_fill().backward_fill())
        .with_columns(c("pv_power", "wind_power").fill_null(0.0))
    )
    input_timeseries = input_timeseries.sort("timestamp").with_columns(
        pl.col("market_price").rolling_quantile(
            quantile=data_config.market_price_lower_quantile, 
            window_size=data_config.market_price_window_size * 24).alias("market_price_lower_quantile"),
        pl.col("market_price").rolling_quantile(
            quantile=data_config.market_price_upper_quantile,
            window_size=data_config.market_price_window_size  * 24).alias("market_price_upper_quantile"),
    )
    
    input_timeseries = (
        input_timeseries
        .filter(c("timestamp").dt.year() == data_config.year)
        .filter(c("timestamp").dt.ordinal_day() < 366)
    )
    return input_timeseries
