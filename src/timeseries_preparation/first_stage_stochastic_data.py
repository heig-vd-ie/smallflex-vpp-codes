from typing import Optional
import polars as pl
import polars.selectors as cs
from datetime import timedelta
from polars import col as c

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig


def process_first_stage_timeseries_data(
    smallflex_input_schema: SmallflexInputSchema,
    data_config: DataConfig,
    water_basin_mapping: dict[str, int],
    custom_market_prices: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:

    

    discharge_volume_synthesized = (
        smallflex_input_schema.discharge_volume_synthesized.filter(
            c("scenario").is_in(data_config.scenario_list)
        )
        .with_columns(
            c("scenario").alias("Ω"),
            c("basin_fk").replace_strict(water_basin_mapping, default=None).alias("B"),
        )
        .filter(pl.struct(["timestamp", "Ω", "B"]).is_first_distinct())
        .drop_nulls(subset=["B"])
    )

    discharge_volume_synthesized = discharge_volume_synthesized.pivot(
        values="discharge_volume",
        index=["timestamp", "Ω"],
        on="B",
    ).select(
        "timestamp",
        "Ω",
        pl.all().exclude(["timestamp", "Ω"]).name.prefix("discharge_volume_"),
    )
    if custom_market_prices is None:
        market_price_synthesized = (
            smallflex_input_schema.market_price_synthesized.filter(
                c("market") == data_config.market
            )
            .filter(c("scenario").is_in(data_config.scenario_list))
            .filter(pl.struct(["timestamp", "scenario"]).is_first_distinct())
            .select("timestamp", c("scenario").alias("Ω"), "market_price")
        )

        timeseries_synthesized = market_price_synthesized.join(
            pl.DataFrame(discharge_volume_synthesized), on=["timestamp", "Ω"], how="left"
        )
    else:
        daily_market_price = custom_market_prices.group_by_dynamic("timestamp", every=timedelta(days=1)).agg(
            pl.col("da").mean().alias("market_price"),
        ).with_columns(
            c("timestamp").dt.ordinal_day().alias("day_of_year")
        )
        discharge_volume_synthesized = discharge_volume_synthesized.with_columns(
            c("timestamp").dt.ordinal_day().alias("day_of_year")
        )

        timeseries_synthesized = daily_market_price.join(
            discharge_volume_synthesized.drop("timestamp"), on="day_of_year", how="right"
        ).drop("day_of_year")

    return timeseries_synthesized
