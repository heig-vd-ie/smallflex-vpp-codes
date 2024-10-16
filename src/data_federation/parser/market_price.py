import json
from config import settings
import os
import polars as pl
from polars import col as c
import polars.selectors as cs

from data_federation.input_model import SmallflexInputSchema
os.chdir(os.getcwd().replace("/src", ""))


def parse_apg_market_price(small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    market_price: pl.DataFrame = pl.DataFrame()
    
    for name in ["apg_capacity", "apg_energy"]:
        col_name = "Capacity Price [€/MWh]" if name == "apg_capacity" else "Energy Price [€/MWh]"
        for entry in os.scandir(market_price_metadata[name]["folder"]):
            file_path: str = entry.path
            data = pl.read_csv(file_path, separator=",", has_header=True, null_values=["NA"])
            data = data\
            .with_columns([
                pl.col("Time from [CET/CEST]").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC").alias("timestamp"),
                pl.concat_str(["Type", "Direction"], separator="_").alias("metadata"),
                (pl.col("Quantity [MW]") * pl.col(col_name)).alias("volume_price")
            ]).group_by(["timestamp", "metadata"]).agg(
                c(col_name).max().alias("max"),
                ((c(col_name) *c("Quantity [MW]")).sum()/c("Quantity [MW]").sum()).alias("mean"),
                c(col_name).min().alias("min")
            ).with_columns(
                c("metadata").replace_strict(market_price_metadata[name]["data"], default=None)
            ).unnest("metadata")\
            .filter(pl.any_horizontal(~c("max", "mean", "min").is_null()))
            market_price = pl.concat([market_price, data], how="diagonal_relaxed")
        
    return small_flex_input_schema.add_table(market_price=market_price)

def parse_da_ida_market_price(small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    market_price: pl.DataFrame = pl.DataFrame()
    for name in ["da", "ida"]:

        data = pl.read_csv(market_price_metadata[name]["file"], separator=";")\
                .slice(offset=4).with_columns(
                    c("ID").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC").alias("timestamp")
                ).unpivot(index="timestamp", on= ~cs.by_name("timestamp", "ID"), variable_name="country", value_name="mean")\
                .with_columns(
                    c("mean").cast(pl.Float64),
                ).with_columns(
                    pl.lit(value).alias(name) for name, value in market_price_metadata[name]["data"].items()
                ).drop_nulls("mean")
        market_price = pl.concat([market_price, data], how="diagonal_relaxed")
    return small_flex_input_schema.add_table(market_price=market_price)

def parse_market_price(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]) -> SmallflexInputSchema:

    market_price_metadata: dict = json.load(open(input_file_names["market_price_metadata"]))
    
    kwargs: dict= {"small_flex_input_schema": small_flex_input_schema, "market_price_metadata": market_price_metadata}
    kwargs["small_flex_input_schema"] = parse_apg_market_price(**kwargs)
    kwargs["small_flex_input_schema"] = parse_da_ida_market_price(**kwargs)
    
    return kwargs["small_flex_input_schema"]