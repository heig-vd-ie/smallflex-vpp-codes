import json
from config import settings
import os
import polars as pl
from polars import col as c
import polars.selectors as cs

from data_federation.input_model import SmallflexInputSchema
from utility.general_function import dictionary_key_filtering

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
                ((c(col_name) *c("Quantity [MW]")).sum()/c("Quantity [MW]").sum()).alias("avg"),
                c(col_name).min().alias("min")
            ).with_columns(
                c("metadata").replace_strict(market_price_metadata[name]["data"], default=None)
            ).unnest("metadata")\
            .filter(pl.any_horizontal(~c("max", "avg", "min").is_null()))
            
            market_price = pl.concat([market_price, data], how="diagonal_relaxed")
        
    return small_flex_input_schema.add_table(market_price=market_price)

def parse_da_ida_market_price(small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    market_price: pl.DataFrame = pl.DataFrame()
    for name in ["da", "ida"]:

        data = pl.read_csv(market_price_metadata[name]["file"], separator=";")\
                .slice(offset=4).with_columns(
                    c("ID").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC").alias("timestamp")
                ).unpivot(index="timestamp", on= ~cs.by_name("timestamp", "ID"), variable_name="country", value_name="avg")\
                .with_columns(
                    c("avg").cast(pl.Float64),
                ).with_columns(
                    pl.lit(value).alias(name) for name, value in market_price_metadata[name]["data"].items()
                ).drop_nulls("avg")
        market_price = pl.concat([market_price, data], how="diagonal_relaxed")
    return small_flex_input_schema.add_table(market_price=market_price)

def get_reg_timestamps(date_str: pl.Expr, time_str: pl.Expr):
    date: pl.Expr = date_str.cast(pl.Datetime(time_zone="UTC"))
    time: pl.Expr = (time_str.cast(pl.Int32)*1e3*60*60).cast(pl.Duration(time_unit="ms")) # from ms to min
    return date + time

def parse_reg_frc_market_price(small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    market_price: pl.DataFrame = pl.DataFrame()
    name = "reg_fcr"
    for entry in list(os.scandir(market_price_metadata[name]["folder"])):
        if entry.name.startswith("RESULT_OVERVIEW"):
            data: pl.DataFrame = pl.read_excel(entry.path)
            if data.is_empty():
                continue
            data = data.with_columns(
                c("PRODUCTNAME").str.split("_").list.get(1).alias("time"),
            ).with_columns(
                get_reg_timestamps(date_str=c("DATE_FROM"), time_str=c("time")).alias("timestamp"),
            ).unpivot(
                on = cs.contains("SETTLEMENTCAPACITY"), index= "timestamp", 
                value_name="max", variable_name="country"
            ).with_columns(
                c("country").str.split("_").list.get(0).replace_strict(market_price_metadata[name]["country_mapping"], default=None),
                c("max").cast(pl.Float64, strict=False),
            ).with_columns(
                pl.lit(value).alias(name) for name, value in market_price_metadata[name]["data"].items()
            ).drop_nulls(subset=["max", "country"])
            market_price = pl.concat([market_price, data], how="diagonal_relaxed")
    return small_flex_input_schema.add_table(market_price=market_price)

def parse_reg_frr_market_price(small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    market_price: pl.DataFrame = pl.DataFrame()
    for name in  ["reg_afrr_ene", "reg_afrr_cap", "reg_mfrr_ene", "reg_mfrr_cap"]:
        for entry in list(os.scandir(market_price_metadata[name]["folder"])):
            if entry.name.startswith("RESULT_OVERVIEW"):
                data: pl.DataFrame = pl.read_excel(entry.path)
                if data.is_empty():
                    continue
                col_mapping = dictionary_key_filtering(market_price_metadata[name]["col_mapping"], data.columns)
                data = data[list(col_mapping.keys())].rename(col_mapping).with_columns(
                    c("min", "avg", "max").cast(pl.Float64),
                    pl.col("metadata").str.split("_").list.slice(offset=0, length=2)
                    .list.to_struct(fields=["direction", "time"])
                ).unnest("metadata")
                # From october 2022, secondary energy market will be 15 minutes-based, so we need to convert the time to 4 hours
                if len(data["time"][0]) == 3:
                    data = data.with_columns(
                        (c("time").cast(pl.Int32)//16*4).alias("time")
                    ).group_by(["date", "time", "direction"]).agg(
                        c("min").min(), c("avg").mean(), c("max").max()
                    )
                data = data.with_columns(
                    get_reg_timestamps(date_str=c("date"), time_str=c("time")).alias("timestamp"),
                    c("direction").str.to_lowercase(),
                ).with_columns(
                    pl.lit(value).alias(name) for name, value in market_price_metadata[name]["data"].items()
                ).filter(pl.any_horizontal(~c("max", "avg", "min").is_null()))
                market_price = pl.concat([market_price, data], how="diagonal_relaxed")
    return small_flex_input_schema.add_table(market_price=market_price)

def parse_rte_cap_market_price(
    small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict
    ) -> SmallflexInputSchema:

    name = "rte_cap"
    market_price: pl.DataFrame = pl.read_csv(
        market_price_metadata[name]["file"], separator=";", null_values=["*"])

    market_price = market_price.rename(market_price_metadata[name]["col_mapping"]).with_columns(
        c("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC").dt.truncate(every="4h").alias("timestamp"),
        c(["market", "direction"]).replace_strict(market_price_metadata[name]["value_mapping"], default=None)
    ).drop_nulls(subset=["market", "direction"])\
    .group_by(["timestamp", "direction", "market"]).agg(
        c("avg").min().alias("min"),
        ((c("quantity")*c("avg")).sum()/c("quantity").sum()).alias("avg"),
        c("avg").max().alias("max"),
    ).with_columns(
        pl.lit(value).alias(name) for name, value in market_price_metadata[name]["data"].items()
    )
    return small_flex_input_schema.add_table(market_price=market_price)

def parse_market_price(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]) -> SmallflexInputSchema:

    market_price_metadata: dict = json.load(open(input_file_names["market_price_metadata"]))
    
    kwargs: dict= {"small_flex_input_schema": small_flex_input_schema, "market_price_metadata": market_price_metadata}
    # kwargs["small_flex_input_schema"] = parse_apg_market_price(**kwargs)
    # kwargs["small_flex_input_schema"] = parse_da_ida_market_price(**kwargs)
    kwargs["small_flex_input_schema"] = parse_reg_frr_market_price(**kwargs)
    # kwargs["small_flex_input_schema"] = parse_reg_frc_market_price(**kwargs)
    kwargs["small_flex_input_schema"] = parse_rte_cap_market_price(**kwargs)
    return kwargs["small_flex_input_schema"]