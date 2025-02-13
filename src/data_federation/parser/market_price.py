import json
from config import settings
import os
import polars as pl
from polars import col as c
import polars.selectors as cs
import tqdm
from data_federation.input_model import SmallflexInputSchema
from general_function import dictionary_key_filtering

def parse_apg_market_price(
    small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    
    market_price_measurement: pl.DataFrame = pl.DataFrame()
    
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
            
            market_price_measurement = pl.concat([market_price_measurement, data], how="diagonal_relaxed")
        
    return small_flex_input_schema.add_table(market_price_measurement=market_price_measurement)

def parse_da_ida_market_price(small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    market_price_measurement: pl.DataFrame = pl.DataFrame()
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
        market_price_measurement = pl.concat([market_price_measurement, data], how="diagonal_relaxed")
    return small_flex_input_schema.add_table(market_price_measurement=market_price_measurement)

def get_reg_timestamps(date_str: pl.Expr, time_str: pl.Expr):
    try:
        date: pl.Expr = date_str.str.to_datetime(format="%Y-%m-%d %H:%M:%S", time_zone="UTC")
    except:
        date: pl.Expr = date_str.cast(pl.Datetime(time_zone="UTC"))
    time: pl.Expr = (time_str.cast(pl.Int32)*1e3*60*60).cast(pl.Duration(time_unit="ms")) # from ms to min
    return date + time

def parse_reg_frc_market_price(small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    market_price_measurement: pl.DataFrame = pl.DataFrame()
    name = "reg_fcr"
    for entry in list(os.scandir(market_price_metadata[name]["folder"])):
        if entry.name.startswith("RESULT_OVERVIEW"):
            data: pl.DataFrame = pl.read_excel(entry.path, infer_schema_length=0)
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
            market_price_measurement = pl.concat([market_price_measurement, data], how="diagonal_relaxed")
            
    return small_flex_input_schema.add_table(market_price_measurement=market_price_measurement)

def parse_reg_frr_market_price(small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict) -> SmallflexInputSchema:
    market_price_measurement: pl.DataFrame = pl.DataFrame()
    for name in  ["reg_afrr_ene", "reg_afrr_cap", "reg_mfrr_ene", "reg_mfrr_cap"]:
        for entry in list(os.scandir(market_price_metadata[name]["folder"])):
            if entry.name.startswith("RESULT_OVERVIEW"):
                data: pl.DataFrame = pl.read_excel(entry.path, infer_schema_length=0)
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
                market_price_measurement = pl.concat([market_price_measurement, data], how="diagonal_relaxed")
    return small_flex_input_schema.add_table(market_price_measurement=market_price_measurement)

def parse_rte_cap_market_price(
    small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict
    ) -> SmallflexInputSchema:

    name = "rte_cap"
    market_price_measurement: pl.DataFrame = pl.read_csv(
        market_price_metadata[name]["file"], separator=";", null_values=["*"])

    market_price_measurement = market_price_measurement.rename(market_price_metadata[name]["col_mapping"]).with_columns(
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
    return small_flex_input_schema.add_table(market_price_measurement=market_price_measurement)

def parse_rte_ene_market_price(
    small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict
    ) -> SmallflexInputSchema:

    metadata:dict = market_price_metadata["rte_ene"]

    market_price_measurement: pl.DataFrame = pl.read_csv(metadata["file"], separator=";")\
        .rename(metadata["col_mapping"])[list(metadata["col_mapping"].values())]\
        .with_columns(
            c("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC").alias("timestamp")
        ).unpivot(
            index="timestamp", on = cs.exclude("timestamp"),  # type: ignore
            value_name="avg", variable_name="market"
        ).with_columns(
            c("market").str.replace("-", "-act_").str.split_exact("_", n=1)
            .struct.rename_fields(["market", "direction"]),
        ).unnest("market").with_columns(
            pl.lit(value).alias(name) for name, value in metadata["data"].items()
        ).drop_nulls("avg")
        
    return small_flex_input_schema.add_table(market_price_measurement=market_price_measurement)

def parse_swissgrid_cap_market_price(
    small_flex_input_schema:SmallflexInputSchema, market_price_metadata: dict
    ) -> SmallflexInputSchema:
    
    metadata = market_price_metadata["swissgrid_cap"]
    
    market_price_measurement: pl.DataFrame = pl.DataFrame()

    for entry in list(os.scandir(metadata["folder"])):

        data: pl.DataFrame = pl.read_csv(entry.path, separator=";", encoding='iso-8859-1', null_values=["*", "N/A"])
        # We have removed weekly-based market prices
        data = data.rename(metadata["col_mapping"])\
            .filter(pl.col("quantity") > 0)\
            .filter(~ pl.col("metadata").str.contains("KW"))
            
        data = data.with_columns(
            pl.col("time").str.split(" bis ").list.get(0).str.split(" ").list.get(-1).alias("time"),
            pl.col("metadata").str.split_exact("_", 3).struct.rename_fields(["market", "y", "m", "d"]),
        ).unnest("metadata")\
        .with_columns(
            pl.concat_str(["y", "m", "d", "time"], separator="-")
            .str.to_datetime("%y-%m-%d-%H:%M", time_zone="UTC", strict=False).alias("timestamp")
        ).drop_nulls("timestamp")
        
        data = data.group_by(["timestamp", "market"]).agg(
            c("price").min().alias("min"),
            ((c("price") *c("quantity")).sum()/c("quantity").sum()).alias("avg"),
            c("price").max().alias("max"),
        ).with_columns(
            c("market").replace_strict(metadata["market_mapping"])
        ).with_columns(
            pl.lit(value).alias(name) for name, value in metadata["data"].items()
        ).unnest("market")
        
        market_price_measurement = pl.concat([market_price_measurement, data], how="diagonal_relaxed")
    return small_flex_input_schema.add_table(market_price_measurement=market_price_measurement)


def parse_market_price(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]) -> SmallflexInputSchema:

    market_price_metadata: dict = json.load(open(input_file_names["market_price_metadata"]))
    
    kwargs: dict= {"small_flex_input_schema": small_flex_input_schema, "market_price_metadata": market_price_metadata}
    with tqdm.tqdm(total=7, desc="Parse market price input data") as pbar:
        kwargs["small_flex_input_schema"] = parse_apg_market_price(**kwargs)
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_da_ida_market_price(**kwargs)
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_reg_frr_market_price(**kwargs)
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_reg_frc_market_price(**kwargs)
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_rte_cap_market_price(**kwargs)
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_rte_ene_market_price(**kwargs)
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_swissgrid_cap_market_price(**kwargs)    
        pbar.update()                                                    
    return kwargs["small_flex_input_schema"]