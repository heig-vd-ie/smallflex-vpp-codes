import os
import polars as pl
from polars import col as c
import polars.selectors as cs
from datetime import timedelta

from data_federation.input_model import SmallflexInputSchema


NAME_MAPPING: dict[str, str] = { 
    "glob": "irradiation", "hum": "humidity", "prec": "precipitation", 
    "temp": "temperature", "wind": "wind", "ssd": "ssd"
    }

def parse_weather(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], area: str
    ) -> SmallflexInputSchema:
    
    weather_measurement: pl.DataFrame = pl.DataFrame()

    for entry in list(os.scandir(input_file_names["greis_wsl_data"])):
        if entry.name.endswith("_meteo.csv"):
            col_list = ["index"] + list(
                pl.read_csv(entry.path, separator=",", n_rows=1, has_header=False, truncate_ragged_lines=True).row(0))

            data: pl.DataFrame = pl.read_csv(
                entry.path, separator=",", skip_rows=1, has_header=False, new_columns=col_list)
            
            data = data.with_columns(
                    (c("time").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC") - pl.lit(timedelta(hours=1)))
                    .alias("timestamp"),
                ).drop(cs.ends_with("X0")).drop(["index", "time",  "weekday", "yy", "dd", "mm", "hh"])


            data = data.unpivot(
                    index=["timestamp"], on= cs.exclude("timestamp"), # type: ignore
                    variable_name="metadata", value_name="value", 
                ).with_columns(
                    c("metadata").str.replace("__", "_").str.split_exact("_X", 1).struct.rename_fields(["type", "location"]),
                ).unnest("metadata").with_columns(
                    (area + "_" + c("location").str.slice(0, 1)).alias("sub_basin"),
                    ((c("location").str.slice(1, 2) + "00").cast(pl.Int32)-50).alias("start_height"),
                ).pivot(on="type", values="value", index=["start_height", "sub_basin", "timestamp"])\
                .rename(NAME_MAPPING)
                
            weather_measurement = pl.concat([weather_measurement, data], how="diagonal_relaxed")

    return small_flex_input_schema.add_table(weather_measurement=weather_measurement)