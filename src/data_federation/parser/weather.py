import os
import polars as pl
from polars import col as c
import polars.selectors as cs
from datetime import timedelta

from data_federation.input_model import SmallflexInputSchema


NAME_MAPPING: dict[str, str] = { 
    "irradiation": "glob_", "humidity": "hum_", 
    "precipitation": "prec_", "temperature": "temp_",
    "wind": "wind_", "ssd": "ssd"
    }

def parse_weather(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], area: str
    ) -> SmallflexInputSchema:
    
    cleaned_data_dict = {
        "irradiation": pl.DataFrame(), "humidity": pl.DataFrame(), 
        "precipitation": pl.DataFrame(), "temperature": pl.DataFrame(),
        "wind": pl.DataFrame(), "ssd": pl.DataFrame()
    }

    for entry in list(os.scandir(input_file_names["greis_wsl_data"])):
        if entry.name.endswith("_meteo.csv"):
            col_list = ["index"] + list(
                pl.read_csv(entry.path, separator=",", n_rows=1, has_header=False, truncate_ragged_lines=True).row(0))

            data: pl.DataFrame = pl.read_csv(
                entry.path, separator=",", skip_rows=1, has_header=False, new_columns=col_list)
            
            data = data.with_columns(
                    (c("time").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC") - pl.lit(timedelta(hours=1)))
                    .alias("timestamp"),
                ).drop(cs.ends_with("X0"))

            for table, column_name in NAME_MAPPING.items():
                
                cleaned_data_dict[table] = pl.concat([
                    cleaned_data_dict[table], 
                    data.unpivot(
                        index=["timestamp"], on=cs.starts_with(column_name),
                        variable_name="metadata", value_name="value", 
                    ).with_columns(
                        (area + "_" + c("metadata").str.slice(-3, 1)).alias("sub_basin"),
                        ((c("metadata").str.slice(-2) + "00").cast(pl.Int32)-50).alias("start_height")
                    )],  how="diagonal_relaxed"
                )

    return small_flex_input_schema.add_table(**cleaned_data_dict)