import os
import polars as pl
from polars import col as c
import polars.selectors as cs
from datetime import timedelta

from data_federation.input_model import SmallflexInputSchema
from utility.general_function import dictionary_key_filtering

def parse_discharge_flow(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]
    ) -> SmallflexInputSchema:
    
    discharge_flow_measurement: pl.DataFrame = pl.DataFrame()
    for entry in list(os.scandir(input_file_names["greis_wsl_data"])):
        if entry.name.endswith("_hydro.csv"):

            col_list = ["index"] + list(pl.read_csv(
                entry.path, separator=",", n_rows=1, has_header=False, truncate_ragged_lines=True).row(0))

            data: pl.DataFrame = pl.read_csv(
                entry.path, separator=",", skip_rows=1, has_header=False, new_columns=col_list)

            data = data.with_columns(
                (c("time").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC") - pl.lit(timedelta(hours=1))).alias("timestamp"),
            ).unpivot(
                index=["timestamp"], variable_name="river", value_name="value", on=cs.starts_with("discharge_")
            ).with_columns(
                c("river").str.replace("discharge_", "")
            )
            discharge_flow_measurement = pl.concat([discharge_flow_measurement, data], how="diagonal_relaxed")

    return small_flex_input_schema.add_table(discharge_flow_measurement=discharge_flow_measurement)