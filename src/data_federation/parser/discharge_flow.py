import os
import polars as pl
from polars import col as c
import polars.selectors as cs
from datetime import timedelta
import tqdm
from datetime import datetime
from pathlib import Path 
from rpy2.robjects import pandas2ri
import rpy2.robjects as ro 

from data_federation.input_model import SmallflexInputSchema

from polars_function import concat_list_of_list
from general_function import dictionary_key_filtering, pl_to_dict, generate_log, extract_archive, scan_folder

def parse_discharge_flow_historical(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]
    ) -> SmallflexInputSchema:
    
    file_type = "hydro.rda"
    discharge_flow_historical: pl.DataFrame = pl.DataFrame()
    location_fk_mapping: dict = pl_to_dict(small_flex_input_schema.water_basin[["name", "uuid"]])
    
    file_names_list = scan_folder(
        folder_name=input_file_names["wsl_historical_data"], extension=".rda", file_names=file_type)
    
    for file_name in tqdm.tqdm(file_names_list, desc=f"Parsing discharge flow historical files"):
        names = ro.r['load'](file_name) # type: ignore
        all_df = ro.r[names[0]]
        data: pl.DataFrame = pl.from_pandas(pandas2ri.rpy2py(all_df)) 

        data = data.with_columns(
            (pl.from_epoch(c("time")).cast(pl.Datetime(time_zone="UTC")) 
            - pl.lit(timedelta(hours=1))).alias("timestamp") # UTC -1 hour because the data is in UTC+1 (without DST)
        ).unpivot(
            index=["timestamp"], variable_name="location", value_name="value", on=cs.starts_with("discharge_")
        ).with_columns(
            c("location").str.replace("discharge_", "")
        ).with_columns(
            c("location").replace_strict(location_fk_mapping, default=None).alias("basin_fk")
        )
        discharge_flow_historical = pl.concat([discharge_flow_historical, data], how="diagonal_relaxed")

    return small_flex_input_schema.add_table(discharge_flow_historical=discharge_flow_historical)


def parse_discharge_flow_forecast(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]
    ) -> SmallflexInputSchema:
    
    location_mapping: dict[str, str] = {
        'Gle100_5': "Gletsch",
        'Gri200_5': "Griessee_not_used", 
        'Gri200_6': "Griessee_below_dam", 
        'Gri200_7': "Griessee", 
        'Gri200_8': "Griessee_Lengtalstafel",
        'SF1200_5': "Wanneboden",
        'SF1200_6': "Merezenbach"
    }
    location_fk_mapping: dict = pl_to_dict(small_flex_input_schema.water_basin[["name", "uuid"]])
    
    col_to_drop =  ["time", "weekday", "yy", "dd", "mm", "hh"]
    file_type = input_file_names["wsl_discharge_flow_forecast"]
    folder_name = input_file_names["wsl_forecast_data"]
    forecast_data: pl.DataFrame = pl.DataFrame()
    measurement_data: pl.DataFrame = pl.DataFrame()
    extract_archive(file_name=input_file_names["wsl_forecast_data"])
    file_names_list = scan_folder(folder_name=os.path.splitext(folder_name)[0], extension=".rda", file_names=file_type)
    
    for file_name in tqdm.tqdm(sorted(file_names_list), desc=f"Parsing discharge flow forecast files"):
        actual_date = datetime.strptime(Path(file_name).parent.name+"T12:00"  , '%Y-%m-%dT%H:%M')
        names = ro.r['load'](file_name) # type: ignore
        all_df = ro.r[names[0]]

        day_forecast = pl.DataFrame() 
        for scenario in all_df.names: # type: ignore
            data: pl.DataFrame = pl.from_pandas(pandas2ri.rpy2py(all_df.rx2(scenario))) # type: ignore
            data = data.with_columns(
                    pl.from_epoch(c("time")).alias("timestamp"),
                ).drop(col_to_drop)
            
            forecast = data.filter(c("timestamp") > actual_date)\
                .sort("timestamp")\
                .with_columns(
                    pl.lit(actual_date).alias("timestamp")
                ).group_by("timestamp", maintain_order=True)\
                .agg(pl.all().exclude("timestamp"))
            
            forecast = forecast.unpivot(
                on=cs.exclude("timestamp"), index="timestamp", # type: ignore
                value_name="value", variable_name="location"
            ).with_columns(
                pl.lit(scenario).alias("scenario")
            )
            day_forecast= pl.concat([day_forecast, forecast], how="diagonal_relaxed")
                
        day_forecast = day_forecast\
            .pivot(on="scenario", index=["timestamp", "location"], values="value")   
            
        forecast_data = pl.concat([forecast_data, day_forecast], how="diagonal_relaxed")
        
        measurement_data = pl.concat([
            measurement_data,
            data.slice(0, 24)
            .unpivot(
                on=cs.exclude("timestamp"), index="timestamp", # type: ignore
                value_name="value", variable_name="location"
            )
        ], how="diagonal_relaxed")

    measurement_data = pl.concat([
        measurement_data,
        data.slice(24).filter(c("timestamp") < actual_date)
        .unpivot(
            on=cs.exclude("timestamp"), index="timestamp", # type: ignore
            value_name="value", variable_name="location"
        )
    ], how="diagonal_relaxed")

    forecast_data = forecast_data.select(
        (c("timestamp").cast(pl.Datetime(time_zone="UTC")) - pl.lit(timedelta(hours=1))).alias("timestamp"),
        c("location").replace_strict(location_mapping, default=None).alias("location"),
        pl.all().exclude("timestamp", "location").pipe(concat_list_of_list).alias("forecast")
    ).with_columns(
        c("location").replace_strict(location_fk_mapping, default=None).alias("basin_fk")
    )

    measurement_data = measurement_data\
        .with_columns(
            (c("timestamp").cast(pl.Datetime(time_zone="UTC")) - pl.lit(timedelta(hours=1))).alias("timestamp"),
            c("location").replace_strict(location_mapping, default=None).alias("location"),
        ).with_columns(
            c("location").replace_strict(location_fk_mapping, default=None).alias("basin_fk")
        )
    table_to_add = {
        "discharge_flow_forecast": forecast_data,
        "discharge_flow_measurement": measurement_data
    }
    return small_flex_input_schema.add_table(**table_to_add)
