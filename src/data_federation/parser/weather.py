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


NAME_MAPPING: dict[str, str] = { 
    "glob": "irradiation", "hum": "humidity", "prec": "precipitation", 
    "temp": "temperature", "wind": "wind", "ssd": "ssd"
    }

def parse_weather_historical(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], area: str
    ) -> SmallflexInputSchema:
    
    file_type = "meteo.rda"
    weather_historical: pl.DataFrame = pl.DataFrame()

    file_names_list = scan_folder(
        folder_name=input_file_names["wsl_historical_data"], extension=".rda", file_names=file_type)

    for file_name in tqdm.tqdm(file_names_list, desc=f"Parsing weather historical files"):

        names = ro.r['load'](file_name) # type: ignore
        all_df = ro.r[names[0]]
        data: pl.DataFrame = pl.from_pandas(pandas2ri.rpy2py(all_df)) 

        data = data.with_columns(
            pl.from_epoch(c("time")).cast(pl.Datetime(time_zone="UTC")).alias("timestamp")
            # - pl.lit(timedelta(hours=1))).alias("timestamp") # UTC -1 hour because the data is in UTC+1 (without DST)
        ).drop(cs.ends_with("X0")).drop(["time",  "weekday", "yy", "dd", "mm", "hh"])


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
            
        weather_historical = pl.concat([weather_historical, data], how="diagonal_relaxed")

    return small_flex_input_schema.add_table(weather_historical=weather_historical)


def parse_weather_forecast(
    small_flex_input_schema: SmallflexInputSchema, input_file_names: dict[str, str]
    ) -> SmallflexInputSchema:

    weather_metadata = pl.read_csv(input_file_names["wsl_weather_metadata"]).select(
        c("ID").alias("location"),
        c("Average_Elevation").alias("avg_height"),
        c("Sub_Basin").alias("sub_basin"),
    )

    var_type_mapping = {
        "wind": "wind",
        "prec": "precipitation",
        "rad_": "irradiation",
        "hum_": "humidity",
        "temp": "temperature"
    }

    col_to_drop =  ["time", "weekday", "yy", "dd", "mm", "hh"]

    file_type = input_file_names["wsl_weather_forecast"]
    folder_name = input_file_names["wsl_forecast_data"]
    forecast_data: pl.DataFrame = pl.DataFrame()
    measurement_data: pl.DataFrame = pl.DataFrame()

    extract_archive(file_name=input_file_names["wsl_forecast_data"])
    
    file_names_list = scan_folder(folder_name=os.path.splitext(folder_name)[0], extension=".rda", file_names=file_type)


    for file_name in tqdm.tqdm(sorted(file_names_list), desc=f"Parsing weather forecast files"):
        actual_date = datetime.strptime(Path(file_name).parent.name+"T12:00"  , '%Y-%m-%dT%H:%M')
        names = ro.r['load'](file_name) # type: ignore
        all_df = ro.r[names[0]]
        
        day_forecast = pl.DataFrame() 
        for scenario in all_df.names: # type: ignore

            data: pl.DataFrame = pl.from_pandas(pandas2ri.rpy2py(all_df.rx2(scenario))) # type: ignore
            # data = data.with_columns(
            #         pl.from_epoch(c("time")).alias("timestamp"),
            #     ).drop(col_to_drop)
            
            data = data.select(
                pl.from_epoch(c("time")).alias("timestamp") ,
                cs.contains("Gri")
            )
            forecast = data.filter(pl.all_horizontal(c("timestamp") > actual_date))\
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
                

        day_forecast = day_forecast.select(
            (c("timestamp").cast(pl.Datetime(time_zone="UTC")) - pl.lit(timedelta(hours=1))).alias("timestamp"),
            c("location").str.slice(offset=4).alias("location"),
            pl.all().exclude("timestamp", "location").pipe(concat_list_of_list).alias("forecast"),
            c("location").str.slice(offset=0, length=4).replace_strict(var_type_mapping, default=None).alias("var_type"), 
        ).drop_nulls(subset=["var_type"])\
        .pivot(on="var_type", index=["timestamp", "location"], values="forecast") 
        
        forecast_data = pl.concat([forecast_data, day_forecast], how="diagonal_relaxed")
        
        measurement_data = pl.concat([
            measurement_data,
            data.slice(0, 24)\
                .unpivot(
                    on=cs.exclude("timestamp"), index="timestamp", # type: ignore
                    value_name="value", variable_name="location"
                ).with_columns(
                    (c("timestamp").cast(pl.Datetime(time_zone="UTC")) - pl.lit(timedelta(hours=1))).alias("timestamp"),
                    c("location").str.slice(offset=4).alias("location"),
                    c("location").str.slice(offset=0, length=4).replace_strict(var_type_mapping, default=None).alias("var_type"), 
                ).drop_nulls(subset=["var_type"])\
                .pivot(on="var_type", index=["timestamp", "location"], values="value") 
        ], how="diagonal_relaxed")  
        
        
    measurement_data = pl.concat([
            measurement_data,
            data.slice(24).filter(c("timestamp") <= actual_date)\
                .unpivot(
                    on=cs.exclude("timestamp"), index="timestamp", # type: ignore
                    value_name="value", variable_name="location"
                ).with_columns(
                    (c("timestamp").cast(pl.Datetime(time_zone="UTC")) - pl.lit(timedelta(hours=1))).alias("timestamp"),
                    c("location").str.slice(offset=4).alias("location"),
                    c("location").str.slice(offset=0, length=4).replace_strict(var_type_mapping, default=None).alias("var_type"), 
                ).drop_nulls(subset=["var_type"])\
                .pivot(on="var_type", index=["timestamp", "location"], values="value") 
        ], how="diagonal_relaxed")
    
    forecast_data = forecast_data.join(weather_metadata, on="location", how="left").drop_nulls(subset=["sub_basin"])
    measurement_data = measurement_data.join(weather_metadata, on="location", how="left").drop_nulls(subset=["sub_basin"])
    
    table_to_add = {
        "weather_forecast": forecast_data,
        "weather_measurement": measurement_data
    }
    return small_flex_input_schema.add_table(**table_to_add)
