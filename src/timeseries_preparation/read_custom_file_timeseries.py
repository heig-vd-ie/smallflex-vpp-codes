import os
import polars as pl
from polars import col as c

def read_and_validate_custom_file(file_name: str) -> pl.DataFrame:
    if not os.path.isfile(file_name):
        raise FileNotFoundError(f"Market price file {file_name} not found")

    data = pl.read_csv(file_name)

    if set(data.columns) != {"timestamp", "da", "fcr"}:
        raise ValueError(f"Market price file {file_name} does not contain the required columns 'timestamp', 'da', 'fcr'")

    if data.filter(pl.col("timestamp").is_duplicated()).height > 0:
        raise ValueError(f"Market price file {file_name} contains duplicate timestamps")

    try: 
        data = data.with_columns(
            c("timestamp").str.strptime(dtype= pl.Datetime(time_zone="UTC"), format="%Y-%m-%d %H:%M:%S")
        )
    except:
        raise ValueError(f"Market price file contains invalid timestamp format, expected YYYY-MM-DD HH:MM:SS")

    try: 
        data = data.with_columns(
            c("da", "fcr").cast(pl.Float64)
        )
    except:
        raise ValueError(f"Market price file contains invalid price format, expected float values")

    if data.filter(c("timestamp").dt.year().is_first_distinct()).height != 1:
        raise ValueError(f"Market price file {file_name} does not cover a full year with hourly data")

    year = data.select(c("timestamp").dt.year())["timestamp"][0]

    da_data = pl.datetime_range(
        start=pl.datetime(year, 1, 1, time_zone="UTC"),
        end=pl.datetime(year+1, 1, 1, time_zone="UTC"),
        closed="left",
        interval="1h",
        eager=True, 
    ).rename("timestamp").to_frame().join(data, on="timestamp", how="left")

    if da_data.filter(c("da").is_null()).height > 0:
        raise ValueError(f"Market price file contains missing DA prices")

    fcr_data = pl.datetime_range(
        start=pl.datetime(year, 1, 1, time_zone="UTC"),
        end=pl.datetime(year+1, 1, 1, time_zone="UTC"),
        closed="left",
        interval="4h",
        eager=True, 
    ).rename("timestamp").to_frame().join(data, on="timestamp", how="left")

    if fcr_data.filter(c("fcr").is_null()).height > 0:
        raise ValueError(f"Market price file contains missing FCR prices")

    return data