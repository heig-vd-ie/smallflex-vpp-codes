import patito as pt
import polars as pl
from datetime import datetime

from typing_extensions import Optional

class WeatherHistorical(pt.Model):
    timestamp: datetime = pt.Field(dtype=pl.Datetime(time_unit="us", time_zone="UTC"), description="Timestamp of the humidity")
    sub_basin: str = pt.Field(dtype=pl.Utf8, description="Sub-basin name")
    start_height: int = pt.Field(dtype=pl.Int32, description="Start height in masl (end_height is 100m higher)")
    humidity: Optional[float] = pt.Field(dtype=pl.Float64, description="Humidity value in %")
    irradiation: Optional[float] = pt.Field(dtype=pl.Float64, description="Global irradiation value in W/m^2")
    precipitation: Optional[float] = pt.Field(dtype=pl.Float64, description="Precipitation value in mm/h")
    wind: Optional[float] = pt.Field(dtype=pl.Float64, description="Wind value in m/s")
    temperature: Optional[float] = pt.Field(dtype=pl.Float64, description="Temperature value in ºC")


