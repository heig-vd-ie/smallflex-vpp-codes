import patito as pt
import polars as pl
from datetime import datetime


from typing_extensions import Optional, Literal

# from data_federation.input_model._constraints import literal_constraint

# RIVER = Literal["Griessee","Chummbach_Restgebiet","Laengtalbach_Altstafel","Greissbach_Altstafel"]
UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"

class WeatherForecast(pt.Model):
    timestamp: datetime = pt.Field(dtype=pl.Datetime(time_unit="us", time_zone="UTC"), description="Timestamp of the humidity")
    sub_basin: str = pt.Field(dtype=pl.Utf8, description="Sub-basin name")
    avg_height: int = pt.Field(dtype=pl.Int32, description="Start height in masl (end_height is 100m higher)")
    humidity: list[list[float]] = pt.Field(dtype=pl.List(pl.List(pl.Float64)), description="Humidity value in %")
    irradiation: list[list[float]] = pt.Field(dtype=pl.List(pl.List(pl.Float64)), description="Global irradiation value in W/m^2")
    precipitation: list[list[float]] = pt.Field(dtype=pl.List(pl.List(pl.Float64)), description="Precipitation value in mm/h")
    wind: list[list[float]] = pt.Field(dtype=pl.List(pl.List(pl.Float64)), description="Wind value in m/s")
    temperature: list[list[float]]= pt.Field(dtype=pl.List(pl.List(pl.Float64)), description="Temperature value in ºC")
