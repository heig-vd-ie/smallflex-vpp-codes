import patito as pt
import polars as pl
from datetime import datetime


class Ssd(pt.Model):
    timestamp: datetime = pt.Field(dtype=pl.Datetime(time_unit="us", time_zone="UTC"), description="Timestamp of the humidity")
    sub_basin: str = pt.Field(dtype=pl.Utf8, description="Sub-basin name")
    start_height: int = pt.Field(dtype=pl.Int32, description="Start height in masl (end_height is 100m higher)")
    value: float = pt.Field(dtype=pl.Float64, description="Ssd value in mm")

