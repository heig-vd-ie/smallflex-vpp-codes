
import patito as pt
import polars as pl
from datetime import datetime

from typing_extensions import Optional


UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"


class PowerProductionMeasurement(pt.Model):
    timestamp: datetime = pt.Field(dtype=pl.Datetime(time_unit="us", time_zone="UTC"), description="Timestamp of the humidity")
    power_plant_fk: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant uuid")
    resource_fk: Optional[str] = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant uuid")
    min_current: Optional[float] = pt.Field(dtype=pl.Float64, description="Min current in A")
    avg_current: Optional[float] = pt.Field(dtype=pl.Float64, description="Average current in A")
    max_current: Optional[float] = pt.Field(dtype=pl.Float64, description="Max current in A")
    min_voltage: Optional[float] = pt.Field(dtype=pl.Float64, description="Min voltage in kV")
    avg_voltage: Optional[float] = pt.Field(dtype=pl.Float64, description="Average voltage in kV")
    max_voltage: Optional[float] = pt.Field(dtype=pl.Float64, description="Max voltage in kV")
    min_active_power: Optional[float] = pt.Field(dtype=pl.Float64, description="Min active power in MW")
    avg_active_power: Optional[float] = pt.Field(dtype=pl.Float64, description="Average active power in MW")
    max_active_power: Optional[float] = pt.Field(dtype=pl.Float64, description="Max active power in MW")
    min_reactive_power: Optional[float] = pt.Field(dtype=pl.Float64, description="Min reactive power in MVAr")
    avg_reactive_power: Optional[float] = pt.Field(dtype=pl.Float64, description="Average reactive power in MVAr")
    max_reactive_power: Optional[float] = pt.Field(dtype=pl.Float64, description="Max reactive power in MVAr")
