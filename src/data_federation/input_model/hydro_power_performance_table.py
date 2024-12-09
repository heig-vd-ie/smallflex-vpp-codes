import patito as pt
import polars as pl
from typing_extensions import Optional

UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"

class HydroPowerPerformanceTable(pt.Model):
    power_plant_fk: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant uuid")
    power_plant_state_fk: Optional[str] = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant state uuid")
    state_number: Optional[int] = pt.Field(dtype=pl.Int32, description="Power plant state number")
    head: float = pt.Field(dtype=pl.Float64, description="Head value of the waterfall in meters")
    flow: float = pt.Field(dtype=pl.Float64, default = 0.0, description="Flow value of the waterfall in m3/s")
    electrical_power: float = pt.Field(
        dtype=pl.Float64, default=0.0, description="Electrical power generate or consumed after the transformer in MW")
    efficiency: float = pt.Field(
        dtype=pl.Float64, default=0.0,
        description="Overall efficiency of the power plant including all losses")
    