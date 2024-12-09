import patito as pt
import polars as pl
from typing_extensions import Optional, Literal
from data_federation.input_model._constraints import literal_constraint

UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"

class WindPowerPlant(pt.Model):
    name: Optional[str] = pt.Field(dtype=pl.Utf8, description="Power plant name")
    uuid: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant uuid")
    resource_fk_list: list[str] = pt.Field(dtype=pl.List(pl.Utf8), description="List of resource uuid in the power plant")
    rated_power: float = pt.Field(dtype=pl.Float64, description="Rated power of the power plant in MW")