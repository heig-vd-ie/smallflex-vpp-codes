import patito as pt
import polars as pl

from typing_extensions import Optional, Literal

from data_federation.input_model._constraints import literal_constraint

RESOURCE_TYPE = Literal[
    "hydro_turbine", "photovoltaic", "wind_turbine",
    "energy_storage", "hydro_pump"]

UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"

class Resource(pt.Model):
    name: Optional[str] = pt.Field(dtype=pl.Utf8, description="Resource name")
    uuid: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Resource uuid")
    type: RESOURCE_TYPE = pt.Field(
        dtype=pl.Utf8, constraints=literal_constraint(pt.field, RESOURCE_TYPE), description="Resource type")
    power_plant_fk: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant uuid that use this resource")
    rated_power: float = pt.Field(dtype=pl.Float64, description="Rated power of the resource in MW")
    installed: bool = pt.Field(dtype=pl.Boolean, default=True, description="Resource installation status")