import patito as pt
import polars as pl
from typing_extensions import Optional, Literal
from data_federation.input_model._constraints import literal_constraint

UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"
CONTROL = Literal["continuous", "discrete", "uncontrollable"]
TYPE = Literal["buildup_turbine", "buildup_pump_turbine", "run_of_river_turbine"]

class HydroPowerPlant(pt.Model):
    name: Optional[str] = pt.Field(dtype=pl.Utf8, description="Power plant name")
    uuid: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant uuid")
    resource_fk_list: list[str] = pt.Field(dtype=pl.List(pl.Utf8), description="List of resource uuid in the power plant")
    upstream_basin_fk: Optional[str] = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Upstream water basin uuid")
    downstream_basin_fk: Optional[str] = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Downstream water basin uuid")
    rated_power: float = pt.Field(dtype=pl.Float64, description="Rated power of the power plant in MW")
    rated_flow: float = pt.Field(dtype=pl.Float64, description="Rated flow of the power plant in m3/s")
    control: CONTROL = pt.Field(
        dtype=pl.Utf8, constraints=literal_constraint(pt.field, CONTROL),
        description="Control type of the power plant")
    type: TYPE = pt.Field(
        dtype=pl.Utf8, constraints=literal_constraint(pt.field, TYPE),
        description="Type of the power plant")
    