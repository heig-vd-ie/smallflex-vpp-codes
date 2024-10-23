import uuid
import patito as pt
import polars as pl


UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"

class PowerPlantState(pt.Model):
    uuid: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant state uuid")
    state_number: int = pt.Field(dtype=pl.Int32, description="State number")
    power_plant_fk: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant uuid")
    resource_state_list: list[bool] = pt.Field(dtype=pl.List(pl.Boolean), description="List of resource activation state")