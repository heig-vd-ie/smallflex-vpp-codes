from curses import def_shell_mode
import patito as pt
import polars as pl

from traitlets import default
from typing_extensions import Optional

UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"

class WaterBasin(pt.Model):
    name: Optional[str] = pt.Field(dtype=pl.Utf8, description="Water basin name")
    uuid: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Water basin uuid")
    power_plant_fk: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Power plant uuid that use this water basin")
    volume_max: float = pt.Field(dtype=pl.Float64, default=0.0, description="Water basin maximum volume in m3")
    volume_min: Optional[float] = pt.Field(dtype=pl.Float64, description="Water basin minimum volume in m3")
    height_max: float = pt.Field(dtype=pl.Float64, description="Water basin maximum height in masl") 
    height_min: Optional[float] = pt.Field(dtype=pl.Float64, description="Water basin minimum height in masl")
    n_state_min: int = pt.Field(dtype=pl.Int32, default=1, description="Water basin minimum height in masl")
