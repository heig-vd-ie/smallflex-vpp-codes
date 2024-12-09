import patito as pt
import polars as pl

UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"

class BasinHeightVolumeTable(pt.Model):
    water_basin_fk: str = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="Water basin uuid that use this table")
    height: float = pt.Field(dtype=pl.Float64, description="Water basin height in masl")
    volume: float = pt.Field(dtype=pl.Float64, default=0.0, description="Water basin volume in m3")
