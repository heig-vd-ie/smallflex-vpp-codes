import patito as pt
import polars as pl
from datetime import datetime


from typing_extensions import Optional, Literal

# from data_federation.input_model._constraints import literal_constraint

# RIVER = Literal["Griessee","Chummbach_Restgebiet","Laengtalbach_Altstafel","Greissbach_Altstafel"]
UUID_REG = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"

class DischargeFlowMeasurement(pt.Model):
    basin_fk: Optional[str] = pt.Field(dtype=pl.Utf8, pattern=UUID_REG, description="water basin uuid")
    timestamp: datetime = pt.Field(dtype=pl.Datetime(time_unit="us", time_zone="UTC"), description="Timestamp of the discharge flow")
    location: str = pt.Field(dtype=pl.Utf8)
    value: float = pt.Field(dtype=pl.Float64, description="discharge floe in m^3/s")

