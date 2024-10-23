import patito as pt
import polars as pl
from datetime import datetime


from typing_extensions import Optional, Literal

from data_federation.input_model._constraints import literal_constraint

RIVER = Literal["Griessee","Chummbach_Restgebiet","Laengtalbach_Altstafel","Greissbach_Altstafel"]

class DischargeFlow(pt.Model):
    timestamp: datetime = pt.Field(dtype=pl.Datetime(time_unit="us", time_zone="UTC"), description="Timestamp of the discharge flow")
    river: RIVER = pt.Field(dtype=pl.Utf8, constraints=literal_constraint(pt.field, RIVER), description="River name")
    value: float = pt.Field(dtype=pl.Float64, description="discharge floe in m^3/s")

