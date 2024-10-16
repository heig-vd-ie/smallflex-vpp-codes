import patito as pt
import polars as pl
from datetime import datetime

from typing_extensions import Optional, Literal

from data_federation.input_model._constraints import literal_constraint

MARKET = Literal["aFRR-cap", "FCR-cap", "mFRR-cap", "mFRR-act", "aFRR-act", 'DA', 'IDA']
DIRECTION = Literal["pos", "sym", "neg"]
COUNTRY = Literal[
    'AT','CH', 'CH-IDA1', 'CH-IDA2', 'DE', 'FR', 'IT-NORD',
    'IT-NORD-MI-A1', 'IT-NORD-MI-A2', 'IT-NORD-MI-A3', 'IT-NORD-MI1',
    'IT-NORD-MI2', 'IT-NORD-MI3', 'IT-NORD-MI4', 'IT-NORD-MI5', 
    'IT-NORD-MI6', 'IT-NORD-MI7']

UNIT = Literal["EUR/MWh", "EUR/MW", "CH/MWh", "CH/MW"]

class MarketPrice(pt.Model):
    timestamp: datetime = pt.Field(dtype=pl.Datetime(time_unit='us', time_zone='UTC'))
    market: MARKET = pt.Field(dtype=pl.Utf8, constraints=literal_constraint(pt.field, MARKET))
    direction: DIRECTION = pt.Field(dtype=pl.Utf8, constraints=literal_constraint(pt.field, DIRECTION))
    country: COUNTRY= pt.Field(dtype=pl.Utf8, constraints=literal_constraint(pt.field, COUNTRY))
    source: Optional[str] = pt.Field(dtype=pl.Utf8)
    unit: UNIT = pt.Field(dtype=pl.Utf8, constraints=literal_constraint(pt.field, UNIT))
    max: Optional[float] = pt.Field(dtype=pl.Float64)
    mean: Optional[float] = pt.Field(dtype=pl.Float64)
    min: Optional[float] = pt.Field(dtype=pl.Float64)
