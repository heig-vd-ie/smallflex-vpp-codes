import patito as pt
import polars as pl
from datetime import datetime

from typing_extensions import Optional, Literal

from data_federation.input_model._constraints import literal_constraint

MARKET = Literal["FCR-cap","aFRR-cap", "aFRR-act", "mFRR-cap", "mFRR-act", 'DA', 'IDA']
DIRECTION = Literal["pos", "sym", "neg"]
COUNTRY = Literal[
    'AT','CH','DE', 'FR', 'NL', 'DK', 'BE', 'CH-IDA1', 'CH-IDA2', 'IT-NORD',
    'IT-NORD-MI-A1', 'IT-NORD-MI-A2', 'IT-NORD-MI-A3', 'IT-NORD-MI1',
    'IT-NORD-MI2', 'IT-NORD-MI3', 'IT-NORD-MI4', 'IT-NORD-MI5', 
    'IT-NORD-MI6', 'IT-NORD-MI7']

UNIT = Literal["EUR/MWh", "EUR/MW", "CH/MWh", "CH/MW"]

class MarketPriceMeasurement(pt.Model):
    timestamp: datetime = pt.Field(dtype=pl.Datetime(time_unit='us', time_zone='UTC'), description="Timestamp of the market price")
    market: MARKET = pt.Field(
        dtype=pl.Utf8, constraints=literal_constraint(pt.field, MARKET), description="Market type")
    direction: DIRECTION = pt.Field(
        dtype=pl.Utf8, constraints=literal_constraint(pt.field, DIRECTION), description="Direction of the market price")
    country: COUNTRY= pt.Field(
        dtype=pl.Utf8, constraints=literal_constraint(pt.field, COUNTRY), description="Country of the market price")
    source: Optional[str] = pt.Field(dtype=pl.Utf8, description="Source of the values")
    unit: UNIT = pt.Field(dtype=pl.Utf8, constraints=literal_constraint(pt.field, UNIT), description="Unit of the market price")
    max: Optional[float] = pt.Field(dtype=pl.Float64, description="Maximum value of the market price")
    avg: Optional[float] = pt.Field(dtype=pl.Float64, description="Average value of the market price")
    min: Optional[float] = pt.Field(dtype=pl.Float64, description="Minimum value of the market price")
