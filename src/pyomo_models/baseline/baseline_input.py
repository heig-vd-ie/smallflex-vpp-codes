from typing import Optional
from datetime import datetime, timedelta, timezone
import polars as pl
from polars import col as c
import pyomo.environ as pyo
import tqdm

from data_federation.input_model import SmallflexInputSchema


class BaseLineInput():
    def __init__(
        self, input_schema_file_name: str, real_timestep: timedelta, year: int, market_country: str = "CH", 
        market: str = "DA", hydro_power_mask: Optional[pl.Expr] = None, max_alpha_error: float = 1.3, 
        volume_factor: float = 1e-6, solver_name: str = 'gurobi'):
        
        self.real_timestep = real_timestep
        self.min_datetime = datetime(year, 1, 1, tzinfo=timezone.utc)
        self.max_datetime = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        self.small_flex_input_schema: SmallflexInputSchema = SmallflexInputSchema()\
            .duckdb_to_schema(file_path=input_schema_file_name)
        self.year = year
        self.market_country = market_country
        self.market = market
        self.hydro_power_mask = hydro_power_mask
        self.max_alpha_error = max_alpha_error  
        self.volume_factor = volume_factor
        self.solver= pyo.SolverFactory(solver_name)
        
        self.discharge_flow_measurement: pl.DataFrame = self.small_flex_input_schema.discharge_flow_measurement\
            .filter(c("river") == "Griessee")\
            .with_columns(
                (c("value") * real_timestep.total_seconds() * volume_factor).alias("discharge_volume"),
                pl.lit(0).alias("B")
            )
            
        self.market_price_measurement:pl.DataFrame = self.small_flex_input_schema.market_price_measurement\
            .filter(c("country") == self.market_country)\
            .filter(c("market") == self.market)
        
        