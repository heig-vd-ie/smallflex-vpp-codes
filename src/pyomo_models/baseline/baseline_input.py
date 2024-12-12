from typing import Optional
from datetime import datetime, timedelta, timezone
import polars as pl
from polars import col as c
import pyomo.environ as pyo
import tqdm

from data_federation.input_model import SmallflexInputSchema
from pyomo_models.input_data_preprocessing import (
    generate_water_flow_factor, generate_basin_volume_table, clean_hydro_power_performance_table,
)

class BaseLineInput():
    def __init__(
        self, input_schema_file_name: str, real_timestep: timedelta, year: int, market_country: str = "CH", 
        market: str = "DA", hydro_power_mask: Optional[pl.Expr] = None, max_alpha_error: float = 1.3, 
        volume_factor: float = 1e-6, solver_name: str = 'gurobi'):
        
        if hydro_power_mask is None:
            hydro_power_mask = pl.lit(True)
        
        self.real_timestep = real_timestep
        self.min_datetime = datetime(year, 1, 1, tzinfo=timezone.utc)
        self.max_datetime = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        
        self.year = year
        self.market_country = market_country
        self.market = market
        
        self.hydro_power_mask = hydro_power_mask
        self.max_alpha_error = max_alpha_error  
        self.volume_factor = volume_factor
        self.solver= pyo.SolverFactory(solver_name)
        self.discharge_flow_measurement: pl.DataFrame = pl.DataFrame()
        self.market_price_measurement: pl.DataFrame = pl.DataFrame()
        self.power_performance_table: list[dict] = []
        self.water_flow_factor: pl.DataFrame = pl.DataFrame()
        self.index: dict[str, pl.DataFrame] = {}
        self.build_input_data(input_schema_file_name)
        
    def build_input_data(self, input_schema_file_name):
        small_flex_input_schema: SmallflexInputSchema = SmallflexInputSchema()\
            .duckdb_to_schema(file_path=input_schema_file_name)
        
        self.discharge_flow_measurement = small_flex_input_schema.discharge_flow_measurement\
            .filter(c("river") == "Griessee")\
            .with_columns(
                (c("value") * self.real_timestep.total_seconds() * self.volume_factor).alias("discharge_volume"),
                pl.lit(0).alias("B")
            )
            
        self.market_price_measurement =small_flex_input_schema.market_price_measurement\
            .filter(c("country") == self.market_country)\
            .filter(c("market") == self.market)
        
        self.index["hydro_power_plant"] = small_flex_input_schema.hydro_power_plant\
            .filter(self.hydro_power_mask).with_row_index(name="H")
            
        self.index["water_basin"] = small_flex_input_schema.water_basin\
                .filter(c("power_plant_fk").is_in(self.index["hydro_power_plant"]["uuid"]))\
                .with_columns(
                    c("volume_max", "volume_min", "start_volume")*self.volume_factor
                ).with_row_index(name="B")
                
        self.water_flow_factor = generate_water_flow_factor(index=self.index)
        basin_volume_table: dict[int, Optional[pl.DataFrame]] = generate_basin_volume_table(
        small_flex_input_schema=small_flex_input_schema, index=self.index, volume_factor=self.volume_factor)

        self.power_performance_table = clean_hydro_power_performance_table(
            small_flex_input_schema=small_flex_input_schema, index=self.index, 
            basin_volume_table=basin_volume_table)

        