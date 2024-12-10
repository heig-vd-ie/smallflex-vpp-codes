from typing import Optional
from datetime import datetime, timedelta, timezone
import polars as pl
from polars import col as c
import pyomo.environ as pyo
import tqdm

from data_federation.input_model import SmallflexInputSchema
from pyomo_models.input_data_preprocessing import (
    generate_baseline_index, generate_clean_timeseries, generate_water_flow_factor, generate_basin_volume_table,
    clean_hydro_power_performance_table, generate_hydro_power_state
)
from utility.general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log

from pyomo_models.baseline.first_stage.sets import baseline_sets
from pyomo_models.baseline.first_stage.parameters import baseline_parameters
from pyomo_models.baseline.first_stage.variables import baseline_variables
from pyomo_models.baseline.first_stage.objective import baseline_objective
from pyomo_models.baseline.first_stage.constraints.basin_volume import basin_volume_constraints
from pyomo_models.baseline.first_stage.constraints.turbine import turbine_constraints
from pyomo_models.baseline.first_stage.constraints.pump import pump_constraints

class BaseLineInput():
    def __init__(
        self, input_schema_file_name: str, real_timestep: timedelta, year: int, market_country: str = "CH", 
        market: str = "DA", hydro_power_mask: Optional[pl.Expr] = None, max_alpha_error: float = 1.3, 
        volume_factor: float = 1e-3, solver_name: str = 'gurobi'):
        
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
        
        