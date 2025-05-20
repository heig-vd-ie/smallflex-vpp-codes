from typing import Optional
from datetime import datetime, timedelta, timezone
import polars as pl
from polars import col as c
import pyomo.environ as pyo

from utility.input_data_preprocessing import (
    generate_water_flow_factor, generate_basin_volume_table, clean_hydro_power_performance_table
)

from general_function import pl_to_dict, duckdb_to_dict

class BaseLineInput():
    def __init__(
        self, input_schema_file_name: str, real_timestep: timedelta, year: int, market_country: str = "CH",
        market: str = "DA", hydro_power_mask: pl.Expr = pl.lit(True), max_alpha_error: float = 1.3,
        volume_factor: float = 1e-6, solver_name: str = 'gurobi'):
        
        if hydro_power_mask is None:
            hydro_power_mask = pl.lit(True)
        
        self.real_timestep: timedelta = real_timestep
        self.min_datetime: datetime = datetime(year, 1, 1, tzinfo=timezone.utc)
        self.max_datetime: datetime = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        
        self.year:int = year
        self.market_country: str = market_country
        self.market: str = market
        
        self.hydro_power_mask: pl.Expr = hydro_power_mask
        self.max_alpha_error: float = max_alpha_error
        self.volume_factor: float = volume_factor
        self.solver= pyo.SolverFactory(solver_name)
        self.discharge_flow_measurement: pl.DataFrame = pl.DataFrame()
        self.market_price_measurement: pl.DataFrame = pl.DataFrame()
        self.power_performance_table: list[dict] = []
        self.water_flow_factor: pl.DataFrame = pl.DataFrame()
        self.index: dict[str, pl.DataFrame] = {}
        self.build_input_data(input_schema_file_name)
        
    def build_input_data(self, input_schema_file_name):
        schema_dict: dict[str, pl.DataFrame] = duckdb_to_dict(file_path=input_schema_file_name)
        
        self.market_price_measurement = schema_dict["market_price_measurement"]\
            .filter(c("country") == self.market_country)\
            .filter(c("market") == self.market)
        
        self.index["hydro_power_plant"] = schema_dict["hydro_power_plant"]\
            .filter(self.hydro_power_mask).with_row_index(name="H")
            
        self.index["water_basin"] = schema_dict["water_basin"]\
                .filter(c("power_plant_fk").is_in(self.index["hydro_power_plant"]["uuid"]))\
                .with_columns(
                    c("volume_max", "volume_min", "start_volume")*self.volume_factor
                ).with_row_index(name="B")
        
        basin_index_mapping = pl_to_dict(self.index["water_basin"][["uuid", "B"]])
        
        self.discharge_flow_measurement = schema_dict["discharge_flow_historical"]\
            .with_columns(
                c("basin_fk").replace_strict(basin_index_mapping, default=None).alias("B")
            ).drop_nulls(subset="basin_fk").with_columns(
                (c("value") * self.real_timestep.total_seconds() * self.volume_factor).alias("discharge_volume")
            )
        
        self.water_flow_factor = generate_water_flow_factor(index=self.index)
        basin_volume_table: dict[int, Optional[pl.DataFrame]] = generate_basin_volume_table(
            index=self.index,
            basin_height_volume_table=schema_dict["basin_height_volume_table"],
            volume_factor=self.volume_factor)

        self.power_performance_table = clean_hydro_power_performance_table(
            index=self.index,
            schema_dict=schema_dict,
            basin_volume_table=basin_volume_table)

        