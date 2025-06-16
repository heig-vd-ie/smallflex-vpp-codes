from typing import Optional
from datetime import datetime, timedelta, timezone
import polars as pl
from polars import col as c
import pyomo.environ as pyo
from smallflex_data_schema import SmallflexInputSchema

from utility.input_data_preprocessing import (
    generate_basin_volume_table, clean_hydro_power_performance_table
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
        smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(file_path=input_schema_file_name)

        self.index["hydro_power_plant"] = smallflex_input_schema.hydro_power_plant\
            .filter(self.hydro_power_mask)\
            .with_row_index(name="H")
            
        water_flow_factor = self.index["hydro_power_plant"].unpivot(
            on=["upstream_basin_fk", "downstream_basin_fk"], index= ["H", "type"], variable_name="basin_type", value_name="basin_fk")
            
        bassin_uuid = water_flow_factor["basin_fk"].unique().to_list()

        self.index["water_basin"] = smallflex_input_schema.water_basin\
            .filter(c("uuid").is_in(bassin_uuid))\
            .with_columns(
                c("volume_max", "volume_min", "start_volume")*self.volume_factor
            ).with_row_index(name="B")
            
        basin_index_mapping = pl_to_dict(self.index["water_basin"][["uuid", "B"]])


        self.index["hydro_power_plant"] = self.index["hydro_power_plant"].with_columns(
            c(f"{col}_basin_fk").replace_strict(basin_index_mapping, default=None).alias(f"{col}_B")
            for col in ["upstream", "downstream"]
        )

        self.discharge_flow_measurement = smallflex_input_schema.discharge_flow_historical\
            .with_columns(
                c("basin_fk").replace_strict(basin_index_mapping, default=None).alias("B")
            ).drop_nulls("B")\
            .drop_nulls(subset="basin_fk").with_columns(
                (c("value") * self.real_timestep.total_seconds() * self.volume_factor).alias("discharge_volume")
            )
        

        self.market_price_measurement = smallflex_input_schema.market_price_measurement\
            .filter(c("country") == self.market_country)\
            .filter(c("market") == self.market)
        
        
        water_volume_mapping = {
                "upstream_basin_fk" : -1, "downstream_basin_fk" :  1
            }

        hydro_type_mapping = {
                "turbine" : 1, "pump" :  -1
            }

        water_flow_factor = water_flow_factor.with_columns(
            c("basin_fk").replace_strict(basin_index_mapping, default=None).alias("B"),
            (
                c("basin_type").replace_strict(water_volume_mapping, default=None) * 
                c("type").replace_strict(hydro_type_mapping, default=None)
            ).alias("water_factor")
        )

        self.spilled_factor = water_flow_factor.filter(c("basin_type") =="upstream_basin_fk").select(
            "B", pl.lit(1e3).alias("spilled_factor")
        ).unique(subset="B")
        
        self.water_flow_factor = water_flow_factor.select("B", "H", pl.concat_list(["B", "H"]).alias("BH"), "water_factor")

        self.basin_volume_table: dict[int, Optional[pl.DataFrame]] = generate_basin_volume_table(
            index=self.index,
            basin_height_volume_table=smallflex_input_schema.basin_height_volume_table,
            volume_factor=1e-6)
        
        self.power_performance_table: list[dict] = clean_hydro_power_performance_table(
                    index=self.index,
                    smallflex_input_schema=smallflex_input_schema,
                    basin_volume_table=self.basin_volume_table)
            

        # self.market_price_measurement = smallflex_input_schema.market_price_measurement\
        #     .filter(c("country") == self.market_country)\
        #     .filter(c("market") == self.market)

        # self.index["hydro_power_plant"] = smallflex_input_schema.hydro_power_plant\
        #     .filter(self.hydro_power_mask).select(pl.all().repeat_by(2).flatten()).with_row_index(name="H")

        # self.index["water_basin"] = smallflex_input_schema.water_basin\
        #     .filter(c("power_plant_fk").is_in(self.index["hydro_power_plant"]["uuid"]))\
        #     .with_columns(
        #         c("volume_max", "volume_min", "start_volume")*self.volume_factor
        #     ).with_row_index(name="B")

        # basin_index_mapping = pl_to_dict(self.index["water_basin"][["uuid", "B"]])

        # self.discharge_flow_measurement = smallflex_input_schema.discharge_flow_historical\
        #     .with_columns(
        #         c("basin_fk").replace_strict(basin_index_mapping, default=None).alias("B")
        #     ).drop_nulls(subset="basin_fk").with_columns(
        #         (c("value") * self.real_timestep.total_seconds() * self.volume_factor).alias("discharge_volume")
        #     )
        
        # self.water_flow_factor = generate_water_flow_factor(index=self.index)
        # basin_volume_table: dict[int, Optional[pl.DataFrame]] = generate_basin_volume_table(
        #     index=self.index,
        #     basin_height_volume_table=smallflex_input_schema.basin_height_volume_table,
        #     volume_factor=self.volume_factor)

        # self.power_performance_table = clean_hydro_power_performance_table(
        #     index=self.index,
        #     schema_dict=schema_dict,
        #     basin_volume_table=basin_volume_table)

        