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
from pipelines.data_configs import PipelineConfig

class PipelineDataManager():
    def __init__(
        self,
        pipeline_config: PipelineConfig,
        smallflex_input_schema: SmallflexInputSchema,
        hydro_power_mask: Optional[pl.Expr] = None

    ):
        
        if hydro_power_mask is None:
            hydro_power_mask = pl.lit(True)

        self.hydro_power_plant: pl.DataFrame = pl.DataFrame()
        self.water_basin: pl.DataFrame = pl.DataFrame()
        self.basin_volume_table: pl.DataFrame = pl.DataFrame()
        self.spilled_factor: pl.DataFrame = pl.DataFrame()
        self.power_performance_table: pl.DataFrame = pl.DataFrame()
        self.water_flow_factor: pl.DataFrame = pl.DataFrame()
        
        self.build_hydro_power_plant_data(
            smallflex_input_schema=smallflex_input_schema, 
            pipeline_config=pipeline_config, 
            hydro_power_mask=hydro_power_mask
            )
        self.build_measurements(
            smallflex_input_schema=smallflex_input_schema,
            pipeline_config=pipeline_config
        )
        
    def build_hydro_power_plant_data(self, smallflex_input_schema: SmallflexInputSchema, pipeline_config: PipelineConfig, hydro_power_mask: pl.Expr):
        
        water_volume_mapping = {
                "upstream_basin_fk" : -1, "downstream_basin_fk" :  1
            }

        hydro_type_mapping = {
                "turbine" : 1, "pump" :  -1
            }

        self.hydro_power_plant = smallflex_input_schema.hydro_power_plant\
            .filter(hydro_power_mask)\
            .with_row_index(name="H")
            

        self.water_basin = smallflex_input_schema.water_basin\
            .filter(
                c("uuid").is_in(
                    self.hydro_power_plant["upstream_basin_fk"].to_list() + 
                    self.hydro_power_plant["downstream_basin_fk"].to_list()
                )
            ).with_columns(
                c("volume_max", "volume_min", "start_volume")*pipeline_config.volume_factor
            ).with_row_index(name="B")
            
        basin_index_mapping = pl_to_dict(self.water_basin[["uuid", "B"]])


        self.hydro_power_plant =  self.hydro_power_plant.with_columns(
            c(f"{col}_basin_fk").replace_strict(basin_index_mapping, default=None).alias(f"{col}_B")
            for col in ["upstream", "downstream"]
        )


        water_flow_factor = self.hydro_power_plant\
            .unpivot(
                on=["upstream_basin_fk", "downstream_basin_fk"], index= ["H", "type"], 
                variable_name="basin_type", value_name="basin_fk"
            )

        water_flow_factor = water_flow_factor.with_columns(
            c("basin_fk").replace_strict(basin_index_mapping, default=None).alias("B"),
            (
                c("basin_type").replace_strict(water_volume_mapping, default=None) *
                c("type").replace_strict(hydro_type_mapping, default=None)
            ).alias("water_factor")
        )

        self.spilled_factor = water_flow_factor.filter(c("basin_type") =="upstream_basin_fk").select(
            "B", pl.lit(pipeline_config.spilled_factor).alias("spilled_factor")
        ).unique(subset="B")
        
        self.water_flow_factor = water_flow_factor.select("B", "H", pl.concat_list(["B", "H"]).alias("BH"), "water_factor")

        self.basin_volume_table = generate_basin_volume_table(
            water_basin=self.water_basin,
            basin_height_volume_table=smallflex_input_schema.basin_height_volume_table,
            volume_factor=pipeline_config.volume_factor)
        
        self.power_performance_table = clean_hydro_power_performance_table(
                    hydro_power_plant=self.hydro_power_plant,
                    water_basin=self.water_basin,
                    hydro_power_performance_table=smallflex_input_schema.hydro_power_performance_table.as_polars(),
                    basin_volume_table=self.basin_volume_table)
            
    def build_measurements(self, smallflex_input_schema: SmallflexInputSchema, pipeline_config: PipelineConfig,):
        
        basin_index_mapping = pl_to_dict(self.water_basin[["uuid", "B"]])
    
        self.discharge_flow_measurement = smallflex_input_schema.discharge_flow_historical\
            .with_columns(
                c("basin_fk").replace_strict(basin_index_mapping, default=None).alias("B")
            ).drop_nulls("B")\
            .drop_nulls(subset="basin_fk").with_columns(
                (c("value") * pipeline_config.second_stage_timestep.total_seconds() * pipeline_config.volume_factor).alias("discharge_volume")
            )

        self.market_price_measurement = smallflex_input_schema.market_price_measurement\
            .filter(c("country") == pipeline_config.market_country)\
            .filter(c("market") == pipeline_config.market)
        
        self.ancillary_market_price_measurement = smallflex_input_schema.market_price_measurement\
            .filter(c("country") == pipeline_config.market_country)\
            .filter(c("market") == pipeline_config.ancillary_market)\
            .filter(c("source") == pipeline_config.market_source).sort("timestamp")

