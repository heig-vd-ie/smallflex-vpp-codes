import re
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
from pyomo_models.baseline.baseline_input import BaseLineInput
from pyomo_models.baseline.first_stage.sets import baseline_sets
from pyomo_models.baseline.first_stage.parameters import baseline_parameters
from pyomo_models.baseline.first_stage.variables import baseline_variables
from pyomo_models.baseline.first_stage.objective import baseline_objective
from pyomo_models.baseline.first_stage.constraints.basin_volume import basin_volume_constraints
from pyomo_models.baseline.first_stage.constraints.turbine import turbine_constraints
from pyomo_models.baseline.first_stage.constraints.pump import pump_constraints

log = generate_log(name=__name__)

class BaselineFirstStage(BaseLineInput):
    def __init__(self, input_instance: BaseLineInput, first_time_step: timedelta):
        self.subset_mapping =   {
            "T": "T", "H": "H", "B": "B", "BS": ["B", "S_B"], "HS": ["H", "S_H"], "S_BH": {"H", "H", "S_B", "S_H"}
        }
        self.first_time_step = first_time_step
        self.retrieve_input(input_instance)
        self.model_instance: pyo.Model = pyo.ConcreteModel() 
        self.generate_index()
        self.process_timeseries()
        self.generate_model()
        
    def retrieve_input(self, input_instance):
        for name, value in input_instance.__dict__.items():
            setattr(self, name, value)
        
        
    def generate_index(self):
        self.index : dict[str, pl.DataFrame]= generate_baseline_index(
            small_flex_input_schema=self.small_flex_input_schema,
            year=self.year,
            timestep=self.first_time_step, 
            real_timestep=self.real_time_step,
            hydro_power_mask=self.hydro_power_mask
        )
        
        self.water_flow_factor: pl.DataFrame = generate_water_flow_factor(index=self.index)
        basin_volume_table: dict[int, Optional[pl.DataFrame]] = generate_basin_volume_table(
        small_flex_input_schema=self.small_flex_input_schema, index=self.index)

        self.power_performance_table: list[dict]  = clean_hydro_power_performance_table(
            small_flex_input_schema=self.small_flex_input_schema, index=self.index, 
            basin_volume_table=basin_volume_table)

        self.index: dict[str, pl.DataFrame]  = generate_hydro_power_state(
            power_performance_table=self.power_performance_table, index=self.index, error_percent=1.3
            )
    
    def process_timeseries(self):
        ### Discharge_flow ##############################################################################################
        self.discharge_volume: pl.DataFrame = generate_clean_timeseries(
                data=self.discharge_flow_measurement,
                col_name="discharge_volume",
                min_datetime=self.min_datetime,
                max_datetime=self.max_datetime, 
                timestep=self.first_time_step , 
                agg_type="sum"
            ).with_columns(
                pl.concat_list(["T", pl.lit(0).alias("B")]).alias("TB")
            )
        ### Market price ###############################################################################################
        self.market_price: pl.DataFrame = generate_clean_timeseries(
            data=self.market_price_measurement,
            col_name="avg", 
            min_datetime=self.min_datetime, 
            max_datetime=self.max_datetime, 
            timestep=self.first_time_step, 
            agg_type="mean"
        )
        

    def generate_model(self):
        self.model: pyo.AbstractModel = pyo.AbstractModel()
        self.model = baseline_sets(self.model)
        self.model = baseline_parameters(self.model)
        self.model = baseline_variables(self.model)
        
        self.model = baseline_objective(self.model)
        self.model = basin_volume_constraints(self.model)
        self.model = turbine_constraints(self.model)
        self.model = pump_constraints(self.model)

    def create_model_instance(self):
        hydropower_state: pl.DataFrame = self.index["state"].drop_nulls("H")
        data: dict = {}

        data["T"] = {None: self.index["datetime"]["T"].to_list()}
        data["H"] = {None: self.index["hydro_power_plant"]["H"].to_list()}
        data["B"] = {None: self.index["water_basin"]["B"].to_list()}
        data["S_b"] = pl_to_dict(self.index["state"].group_by("B", maintain_order=True).agg("S"))
        data["S_h"] = pl_to_dict(self.index["state"].drop_nulls("H").group_by("H", maintain_order=True).agg("S"))
        data["S_BH"] = {None: list(map(tuple, self.index["state"].drop_nulls("H")["S_BH"].to_list()))}
    
        data["start_basin_volume"] = pl_to_dict(self.index["water_basin"][["B", "start_volume"]])
        data["water_pumped_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "pumped_factor"])
        data["water_turbined_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "turbined_factor"])
        data["min_basin_volume"] = pl_to_dict_with_tuple(
            self.index["state"].select("BS", c("volume").struct.field("min")))
        data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.index["state"].select("BS", c("volume").struct.field("max")))
        data["max_flow_turbined"] = pl_to_dict_with_tuple(
            hydropower_state.select("HS", c("flow_turbined").struct.field("min")))
        data["max_flow_pumped"] = pl_to_dict_with_tuple(
            hydropower_state.select("HS", c("flow_pumped").struct.field("min")))
        data["alpha_turbined"] = pl_to_dict_with_tuple(
            hydropower_state.select("HS", c("alpha_turbined").struct.field("min")))
        data["alpha_pumped"] = pl_to_dict_with_tuple(
            hydropower_state.select("HS", c("alpha_pumped").struct.field("min")))
        data["discharge_volume"] = pl_to_dict_with_tuple(self.discharge_volume[["TB", "discharge_volume"]])  
        data["market_price"] = pl_to_dict(self.market_price[["T", "avg"]])
        data["max_market_price"] = pl_to_dict(self.market_price[["T", "max_avg"]])
        data["min_market_price"] = pl_to_dict(self.market_price[["T", "min_avg"]])
        data["nb_hours"] = pl_to_dict(self.index["datetime"][["T", "n_index"]])
        
        self.model_instance: pyo.Model = self.model.create_instance({None: data})

    def solve_model(self):
        with tqdm.tqdm(total=1, desc="Solving first stage optimization problem") as pbar:
            
            _ = self.solver.solve(self.model_instance)
            pbar.update()
        