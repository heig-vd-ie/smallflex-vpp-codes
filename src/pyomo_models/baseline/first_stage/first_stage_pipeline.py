import re
from typing import Optional
from datetime import datetime, timedelta, timezone
import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo
import tqdm

from data_federation.input_model import SmallflexInputSchema

from utility.pyomo_preprocessing import (
    extract_optimization_results, pivot_result_table, remove_suffix, generate_clean_timeseries, generate_datetime_index)
from pyomo_models.input_data_preprocessing import (
    generate_hydro_power_state
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
    def __init__(
        self, input_instance: BaseLineInput, timestep: timedelta, pump_factor: float = 1, turbine_factor: float= 0.75,
        error_percent: float = 2):
        
        self.pump_factor: float = pump_factor
        self.turbine_factor: float = turbine_factor
        self.timestep: timedelta = timestep
        self.error_percent: float = error_percent
        self.retrieve_input(input_instance)
        self.model_instance: pyo.Model = pyo.ConcreteModel() 
        
        self.generate_index()
        self.process_timeseries()
        self.generate_model()
        
    def retrieve_input(self, input_instance):
        for name, value in input_instance.__dict__.items():
            setattr(self, name, value)
        
        
    def generate_index(self):
        self.index["datetime"] = generate_datetime_index(
            min_datetime=self.min_datetime, max_datetime=self.max_datetime, model_timestep=self.timestep, 
            real_timestep=self.real_timestep
        )
        self.index["state"]  = generate_hydro_power_state(
            power_performance_table=self.power_performance_table, 
            index=self.index, 
            error_percent=self.error_percent
        )
        
    
    def process_timeseries(self):
        ### Discharge_flow ##############################################################################################
        self.discharge_volume: pl.DataFrame = generate_clean_timeseries(
                data=self.discharge_flow_measurement,
                col_name="discharge_volume",
                min_datetime=self.min_datetime,
                max_datetime=self.max_datetime, 
                timestep=self.timestep , 
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
            timestep=self.timestep, 
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
        
    
    def process_result(self): 

        market_price = self.market_price.select(
            c("T"),
            c("timestamp"),
            cs.ends_with("avg").name.map(lambda x: x.replace("avg", "") + "market_price")
        )
        
        volume_max_mapping: dict[str, float] = pl_to_dict(self.index["water_basin"][["B", "volume_max"]])
        basin_volume = extract_optimization_results(
                model_instance=self.model_instance, var_name="basin_volume"
            ).with_columns(
                (c("basin_volume") / c("B").replace_strict(volume_max_mapping, default=None)).alias("basin_volume")
            )

        basin_volume = pivot_result_table(
            df = basin_volume, on="B", index=["T"], 
            values="basin_volume")

        nb_hours_mapping = pl_to_dict(extract_optimization_results(
                model_instance=self.model_instance, var_name="nb_hours"
        )[["T", "nb_hours"]])

        turbined_volume = extract_optimization_results(
                model_instance=self.model_instance, var_name="turbined_flow"
            ).with_columns(
                (
                    c("turbined_flow") * self.real_timestep.total_seconds() * self.volume_factor *
                    c("T").replace_strict(nb_hours_mapping, default=None)
                ).alias("turbined_volume")
            )
            
        turbined_volume = pivot_result_table(
            df = turbined_volume, on="H", index=["T"], 
            values="turbined_volume")
            
        pumped_volume = extract_optimization_results(
                model_instance=self.model_instance, var_name="pumped_flow"
            ).with_columns(
                (
                    c("pumped_flow") * self.real_timestep.total_seconds() * self.volume_factor *
                    c("T").replace_strict(nb_hours_mapping, default=None)
                ).alias("pumped_volume")
            )
            
        pumped_volume = pivot_result_table(
            df = pumped_volume, on="H", index=["T"], 
            values="pumped_volume")

        pumped_power = extract_optimization_results(
                model_instance=self.model_instance, var_name="pumped_power"
            )

        pumped_power = pivot_result_table(
            df = pumped_power, on="H", index=["T"], 
            values="pumped_power")

        turbined_power = extract_optimization_results(
                model_instance=self.model_instance, var_name="turbined_power"
            )

        turbined_power = pivot_result_table(
            df = turbined_power, on="H", index=["T"], 
            values="turbined_power")

        simulation_results = market_price\
            .join(basin_volume, on = "T", how="inner")\
            .join(turbined_volume, on = "T", how="inner")\
            .join(pumped_volume, on = "T", how="inner")\
            .join(pumped_power, on = "T", how="inner")\
            .join(turbined_power, on = "T", how="inner")\
            .with_columns(
                ((
                    pl.sum_horizontal(cs.starts_with("turbined_power")) -
                    pl.sum_horizontal(cs.starts_with("pumped_power"))
                ) * c("T").replace_strict(nb_hours_mapping, default=None) * c("market_price")).alias("income")
            )
            
        hydro_name = list(map(str, list(self.model_instance.H))) # type: ignore

        self.simulation_results = simulation_results.with_columns(
            pl.struct(cs.ends_with(hydro) & ~cs.starts_with("basin_volume"))
            .pipe(remove_suffix).alias("hydro_" + hydro) 
            for hydro in hydro_name
        ).select(    
            ~(cs.starts_with("turbined") | cs.starts_with("pumped"))
        )

    def create_model_instance(self):
        hydropower_state: pl.DataFrame = self.index["state"].drop_nulls("H")
        data: dict = {}

        data["T"] = {None: self.index["datetime"]["T"].to_list()}
        data["H"] = {None: self.index["hydro_power_plant"]["H"].to_list()}
        data["B"] = {None: self.index["water_basin"]["B"].to_list()}
        data["S_b"] = pl_to_dict(self.index["state"].group_by("B", maintain_order=True).agg("S"))
        data["S_h"] = pl_to_dict(self.index["state"].drop_nulls("H").group_by("H", maintain_order=True).agg("S"))
        data["S_BH"] = {None: list(map(tuple, self.index["state"].drop_nulls("H")["S_BH"].to_list()))}
    
        data["pump_factor"] = {None: self.pump_factor}
        data["turbine_factor"] = {None: self.turbine_factor}
        data["volume_factor"] = {None: self.volume_factor}
        
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
        with tqdm.tqdm(total=1, desc="Solving first stage optimization problem", ncols=150) as pbar:
            self.create_model_instance()
            _ = self.solver.solve(self.model_instance)
            pbar.update()
        self.process_result()
        