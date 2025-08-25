
from datetime import timedelta
from itertools import tee
import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo
import tqdm

from utility.pyomo_preprocessing import (
    extract_optimization_results, pivot_result_table, remove_suffix, generate_clean_timeseries, generate_datetime_index)
from utility.input_data_preprocessing import (
    generate_hydro_power_state, generate_basin_state_table
)
from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log
from baseline_model.baseline_input import BaseLineInput
from baseline_model.optimization_results_processing import process_first_stage_results
from baseline_model.first_stage.sets import baseline_sets
from baseline_model.first_stage.parameters import baseline_parameters
from baseline_model.first_stage.variables import baseline_variables
from baseline_model.first_stage.objective import baseline_objective
from baseline_model.first_stage.constraints.basin_volume import basin_volume_constraints
from baseline_model.first_stage.constraints.hydro_power_plant import hydro_power_plant_constraints

log = generate_log(name=__name__)

class BaselineFirstStage(BaseLineInput):
    def __init__(
        self, input_instance: BaseLineInput, timestep: timedelta, nb_state: int, max_turbined_volume_factor: float= 0.75,
        ):
        
        self.retrieve_input(input_instance)
        
        self.max_turbined_volume_factor: float = max_turbined_volume_factor
        self.timestep: timedelta = timestep
        self.nb_state: int = nb_state
        
        self.optimization_results : pl.DataFrame
        self.model_instance: pyo.Model
        
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
        self.index["basin_state"] = generate_basin_state_table(
            basin_volume_table=self.basin_volume_table, water_basin=self.index["water_basin"] , nb_state=self.nb_state)
        self.index["hydro_power_state"]  = generate_hydro_power_state(
            power_performance_table=self.power_performance_table, basin_state=self.index["basin_state"] )

    def process_timeseries(self):
        ### Discharge_flow ##############################################################################################
        
        self.discharge_volume: pl.DataFrame = generate_clean_timeseries(
                data=self.discharge_flow_measurement,
                col_name="discharge_volume",
                min_datetime=self.min_datetime,
                max_datetime=self.max_datetime,
                timestep=self.timestep, 
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
    
        self.ancillary_market_price: pl.DataFrame = generate_clean_timeseries(
            data=self.ancillary_market_price_measurement,
            col_name="avg",
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            timestep=self.timestep,
            agg_type="mean"
        )

    def generate_model(self):
        self.model: pyo.AbstractModel = pyo.AbstractModel() # type: ignore
        self.model = baseline_sets(self.model)
        self.model = baseline_parameters(self.model)
        self.model = baseline_variables(self.model)
        
        self.model = baseline_objective(self.model)
        self.model = basin_volume_constraints(self.model)
        self.model = hydro_power_plant_constraints(self.model)

    def create_model_instance(self):
        data: dict = {}

        data["T"] = {None: self.index["datetime"]["T"].to_list()}
        data["H"] = {None: self.index["hydro_power_plant"]["H"].to_list()}
        data["B"] = {None: self.index["water_basin"]["B"].to_list()}
        data["DH"] = {None: self.index["hydro_power_plant"].filter(c("control") == "discrete")["H"].to_list()}
        
        data["S_b"] = pl_to_dict(self.index["basin_state"].group_by("B", maintain_order=True).agg("S_B"))
        data["S_h"] = pl_to_dict(self.index["hydro_power_state"].drop_nulls("H").group_by("H", maintain_order=True).agg("S_H"))
        data["S_BH"] = {None: list(map(tuple, self.index["hydro_power_state"].drop_nulls("H")["S_BH"].to_list()))}
    
        data["max_turbined_volume_factor"] = {None: self.max_turbined_volume_factor}
        data["volume_factor"] = {None: self.volume_factor}
        
        data["start_basin_volume"] = pl_to_dict(self.index["water_basin"][["B", "start_volume"]])
        data["water_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "water_factor"])
        data["spilled_factor"] = pl_to_dict(self.spilled_factor["B", "spilled_factor"])

        data["min_basin_volume"] = pl_to_dict_with_tuple(
            self.index["basin_state"].select("BS", "volume_min"))
        data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.index["basin_state"].select("BS", "volume_max"))
        
        data["max_flow"] = pl_to_dict_with_tuple(
            self.index["hydro_power_state"].select("HS", "flow"))
        data["alpha"] = pl_to_dict_with_tuple(
            self.index["hydro_power_state"].select("HS", "alpha"))

        data["discharge_volume"] = pl_to_dict_with_tuple(self.discharge_volume[["TB", "discharge_volume"]])

        data["market_price"] = pl_to_dict(self.market_price[["T", "avg"]])
        data["ancillary_market_price"] = pl_to_dict(self.ancillary_market_price[["T", "avg"]])
        data["max_power"] = {1: 7}
        
        data["max_market_price"] = pl_to_dict(self.market_price[["T", "max_avg"]])
        data["min_market_price"] = pl_to_dict(self.market_price[["T", "min_avg"]])
        data["nb_hours"] = pl_to_dict(self.index["datetime"][["T", "n_index"]])
        
        self.model_instance: pyo.Model = self.model.create_instance({None: data})

    def solve_model(self):
        with tqdm.tqdm(total=1, desc="Solving first stage optimization problem", ncols=150) as pbar:
            self.create_model_instance()
            _ = self.solver.solve(self.model_instance, tee=False)
            pbar.update()
        self.optimization_results = process_first_stage_results(
            model_instance=self.model_instance, market_price=self.market_price, index=self.index,
            flow_to_vol_factor= self.real_timestep.total_seconds() * self.volume_factor)
        