
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
from pipelines.data_configs import PipelineConfig
from pipelines.data_manager import PipelineDataManager

from pipelines.baseline_model.first_stage import first_stage_baseline_model


log = generate_log(name=__name__)

class BaselineFirstStage(PipelineDataManager):
    def __init__(
        self, pipeline_data_manager: PipelineDataManager
        ):
        # Retrieve attributes from pipeline_data_manager
        for key, value in vars(pipeline_data_manager).items():
            setattr(self, key, value)
        
        self.model: pyo.AbstractModel = first_stage_baseline_model()
        self.model_instance: pyo.ConcreteModel
    
    def create_model_instance(self):
        data: dict = {}
        # index
        data["T"] = {None: self.first_stage_timestep_index["T"].to_list()}
        data["H"] = {None: self.first_stage_hydro_power_state["H"].to_list()}
        data["B"] = {None: self.water_basin["B"].to_list()}
        data["DH"] = {None: self.hydro_power_plant.filter(c("control") == "discrete")["H"].to_list()}

        data["S_B"] = pl_to_dict(
            self.first_stage_basin_state
            .group_by("B", maintain_order=True).agg("S")
        )
        data["S_H"] = pl_to_dict(
            self.first_stage_hydro_power_state
            .drop_nulls("H").group_by("H", maintain_order=True).agg("S")
        )
        data["BHS"] = {None: 
            list(map(
                tuple, 
                self.first_stage_hydro_power_state\
                    .drop_nulls("H")["BHS"].to_list()
            ))}
        data["nb_hours"] = pl_to_dict(self.first_stage_timestep_index[["T", "n_index"]])

        # Water basin
        data["start_basin_volume"] = pl_to_dict(self.water_basin[["B", "start_volume"]])
        data["water_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "water_factor"])
        data["spilled_factor"] = pl_to_dict(self.basin_spilled_factor["B", "spilled_factor"])

        data["min_basin_volume"] = pl_to_dict_with_tuple(
            self.first_stage_basin_state.select("BS", "volume_min"))
        data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.first_stage_basin_state.select("BS", "volume_max"))
        #Hydro power plant
        data["max_flow"] = pl_to_dict_with_tuple(
            self.first_stage_hydro_power_state.select("HS", "flow"))
        data["alpha"] = pl_to_dict_with_tuple(
            self.first_stage_hydro_power_state.select("HS", "alpha"))
        data["max_power"] = {1: 7}
        
        # Timeseries
        data["discharge_volume"] = pl_to_dict_with_tuple(self.first_stage_discharge_volume[["TB", "discharge_volume"]])
        data["market_price"] = pl_to_dict(self.first_stage_market_price[["T", "avg"]])
        data["ancillary_market_price"] = pl_to_dict(self.first_stage_ancillary_market_price[["T", "avg"]])

        # Configuration parameters
        data["max_turbined_volume_factor"] = {None: self.first_stage_max_turbined_volume}
        data["volume_factor"] = {None: self.volume_factor}

        self.model_instance = self.model.create_instance({None: data}) # type: ignore

    def solve_model(self):
        with tqdm.tqdm(total=1, desc="Solving first stage optimization problem", ncols=150) as pbar:
            self.create_model_instance()
            _ = self.solver.solve(self.model_instance, tee=self.verbose)
            pbar.update()
        # self.optimization_results = process_first_stage_results(
        #     model_instance=self.model_instance, market_price=self.market_price, index=self.index,
        #     flow_to_vol_factor= self.real_timestep.total_seconds() * self.volume_factor)
        