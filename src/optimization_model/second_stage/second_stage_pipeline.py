from datetime import timedelta
import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo
import tqdm
import logging

from utility.input_data_preprocessing import (
    split_timestamps_per_sim, generate_second_stage_hydro_power_state, generate_seconde_stage_basin_state
)
from utility.pyomo_preprocessing import (generate_datetime_index, generate_clean_timeseries)
from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log

from baseline_model.baseline_input import BaseLineInput
from baseline_model.optimization_results_processing import process_second_stage_results
from baseline_model.first_stage.first_stage_pipeline import BaselineFirstStage
from baseline_model.second_stage.sets import baseline_sets
from baseline_model.second_stage.parameters import baseline_parameters
from baseline_model.second_stage.variables import baseline_variables
from baseline_model.second_stage.objective import baseline_objective
from baseline_model.second_stage.constraints.basin_volume import basin_volume_constraints
from baseline_model.second_stage.constraints.powered_volume import powered_volume_constraints
from baseline_model.second_stage.constraints.hydro_power_plant import hydro_constraints



log = generate_log(name=__name__)

class BaselineSecondStage(BaseLineInput):
    def __init__(
        self,
        ):
    
        

        
        self.solver.options['TimeLimit'] = time_limit
        self.solver.options['Threads'] = 8 
        
        # self.generate_index()
        # self.generate_model()
        
        # self.initialise_volume()
        # self.calculate_powered_volume()
        # self.generate_volume_buffer()
        # self.process_timeseries()
        
        self.generate_constant_parameters()
        
    def retrieve_input(self, input_instance):
        for name, value in input_instance.__dict__.items():
            setattr(self, name, value) 

    def generate_constant_parameters(self):
        
        alpha_pos: dict[int, float] = {}
        alpha_neg: dict[int, float] = {}
        for data in self.power_performance_table:
            alpha = data["power_performance"].with_columns(pl.when(c("flow") < 0).then(-c("alpha")).otherwise(c("alpha")).alias("alpha"))
            alpha_pos[data["H"]] = alpha["alpha"].max()
            alpha_neg[data["H"]] = alpha["alpha"].min()
        
        self.data["H"] = {None: self.index["hydro_power_plant"]["H"].to_list()}
        self.data["DH"] = {None: self.index["hydro_power_plant"].filter(c("control") == "discrete")["H"].to_list()}
        self.data["B"] = {None: self.index["water_basin"]["B"].to_list()}
        self.data["buffer"] = {None: self.buffer}
        self.data["water_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "water_factor"])
        self.data["alpha_pos"] = alpha_pos
        self.data["alpha_neg"] = alpha_neg
        self.data["volume_factor"] = {None: self.volume_factor}
        self.data["spilled_factor"] = pl_to_dict(self.spilled_factor["B", "spilled_factor"])
        self.data["neg_unpowered_price"] = {
            None: self.market_price["avg"].quantile(0.5 + self.quantile)}
        self.data["pos_unpowered_price"] = {
            None: self.market_price["avg"].quantile(0.5 - self.quantile)}
            

            
    def generate_state_index(self):
        start_volume_dict = pl_to_dict(
            self.optimization_results["start_basin_volume"].filter(c("sim_nb") == self.sim_nb)[["B", "start_basin_volume"]])

        discharge_volume_tot= pl_to_dict(
            self.discharge_volume.filter(c("sim_nb") == self.sim_nb).group_by("B").agg(c("discharge_volume").sum()))

        self.index["basin_state"], basin_volume = generate_seconde_stage_basin_state(
            index=self.index, water_flow_factor=self.water_flow_factor, 
            basin_volume_table=self.basin_volume_table, start_volume_dict=start_volume_dict, 
            discharge_volume_tot=discharge_volume_tot,
            timestep=self.sim_timestep, volume_factor=self.volume_factor, nb_state=self.nb_state
        )

        self.index["hydro_power_state"] = generate_second_stage_hydro_power_state(
            power_performance_table=self.power_performance_table, basin_volume=basin_volume)


    def generate_model_instance(self):
        

        self.data["T"] = {None: self.index["datetime"].filter(c("sim_nb") == self.sim_nb)["T"].to_list()}
        self.data["F"] = {None: self.index["datetime"].filter(c("sim_nb") == self.sim_nb)["F"].unique().sort().to_list()}
        self.data["TF"] = {None: list(map(tuple, self.index["datetime"].filter(c("sim_nb") == self.sim_nb)["TF"].to_list()))}
        self.data["S_B"] = pl_to_dict(
            self.index["basin_state"]["B", "S_b"]
            .group_by("B", maintain_order=True).agg("S_b")
            .with_columns(c("S_b").list.sort())
        )
        self.data["S_H"] = pl_to_dict(
            self.index["hydro_power_state"]
            .group_by("H", maintain_order=True)
            .agg("S_h")
            .with_columns(c("S_h").list.sort())
            )

        self.data["S_BH"] = {None: list(map(tuple, self.index["hydro_power_state"]["S_BH"].to_list()))}
        
        self.data["start_basin_volume"] = pl_to_dict(
            self.optimization_results["start_basin_volume"].filter(c("sim_nb") == self.sim_nb)[["B", "start_basin_volume"]])
        self.data["remaining_volume"] = pl_to_dict(self.optimization_results["remaining_volume"].filter(c("sim_nb") == self.sim_nb)[["H", "remaining_volume"]])

        self.data["min_basin_volume"] = pl_to_dict_with_tuple(
                    self.index["basin_state"]["BS", "volume_min"])
        self.data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.index["basin_state"]["BS", "volume_max"])
        self.data["powered_volume_enabled"] = {None: self.powered_volume_enabled}

        self.data["discharge_volume"] = pl_to_dict_with_tuple(self.discharge_volume.filter(c("sim_nb") == self.sim_nb)[["TB", "discharge_volume"]])  
        self.data["market_price"] = pl_to_dict(self.market_price.filter(c("sim_nb") == self.sim_nb)[["T", "avg"]])
        self.data["ancillary_market_price"] = pl_to_dict(self.ancillary_market_price.filter(c("sim_nb") == self.sim_nb)[["F", "avg"]])
        
        if not self.global_price:
            self.data["neg_unpowered_price"] = {
                None: self.market_price.filter(c("sim_nb") == self.sim_nb)["avg"].quantile(0.5 + self.quantile)}
            self.data["pos_unpowered_price"] = {
                None: self.market_price.filter(c("sim_nb") == self.sim_nb)["avg"].quantile(0.5 - self.quantile)}

        self.data["powered_volume"] = pl_to_dict(self.powered_volume.filter(c("sim_nb") == self.sim_nb)[["H", "powered_volume"]])
        self.data["volume_buffer"] = pl_to_dict(self.volume_buffer.filter(c("sim_nb") == self.sim_nb)[["H", "volume_buffer"]])

        self.data["max_flow"] = pl_to_dict_with_tuple(self.index["hydro_power_state"][["HS", "flow"]])  
        self.data["alpha"] = pl_to_dict_with_tuple(self.index["hydro_power_state"][["HS", "alpha"]])  
        
        self.data["max_power"] = {1: 7}
                        
        self.model_instance: pyo.Model = self.model.create_instance({None: self.data}) 


    def solve_model(self):
        logging.getLogger('pyomo.core').setLevel(logging.ERROR)
        pump_id =self.index["hydro_power_plant"].filter(c("type") == "pump")["H"].to_list()
        for sim_nb in tqdm.tqdm(
            range(self.sim_tot + 1),
            desc=f"Solving second stage optimization model number {self.model_nb}",
            position=self.model_nb if self.is_parallel else 0,
            ncols=150,
            leave=True
        ):
            self.sim_nb = sim_nb
            self.generate_state_index()
            self.generate_model_instance()
            solution = self.solver.solve(self.model_instance, tee=self.log_solver_info)
            if solution["Solver"][0]["Status"] == "ok":
                self.optimization_results = process_second_stage_results(
                    model_instance=self.model_instance, optimization_results=self.optimization_results, pump_id=pump_id, sim_nb=self.sim_nb)
            elif solution["Solver"][0]["Status"] == "aborted":
                self.log_mip_gap(solution)
                self.optimization_results = process_second_stage_results(
                    model_instance=self.model_instance, optimization_results=self.optimization_results, pump_id=pump_id, sim_nb=self.sim_nb)
            else:
                self.solver.solve(self.model_instance, tee=True)
                # solved = self.solve_changing_powered_volume_constraint()
                # if not solved:
                log.error(f"Model not solved for sim number {self.sim_nb}")
                break
                
        logging.getLogger('pyomo.core').setLevel(logging.WARNING)   
    
    def solve_one_instance(self, sim_nb: int):
        self.sim_nb = sim_nb
        pump_id =self.index["hydro_power_plant"].filter(c("type") == "pump")["H"].to_list()
        if sim_nb == self.sim_tot:
            self.powered_volume_enabled = False
        self.generate_state_index()
        self.generate_model_instance()
        # _ = self.solver.solve(self.model_instance)
        # self.optimization_results = process_second_stage_results(
        #     model_instance=self.model_instance, optimization_results=self.optimization_results, pump_id=pump_id, sim_nb=self.sim_nb)

    def log_mip_gap(self, solution):
        self.log_book = pl.concat([
            self.log_book,
            pl.DataFrame(
                {
                    "sim_nb": [self.sim_nb],
                    "lower_bound": [solution["Problem"][0]["Lower bound"]],
                    "upper_bound": [solution["Problem"][0]["Upper bound"]]
                }
            )
        ], how="diagonal_relaxed")   
