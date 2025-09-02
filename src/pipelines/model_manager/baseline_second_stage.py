from typing import Optional
from collections import Counter
import logging
import polars as pl
from polars import col as c
import pyomo.environ as pyo
import tqdm

from utility.data_preprocessing import (
    generate_hydro_power_state, generate_basin_state
)
from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log

from pipelines.data_manager import PipelineDataManager
from optimization_model.baseline.second_stage import second_stage_baseline_model


log = generate_log(name=__name__)

class BaselineSecondStage(PipelineDataManager):
    def __init__(
        self, pipeline_data_manager: PipelineDataManager, powered_volume_quota: pl.DataFrame
        ):
        # Retrieve attributes from pipeline_data_manager
        for key, value in vars(pipeline_data_manager).items():
            setattr(self, key, value)
        
        
        self.powered_volume_quota = powered_volume_quota
        self.model: pyo.AbstractModel = second_stage_baseline_model()
        self.model_instances: dict[int, pyo.ConcreteModel] = {}
        self.infeasible_increment = 0
        self.sim_idx: int = 0
        self.data: dict = {}
        self.hydro_flex_power: dict[str, float] = {}
        self.sim_basin_state: pl.DataFrame
        self.sim_hydro_power_state: pl.DataFrame
        self.sim_start_basin_volume: dict[int, float] = pl_to_dict(self.water_basin["B", "start_volume"])
        self.powered_volume_overage: dict[int, float] = pl_to_dict(self.hydro_power_plant.select("H", pl.lit(0)))
        self.powered_volume_shortage: dict[int, float] = pl_to_dict(self.hydro_power_plant.select("H", pl.lit(0)))
        self.generate_constant_parameters()
        self.non_optimal_solution_idx: list[int] = []
        self.unfeasible_solution: list[int] = []

    def generate_constant_parameters(self):
        
        self.data["H"] = {None: self.hydro_power_plant["H"].to_list()}
        self.data["DH"] = {None: self.hydro_power_plant.filter(c("control") == "discrete")["H"].to_list()}
        self.data["B"] = {None: self.water_basin["B"].to_list()}
        self.data["buffer"] = {None: self.volume_buffer}
        self.data["water_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "water_factor"])
        self.data["volume_factor"] = {None: self.volume_factor}
        self.data["spilled_factor"] = pl_to_dict(self.basin_spilled_factor["B", "spilled_factor"])

    def generate_model_instance(self):
    
        self.data["T"] = {
            None: self.second_stage_timestep_index.filter(c("sim_idx") == self.sim_idx)["T"].to_list()}
        self.data["F"] = {
            None: self.second_stage_timestep_index.filter(c("sim_idx") == self.sim_idx)["F"].unique().sort().to_list()}
        self.data["TF"] = {
            None: list(map(tuple, self.second_stage_timestep_index.filter(c("sim_idx") == self.sim_idx)["TF"].to_list()))}
        self.data["S_B"] = pl_to_dict(
            self.sim_basin_state
            .group_by("B", maintain_order=True).agg("S")
            .with_columns(c("S").list.sort())
        )
        self.data["S_H"] = pl_to_dict(
            self.sim_hydro_power_state
            .group_by("H", maintain_order=True)
            .agg("S")
            .with_columns(c("S").list.sort())
            )

        self.data["HBS"] = {None: 
            list(map(
                tuple, 
                self.sim_hydro_power_state\
                    .drop_nulls("H")["HBS"].to_list()
            ))}
        
        self.data["start_basin_volume"] = self.sim_start_basin_volume
        
        self.data["total_positive_flex_power"] = {None: self.hydro_flex_power["total_positive_flex_power"]}
        self.data["total_negative_flex_power"] = {None: self.hydro_flex_power["total_negative_flex_power"]}
        

        self.data["min_basin_volume"] = pl_to_dict_with_tuple(
                    self.sim_basin_state["BS", "volume_min"])
        self.data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.sim_basin_state["BS", "volume_max"])

        self.data["discharge_volume"] = pl_to_dict_with_tuple(
            self.second_stage_discharge_volume.filter(c("sim_idx") == self.sim_idx)[["TB", "discharge_volume"]]
        )  
        self.data["market_price"] = pl_to_dict(
            self.second_stage_market_price.filter(c("sim_idx") == self.sim_idx)[["T", "avg"]]
        )
        self.data["ancillary_market_price"] = pl_to_dict(
            self.second_stage_ancillary_market_price.filter(c("sim_idx") == self.sim_idx)[["F", "avg"]])
        
        powered_volume_quota = self.powered_volume_quota.filter(c("sim_idx") == self.sim_idx)\
            .with_columns(
            (
                c("powered_volume") + 
                c("H").replace_strict(self.powered_volume_shortage, default=0) - 
                c("H").replace_strict(self.powered_volume_overage, default=0)
            ).alias("powered_volume")
        )
        self.data["powered_volume"] = pl_to_dict(powered_volume_quota[["H", "powered_volume"]])
        self.data["shortage_volume_buffer"] = dict(
            Counter(self.volume_buffer) + Counter(dict(map(lambda x: (x[0], x[1]/3), self.powered_volume_shortage.items())))
        ) 
        self.data["overage_volume_buffer"] = dict(
            Counter(self.volume_buffer) + Counter(dict(map(lambda x: (x[0], x[1]/3), self.powered_volume_overage.items())))
            ) 

        self.data["max_flow"] = pl_to_dict_with_tuple(self.sim_hydro_power_state["HS", "flow"])  
        self.data["alpha"] = pl_to_dict_with_tuple(self.sim_hydro_power_state["HS", "alpha"])  
        
        unpowered_factor_price = self.sim_hydro_power_state.group_by("H").agg(
            c("alpha").max().alias("alpha_max"),
            c("alpha").min().alias("alpha_min"),
        ).with_columns(
            pl.when(c("alpha_max")>0).then(c("alpha_max") * self.neg_unpowered_price).otherwise(c("alpha_max") * self.pos_unpowered_price).alias("negative"),
            pl.when(c("alpha_min")>0).then(c("alpha_min")* self.pos_unpowered_price).otherwise(c("alpha_min")* self.neg_unpowered_price).alias("positive"),
        )


        self.data["unpowered_factor_price_pos"] = pl_to_dict(unpowered_factor_price["H", "positive"])
        self.data["unpowered_factor_price_neg"] = pl_to_dict(unpowered_factor_price["H", "negative"])

        self.model_instances[self.sim_idx] = self.model.create_instance({None: self.data}) # type: ignore
    
    def calculate_second_stage_states(self):

        basin_volume_boundaries = self.calculate_basin_volume_boundaries(
            sim_idx=self.sim_idx, start_volume_dict=self.sim_start_basin_volume)
        
        second_stage_basin_state= pl.DataFrame()

        for data in basin_volume_boundaries.to_dicts():
            if data["B"] in self.nb_state_dict.keys():
                nb_state = self.nb_state_dict[data["B"]] 
            else:
                nb_state = data["n_state_min"]

            dt = (data["boundaries"][1] - data["boundaries"][0])/max(1, nb_state - 2)

            data["boundaries"][1] += dt
            data["boundaries"][0] -= dt

            new_basin_state: pl.DataFrame = generate_basin_state(
                basin_volume_table=self.basin_volume_table.filter(c("B") == data["B"]), 
                nb_state=nb_state, boundaries=data["boundaries"])

            second_stage_basin_state = pl.concat([
                second_stage_basin_state, 
                new_basin_state.with_columns(pl.lit(data["B"]).alias("B"))
            ], how="diagonal_relaxed")

        second_stage_basin_state = pl.concat([
                second_stage_basin_state, 
                self.water_basin.filter(~c("B").is_in(self.basin_volume_table["B"]))["B", "volume_max", "volume_min"]
            ], how="diagonal_relaxed")

        self.sim_basin_state = second_stage_basin_state.with_row_index(name="S").with_columns(
                pl.concat_list("B", "S").alias("BS")
                )

        self.sim_hydro_power_state = generate_hydro_power_state(
                power_performance_table=self.power_performance_table, basin_state=self.sim_basin_state)
        
        self.hydro_flex_power: dict[str, float] = self.sim_hydro_power_state\
            .filter(~c("H").is_in(self.data["DH"][None]))\
            .group_by("H")\
            .agg(
                c("power").filter(c("power")>0).min().alias("total_positive_flex_power"),
                (-c("power").filter(c("power")<0).max()).alias("total_negative_flex_power")
            ).sum().to_dicts()[0]
    
    def calculate_basin_volume_boundaries(self, sim_idx: int, start_volume_dict: dict[int, float]):
        
        hydro_power_min_volume = self.hydro_power_plant.select(
            "H", c("upstream_B").alias("B"), 
            (c("rated_flow") * self.second_stage_sim_horizon.total_seconds() * 
            self.volume_factor * self.second_stage_min_volume_ratio).alias("min_volume")
        )
        water_factor = self.water_flow_factor.with_columns(
            c("BH").cast(pl.List(pl.Utf8)).list.join("#")
        )[["BH", "water_factor"]]

        
        discharge_volume_tot= pl_to_dict(
            self.second_stage_discharge_volume
            .filter(c("sim_idx") == sim_idx)
            .group_by("B").agg(c("discharge_volume").sum())
            )

        powered_volume_quota = self.powered_volume_quota.filter(c("sim_idx") == self.sim_idx)\
            .join(hydro_power_min_volume, on="H", how="left")\
            .with_columns(
                pl.concat_list("B", "H").cast(pl.List(pl.Utf8)).list.join("#").alias("BH")  
                ).join(water_factor, on="BH", how="left")\
            .with_columns(
                pl.max_horizontal("powered_volume","min_volume") 
            ).group_by("B").agg(
                c("powered_volume").filter(c("water_factor") > 0).sum().alias("powered_volume_in"),
                c("powered_volume").filter(c("water_factor") < 0).sum().alias("powered_volume_out")
            ).with_columns(
                c("B").replace_strict(start_volume_dict, default=None).alias("start_volume"),
                c("B").replace_strict(discharge_volume_tot, default=0.0).alias("discharge_volume")
            ).sort("B")


        basin_volume_boundaries = powered_volume_quota.with_columns(
            (c("start_volume") + c("powered_volume_in") + c("discharge_volume")).alias("max_volume"),
            (c("start_volume") -  c("powered_volume_out")).alias("min_volume")
        ).with_columns(
            pl.concat_list("min_volume", "max_volume").alias("boundaries")
        )[["B", "boundaries"]]

        basin_volume_boundaries = basin_volume_boundaries.join(
            self.water_basin["B", "volume_max", "volume_min", "n_state_min"], on="B", how="left"
        )
    
        return basin_volume_boundaries

    def solve_every_models(self, nb_sim_tot: Optional[int] = None):
        logging.getLogger('pyomo.core').setLevel(logging.ERROR)
        if not nb_sim_tot:
            nb_sim_tot = self.second_stage_nb_sim
        for self.sim_idx in tqdm.tqdm(
            range(self.second_stage_nb_sim + 1), 
            desc="Solving second stage optimization problem", ncols=150
        ):
            self.infeasible_increment = 0
            self.calculate_second_stage_states()
            self.generate_model_instance()
            
            self.solve_model()

            self.sim_start_basin_volume = self.model_instances[self.sim_idx].end_basin_volume.extract_values() # type: ignore
            self.powered_volume_shortage = self.model_instances[self.sim_idx].powered_volume_shortage.extract_values() # type: ignore
            self.powered_volume_overage = self.model_instances[self.sim_idx].powered_volume_overage.extract_values() # type: ignore

    def solve_model(self):
        
        solution = self.second_stage_solver.solve(self.model_instances[self.sim_idx], tee=self.verbose)
        
        if solution["Solver"][0]["Status"] == "aborted":
            self.non_optimal_solution_idx.append(self.sim_idx)
        elif solution["Solver"][0]["Termination condition"] == "infeasibleOrUnbounded":
            if self.infeasible_increment == 3:
                raise ValueError('Infeasible model')
            else:
                self.infeasible_increment += 1
                self.data["shortage_volume_buffer"] = dict(map(lambda x: (x[0], x[1]*2), self.data["shortage_volume_buffer"].items()))
                self.data["overage_volume_buffer"] = dict(map(lambda x: (x[0], x[1]*2), self.data["overage_volume_buffer"].items()))
                self.model_instances[self.sim_idx] = self.model.create_instance({None: self.data}) # type: ignore
                self.unfeasible_solution.append(self.sim_idx)
                self.solve_model()

                
    
    
