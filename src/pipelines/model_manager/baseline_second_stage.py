
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
    generate_hydro_power_state, generate_basin_state
)
from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log
from pipelines.data_configs import PipelineConfig
from pipelines.data_manager import PipelineDataManager

from pipelines.baseline_model.second_stage import second_stage_baseline_model


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

        self.sim_idx: int = 0
        self.data: dict = {}
        self.sim_basin_state: pl.DataFrame
        self.sim_hydro_power_state: pl.DataFrame
        self.sim_start_basin_volume: dict[int, float] = pl_to_dict(self.water_basin["B", "start_volume"])
        self.sim_remaining_volume_neg: dict[int, float] = pl_to_dict(self.hydro_power_plant.select("H", pl.lit(0)))
        self.sim_remaining_volume_pos: dict[int, float] = pl_to_dict(self.hydro_power_plant.select("H", pl.lit(0)))
        self.generate_constant_parameters()

    def generate_constant_parameters(self):
        
        self.data["H"] = {None: self.hydro_power_plant["H"].to_list()}
        self.data["DH"] = {None: self.hydro_power_plant.filter(c("control") == "discrete")["H"].to_list()}
        self.data["B"] = {None: self.water_basin["B"].to_list()}
        self.data["buffer"] = {None: self.volume_buffer}
        self.data["water_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "water_factor"])
        self.data["volume_factor"] = {None: self.volume_factor}
        self.data["spilled_factor"] = pl_to_dict(self.basin_spilled_factor["B", "spilled_factor"])
        # self.data["neg_unpowered_price"] = {None: self.neg_unpowered_price}
        # self.data["pos_unpowered_price"] = {None: self.pos_unpowered_price}


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
        self.data["sim_remaining_volume_neg"] = self.sim_remaining_volume_neg
        self.data["sim_remaining_volume_pos"] = self.sim_remaining_volume_pos

        self.data["min_basin_volume"] = pl_to_dict_with_tuple(
                    self.sim_basin_state["BS", "volume_min"])
        self.data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.sim_basin_state["BS", "volume_max"])
        # self.data["powered_volume_enabled"] = {None: self.powered_volume_enabled}

        self.data["discharge_volume"] = pl_to_dict_with_tuple(
            self.second_stage_discharge_volume.filter(c("sim_idx") == self.sim_idx)[["TB", "discharge_volume"]]
        )  
        self.data["market_price"] = pl_to_dict(
            self.second_stage_market_price.filter(c("sim_idx") == self.sim_idx)[["T", "avg"]]
        )
        self.data["ancillary_market_price"] = pl_to_dict(
            self.second_stage_ancillary_market_price.filter(c("sim_idx") == self.sim_idx)[["F", "avg"]])

        self.data["powered_volume"] = pl_to_dict(
            self.powered_volume_quota.filter(c("sim_idx") == self.sim_idx)[["H", "powered_volume"]])

        self.data["volume_buffer"] = self.volume_buffer

        self.data["max_flow"] = pl_to_dict_with_tuple(self.sim_hydro_power_state["HS", "flow"])  
        self.data["alpha"] = pl_to_dict_with_tuple(self.sim_hydro_power_state["HS", "alpha"])  
        
        self.data["max_power"] = {0: 7}
        unpowered_factor_price = self.sim_hydro_power_state.group_by("H").agg(
            c("alpha").max().alias("alpha_max"),
            c("alpha").min().alias("alpha_min"),
        ).with_columns(
            pl.when(c("alpha_max")>0).then(c("alpha_max") * self.pos_unpowered_price).otherwise(c("alpha_max") * self.neg_unpowered_price).alias("positive"),
            pl.when(c("alpha_min")>0).then(c("alpha_min")* self.neg_unpowered_price).otherwise(c("alpha_min")* self.pos_unpowered_price).alias("negative"),
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

    
        
    def solve_model(self):
        for self.sim_idx in tqdm.tqdm(range(
            # self.second_stage_nb_sim
            2
            ), 
            desc="Solving second stage optimization problem", ncols=150
        ):

            self.calculate_second_stage_states()
            self.generate_model_instance()
            
            _ = self.second_stage_solver.solve(self.model_instances[self.sim_idx], tee=self.verbose)

            self.sim_start_basin_volume = self.model_instances[self.sim_idx].end_basin_volume.extract_values() # type: ignore
            self.sim_remaining_volume_neg = self.model_instances[self.sim_idx].diff_volume_neg.extract_values() # type: ignore
            self.sim_remaining_volume_pos = self.model_instances[self.sim_idx].diff_volume_pos.extract_values() # type: ignore
            # pbar.update()
            
    
    
