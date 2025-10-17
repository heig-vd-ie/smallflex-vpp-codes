from typing import Optional
from collections import Counter
import logging
import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo
import tqdm

from optimization_model import deterministic_second_stage_old

from smallflex_data_schema import SmallflexInputSchema
from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log


from pipelines.data_configs import DataConfig
from pipelines.data_manager import HydroDataManager


from optimization_model.deterministic_second_stage_old.model import (
    deterministic_second_stage_model_with_battery, deterministic_second_stage_model_without_battery)
from utility.data_preprocessing import (
    generate_hydro_power_state, generate_basin_state, split_timestamps_per_sim
)


log = generate_log(name=__name__)

class DeterministicSecondStage(HydroDataManager):
    def __init__(
        self,
        data_config: DataConfig,
        smallflex_input_schema: SmallflexInputSchema,
        powered_volume_quota: pl.DataFrame,
        hydro_power_mask: Optional[pl.Expr] = None,
        
    ):
    
        super().__init__(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
        )
        self.data_config = data_config
        
        if self.data_config.battery_capacity > 0:
            self.model: pyo.AbstractModel = deterministic_second_stage_model_with_battery()
        else:
            self.model: pyo.AbstractModel = deterministic_second_stage_model_without_battery()
            
        self.model_instances: dict[int, pyo.ConcreteModel] = {}
        self.timeseries: pl.DataFrame
        self.discharge_volume: pl.DataFrame
        
        self.infeasible_increment = 0
        self.sim_idx: int = 0
        self.nb_sims: int
        self.neg_unpowered_price: float
        self.pos_unpowered_price: float
        self.data: dict = {}
        self.hydro_flex_power: dict[str, float] = {}
        self.sim_basin_state: pl.DataFrame
        self.sim_hydro_power_state: pl.DataFrame
        
        self.powered_volume_quota: pl.DataFrame = powered_volume_quota
        self.sim_start_basin_volume: dict[int, float] = pl_to_dict(self.water_basin["B", "start_volume"])
        self.powered_volume_overage: dict[int, float] = pl_to_dict(self.hydro_power_plant.select("H", pl.lit(0)))
        self.powered_volume_shortage: dict[int, float] = pl_to_dict(self.hydro_power_plant.select("H", pl.lit(0)))
        self.sim_start_battery_soc: float = self.data_config.start_battery_soc
        self.non_optimal_solution_idx: list[int] = []
        self.unfeasible_solution: list[int] = []



    def set_timeseries(self, timeseries: pl.DataFrame):
        self.timeseries = (
            split_timestamps_per_sim(
                data=timeseries.sort("timestamp").with_row_index(name="T"),
                divisors=self.data_config.second_stage_nb_timestamp,
            ).with_columns(
                (c("T")//self.data_config.nb_timestamp_per_ancillary).alias("F")
            ).with_columns(
                pl.concat_list("T","F").alias("TF")
            )
        )
        
        self.neg_unpowered_price = self.timeseries["market_price"].quantile(0.5 + self.data_config.second_stage_quantile) # type: ignore
        self.pos_unpowered_price = self.timeseries["market_price"].quantile(0.5 - self.data_config.second_stage_quantile) # type: ignore

        self.nb_sims = self.timeseries["sim_idx"].max() # type: ignore
        
        self.discharge_volume = self.timeseries.unpivot(
                on=cs.starts_with("discharge_volume"),
                index=["T", "sim_idx"],
                variable_name="B",
                value_name="discharge_volume",
            ).filter(~c("B").str.contains("forecast"))\
            .with_columns(
                c("B").str.replace("discharge_volume_", "").cast(pl.UInt32)
            ).with_columns(
                pl.concat_list(["T", "B"]).alias("TB")
            )


    def generate_constant_parameters(self):
        
        self.data["H"] = {None: self.hydro_power_plant["H"].to_list()}
        self.data["DH"] = {None: self.hydro_power_plant.filter(c("control") == "discrete")["H"].to_list()}
        self.data["B"] = {None: self.water_basin["B"].to_list()}
        self.data["nb_timestamp_per_ancillary"] = {None: self.data_config.nb_timestamp_per_ancillary}
        self.data["water_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "water_factor"])
        self.data["spilled_factor"] = pl_to_dict(self.basin_spilled_factor["B", "spilled_factor"])
        self.data["battery_capacity"] = {None: self.data_config.battery_capacity}
        self.data["battery_rated_power"] = {None: self.data_config.battery_rated_power}
        self.data["battery_efficiency"] = {None: self.data_config.battery_efficiency}
        self.data["pos_unpowered_price"] = {None: self.pos_unpowered_price}
        self.data["neg_unpowered_price"] = {None: self.neg_unpowered_price}

    def generate_model_instance(self):
    

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
        self.data["start_battery_soc"] = {None: self.sim_start_battery_soc}
        
        self.data["total_positive_flex_power"] = {None: self.hydro_flex_power["total_positive_flex_power"]}
        self.data["total_negative_flex_power"] = {None: self.hydro_flex_power["total_negative_flex_power"]}
        

        self.data["min_basin_volume"] = pl_to_dict_with_tuple(
                    self.sim_basin_state["BS", "volume_min"])
        self.data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.sim_basin_state["BS", "volume_max"])
        
        self.data["T"] = {
            None: self.timeseries.filter(c("sim_idx") == self.sim_idx)["T"].to_list()}
        self.data["F"] = {
            None: self.timeseries.filter(c("sim_idx") == self.sim_idx)["F"].unique().sort().to_list()}
        self.data["TF"] = {
            None: list(map(tuple, self.timeseries.filter(c("sim_idx") == self.sim_idx)["TF"].to_list()))}

        self.data["discharge_volume"] = pl_to_dict_with_tuple(
            self.discharge_volume.filter(c("sim_idx") == self.sim_idx)[["TB", "discharge_volume"]]
        ) 
        self.data["market_price"] = pl_to_dict(
            self.timeseries.filter(c("sim_idx") == self.sim_idx)[["T", "market_price"]]
        )
        self.data["pv_power"] = pl_to_dict(
            self.timeseries.filter(c("sim_idx") == self.sim_idx)[["T", "pv_power"]]
        )
        self.data["wind_power"] = pl_to_dict(
            self.timeseries.filter(c("sim_idx") == self.sim_idx)[["T", "wind_power"]]
        )
        self.data["ancillary_market_price"] = pl_to_dict(
            self.timeseries.filter(c("sim_idx") == self.sim_idx).filter(c("F").is_first_distinct())
            [["F", "ancillary_market_price"]]
        )
        
        
        powered_volume_quota = self.powered_volume_quota.filter(c("sim_idx") == self.sim_idx)\
            .with_columns(
            (
                c("powered_volume") + 
                c("H").replace_strict(self.powered_volume_shortage, default=0) - 
                c("H").replace_strict(self.powered_volume_overage, default=0)
            ).alias("powered_volume_quota")
        )
        self.data["powered_volume_quota"] = pl_to_dict(powered_volume_quota[["H", "powered_volume_quota"]])
        self.data["shortage_volume_buffer"] = {}
        self.data["overage_volume_buffer"] = {}
        for data in powered_volume_quota.to_dicts():
            overage_volume_buffer = self.volume_buffer[data["H"]] + self.powered_volume_overage[data["H"]]/3
            self.data["shortage_volume_buffer"][data["H"]] = self.volume_buffer[data["H"]] + self.powered_volume_shortage[data["H"]]/3
            # The overage buffer should never be lower than the negative quota to avoid infeasibility
            self.data["overage_volume_buffer"][data["H"]] = (max(overage_volume_buffer, - 1.1*data["powered_volume_quota"]))
        

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
            if data["B"] in self.data_config.nb_state_dict.keys():
                nb_state = self.data_config.nb_state_dict[data["B"]] 
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
            (c("rated_flow") * self.data_config.second_stage_sim_horizon.total_seconds() * 
            self.data_config.second_stage_min_volume_ratio).alias("min_volume")
        )
        water_factor = self.water_flow_factor.with_columns(
            c("BH").cast(pl.List(pl.Utf8)).list.join("#")
        )[["BH", "water_factor"]]

        
        discharge_volume_tot= pl_to_dict(
            self.discharge_volume
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
        
        self.generate_constant_parameters()
        
        if not nb_sim_tot:
            nb_sim_tot = self.nb_sims
        for self.sim_idx in tqdm.tqdm(
            range(nb_sim_tot + 1),
            desc="Solving second stage optimization problem", ncols=150,
            position=0, leave=False
        ):
            self.infeasible_increment = 0
            self.calculate_second_stage_states()
            self.generate_model_instance()
            
            self.solve_model()

            self.sim_start_basin_volume = self.model_instances[self.sim_idx].end_basin_volume.extract_values() # type: ignore
            self.powered_volume_shortage = self.model_instances[self.sim_idx].powered_volume_shortage.extract_values() # type: ignore
            self.powered_volume_overage = self.model_instances[self.sim_idx].powered_volume_overage.extract_values() # type: ignore
            self.sim_start_battery_soc += (
                self.model_instances[self.sim_idx].end_battery_soc_overage.extract_values()[None] - # type: ignore
                self.model_instances[self.sim_idx].end_battery_soc_shortage.extract_values()[None] # type: ignore
            )

    def solve_model(self):

        solution = self.data_config.second_stage_solver.solve(self.model_instances[self.sim_idx], tee=self.data_config.verbose)

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

                
    
    
