from operator import le
from typing import Optional
from collections import Counter
import logging
import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo
from tqdm.auto import tqdm

from smallflex_data_schema import SmallflexInputSchema
from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log


from pipelines.data_configs import DataConfig
from pipelines.data_manager import HydroDataManager


from optimization_model.deterministic_second_stage.model import deterministic_second_stage_model
from utility.data_preprocessing import (
    generate_hydro_power_state, generate_basin_state, split_timestamps_per_sim
)
from utility.data_preprocessing import (
    split_timestamps_per_sim,
    extract_result_table,
    pivot_result_table,
)

log = generate_log(name=__name__)

class DeterministicSecondStage(HydroDataManager):
    def __init__(
        self,
        data_config: DataConfig,
        smallflex_input_schema: SmallflexInputSchema,
        basin_volume_expectation: pl.DataFrame,
        hydro_power_mask: Optional[pl.Expr] = None,
        
    ):
    
        super().__init__(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
        )
        self.data_config = data_config
        
        self.model: pyo.AbstractModel = deterministic_second_stage_model(
            with_ancillary=data_config.with_ancillary,
            with_battery=self.data_config.battery_capacity > 0
        )
            
        self.model_instances: dict[int, pyo.ConcreteModel] = {}
        self.timeseries: pl.DataFrame
        self.discharge_volume: pl.DataFrame
        self.market_price_quantiles: pl.DataFrame
        
        self.sim_idx: int = 0
        self.nb_sims: int
        self.data: dict = {}
        self.hydro_flex_power: dict[str, float] = {}
        self.sim_basin_state: pl.DataFrame
        self.sim_hydro_power_state: pl.DataFrame
        
        self.basin_volume_expectation: pl.DataFrame = basin_volume_expectation.with_columns(
            (c("T")//data_config.first_stage_nb_timestamp).alias("sim_idx")
        ).sort("sim_idx", "B").unique(subset=["sim_idx", "B"], keep="last")
                
        
        
        self.start_basin_volume: pl.DataFrame = self.water_basin["B", "start_volume"]
        self.sim_start_battery_soc: float = self.data_config.start_battery_soc
        self.non_optimal_solution_idx: list[int] = []
        self.unfeasible_solution: list[int] = []
        self.volume_deviation: pl.DataFrame = self.upstream_water_basin\
            .select("B", pl.lit(0).alias("volume_deviation"))


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
        
        self.nb_sims = self.timeseries["sim_idx"].max() + 1 # type: ignore
        
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
        self.market_price_quantiles = self.timeseries.group_by("sim_idx").agg(
            c("timestamp").first(),
            c("market_price_lower_quantile").mean().alias("market_price_lower_quantile"),
            c("market_price_upper_quantile").mean().alias("market_price_upper_quantile"),
        ).sort("sim_idx")

    def generate_constant_parameters(self):
        
        self.data["H"] = {None: self.hydro_power_plant["H"].to_list()}
        self.data["DH"] = {None: self.hydro_power_plant.filter(c("control") == "discrete")["H"].to_list()}
        self.data["B"] = {None: self.water_basin["B"].to_list()}
        self.data["Q"] = {None: list(range(self.data_config.nb_quantiles))}
        self.data["UP_B"] = {None: self.upstream_water_basin["B"].to_list()}
        self.data["nb_timestamp_per_ancillary"] = {None: self.data_config.nb_timestamp_per_ancillary}
        self.data["water_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "water_factor"])
        self.data["spilled_factor"] = pl_to_dict(
            self.upstream_water_basin.select("B", pl.lit(self.data_config.spilled_factor).alias("spilled_factor")))
        self.data["battery_capacity"] = {None: self.data_config.battery_capacity}
        self.data["battery_rated_power"] = {None: self.data_config.battery_rated_power}
        self.data["battery_efficiency"] = {None: self.data_config.battery_efficiency}

        self.data["basin_volume_range"] = pl_to_dict(
            self.water_basin.select("B", "volume_range")
        )
        self.data["rated_alpha"] = pl_to_dict(
            self.hydro_power_plant.with_columns(
                (c("rated_power")/c("rated_flow")).alias("rated_alpha")
            ).group_by("upstream_B").agg(c("rated_alpha").mean())
        )
        self.data["bound_penalty_factor"] = dict(zip(range(self.data_config.nb_quantiles), self.data_config.bound_penalty_factor))
        
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
        
        self.data["start_basin_volume"] = pl_to_dict(self.start_basin_volume)
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
        
        self.data["max_flow"] = pl_to_dict_with_tuple(self.sim_hydro_power_state["HS", "flow"])  
        self.data["alpha"] = pl_to_dict_with_tuple(self.sim_hydro_power_state["HS", "alpha"])  
        self.data["shortage_market_price"] = {
            None: self.market_price_quantiles.filter(c("sim_idx") == self.sim_idx)["market_price_upper_quantile"][0]}
        self.data["overage_market_price"] = {
            None: self.market_price_quantiles.filter(c("sim_idx") == self.sim_idx)["market_price_lower_quantile"][0]}
        
        actual_volume = self.basin_volume_expectation.filter(c("sim_idx") == self.sim_idx + 1)
        self.data["expected_end_basin_volume"] = pl_to_dict(actual_volume["B", "mean"])
        quantile_value = actual_volume.unpivot(
            on=cs.contains("quantile"),
            index=["B"],
            variable_name="quantile",
            value_name="limit"
        ).with_columns(
            c("quantile").str.split("_quantile_").list.to_struct(fields=["direction", "Q"])
        ).unnest("quantile").with_columns(
            c("Q").cast(pl.Int32)
        )

        for direction in ["upper", "lower"]:
            self.data[f"end_basin_volume_{direction}_limit"] = pl_to_dict_with_tuple(
                quantile_value.filter(c("direction") == direction)
                .select(
                    pl.concat_list("B", "Q"), "limit"
                )
            )


        self.model_instances[self.sim_idx] = self.model.create_instance({None: self.data}) # type: ignore
    
    def calculate_second_stage_states(self):
        volume_bound = self.start_basin_volume.join(
            self.basin_volume_expectation.filter(c("T") == self.sim_idx)["B", "diff_volume"], on="B").with_columns(
            pl.concat_list(c("start_volume"), c("start_volume") + c("diff_volume")).list.sort().alias("volume_bound")
            )
            
        second_stage_basin_state= pl.DataFrame()

        for data in volume_bound.to_dicts():
            if data["B"] in self.data_config.nb_state_dict.keys():
                nb_state = self.data_config.nb_state_dict[data["B"]] 
            else:
                nb_state = self.water_basin.filter(c("B").is_in(data["B"]))["n_state_min"][0]
            
            
            new_basin_state: pl.DataFrame = generate_basin_state(
                basin_volume_table=self.basin_volume_table.filter(c("B") == data["B"]), 
                nb_state=nb_state, boundaries=data["volume_bound"])
            second_stage_basin_state = pl.concat([
                            second_stage_basin_state, 
                            new_basin_state.with_columns(pl.lit(data["B"]).alias("B"))
                        ], how="diagonal_relaxed")

        self.sim_basin_state = second_stage_basin_state.with_row_index(name="S").with_columns(
                        pl.concat_list("B", "S").alias("BS")
                        )

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


    def solve_every_models(self, nb_sim_tot: Optional[int] = None):
        logging.getLogger('pyomo.core').setLevel(logging.ERROR)
        
        self.generate_constant_parameters()
        
        if not nb_sim_tot:
            nb_sim_tot = self.nb_sims
            
        for self.sim_idx in tqdm(
            range(nb_sim_tot),
            desc="Solving second stage optimization problem", 
            position=1,
            leave=False
        ):

            self.calculate_second_stage_states()
            self.generate_model_instance()
        
            solution = self.data_config.second_stage_solver.solve(self.model_instances[self.sim_idx], tee=self.data_config.verbose)

            if solution["Solver"][0]["Status"] == "aborted":
                self.non_optimal_solution_idx.append(self.sim_idx)

            
            self.start_basin_volume = extract_result_table(self.model_instances[self.sim_idx], "end_basin_volume").rename({"end_basin_volume": "start_volume"})
            
            if self.data_config.battery_capacity > 0:
                self.sim_start_battery_soc += (
                    self.model_instances[self.sim_idx].end_battery_soc_overage.extract_values()[None] - # type: ignore
                    self.model_instances[self.sim_idx].end_battery_soc_shortage.extract_values()[None] # type: ignore
                )
            