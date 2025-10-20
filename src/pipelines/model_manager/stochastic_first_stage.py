from typing import Optional
from polars import col as c
from polars import selectors as cs
import polars as pl
import pyomo.environ as pyo

from tqdm.auto import tqdm

from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log
from smallflex_data_schema import SmallflexInputSchema

from pipelines.data_manager import HydroDataManager
from pipelines.data_configs import DataConfig
from optimization_model.stochastic_first_stage.model import stochastic_first_stage_model


log = generate_log(name=__name__)


class StochasticFirstStage(HydroDataManager):
    def __init__(
        self,
        data_config: DataConfig,
        smallflex_input_schema: SmallflexInputSchema,
        hydro_power_mask: Optional[pl.Expr] = None,
    ):
        # Retrieve attributes from pipeline_data_manager
        super().__init__(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
            is_linear=True
        )
        self.data_config = data_config
        self.model: pyo.AbstractModel = stochastic_first_stage_model()
        self.model_instance: pyo.ConcreteModel
        
        self.discharge_volume: pl.DataFrame
        self.timeseries: pl.DataFrame
        self.unpowered_factor_price : pl.DataFrame
                
        self.scenario_list = list(self.data_config.rng.choice(
            smallflex_input_schema.discharge_volume_synthesized["scenario"].unique().to_numpy(), 
            size=self.data_config.nb_scenarios, replace=False
        ))

    def set_timeseries(self, timeseries: pl.DataFrame):
        min_timestamp = timeseries["timestamp"].min()
        self.timeseries = (
            timeseries.sort("timestamp", "Ω").group_by_dynamic(
                index_column="timestamp", start_by="datapoint", 
                every=self.data_config.first_stage_timestep, 
                closed="left", group_by=["Ω"]
            ).agg(
                cs.starts_with("discharge_volume").sum(),
                cs.contains("market_price").mean(),
                (24*c("timestamp").count()).alias("nb_hours"),
            ).sort("timestamp").with_columns(
                ((c("timestamp") -  min_timestamp) / self.data_config.first_stage_timestep).cast(pl.Int64).alias("T")
            ).with_columns(
                pl.concat_list("T", "Ω").alias("TΩ")
            )
        )
        
        self.discharge_volume = (
            self.timeseries.unpivot(
                on=cs.starts_with("discharge_volume"),
                index=["T", "Ω"],
                variable_name="B",
                value_name="discharge_volume",
            )
            .filter(~c("B").str.contains("forecast"))
            .with_columns(
                c("B").str.replace("discharge_volume_", "").cast(pl.UInt32).alias("B")
            ).with_columns(
                pl.concat_list(["T", "Ω", "B"]).alias("TΩB")
            )
        )

    def create_model_instance(self):
        data: dict = {}
        # index
        
        data["H"] = {None: self.first_stage_hydro_power_state["H"].to_list()}
        data["B"] = {None: self.water_basin["B"].to_list()}
        data["UP_B"] = {None: self.upstream_water_basin["B"].to_list()}
        data["DH"] = {
            None: self.hydro_power_plant.filter(c("control") == "discrete")["H"].to_list()
        }
        data["Ω"] = {None: self.scenario_list}
        data["HB"] = {
            None: list(map(tuple, self.first_stage_hydro_power_state["HB"].to_list()))
        }

        # Water basin
        data["start_basin_volume"] = pl_to_dict(self.water_basin[["B", "start_volume"]])
        data["water_factor"] = pl_to_dict_with_tuple(
            self.water_flow_factor["BH", "water_factor"]
        )
        data["spilled_factor"] = pl_to_dict(
            self.upstream_water_basin.select("B", pl.lit(self.data_config.spilled_factor).alias("spilled_factor")))
        
        data["min_basin_volume"] = pl_to_dict(
            self.first_stage_basin_state.select("B", "volume_min")
        )
        data["max_basin_volume"] = pl_to_dict(
            self.first_stage_basin_state.select("B", "volume_max")
        )
        data["basin_volume_range"] = pl_to_dict(
            self.water_basin.select("B", "volume_range")
        )
        # Hydro power plant
        data["max_flow"] = pl_to_dict(
            self.first_stage_hydro_power_state.select("H", "flow")
        )
        data["alpha"] = pl_to_dict(
            self.first_stage_hydro_power_state.select("H", "alpha")
        )

        # Timeseries
        
        data["T"] = {None: self.timeseries["T"].to_list()}
        data["nb_hours"] = pl_to_dict(self.timeseries.filter(c("T").is_first_distinct())[["T", "nb_hours"]])

        data["discharge_volume"] = pl_to_dict_with_tuple(
            self.discharge_volume[["TΩB", "discharge_volume"]]
        )
        data["market_price"] = pl_to_dict_with_tuple(
            self.timeseries[["TΩ", "market_price"]]
        )

        # Configuration parameters
        data["max_powered_flow_ratio"] = {None: self.data_config.first_stage_max_powered_flow_ratio}
        

        self.model_instance = self.model.create_instance({None: data})  # type: ignore
    

    def solve_model(self):
        with tqdm(
            total=2, desc="Instantiating first stage optimization problem", position=1, leave=False
        ) as pbar:
            self.create_model_instance()
            pbar.update()
            pbar.set_description("Solving first stage optimization problem")
            _ = self.data_config.first_stage_solver.solve(self.model_instance, tee=self.data_config.verbose)
            pbar.update()
    
            
