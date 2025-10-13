from typing import Optional
from polars import col as c
import polars as pl
import pyomo.environ as pyo
from pyomo.environ import TransformationFactory
import tqdm

from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log

# from pipelines.data_manager.stochastic_data_manager import StochasticDataManager

from optimization_model.stochastic_first_stage.model import stochastic_first_stage_model


log = generate_log(name=__name__)


class StochasticFirstStage(HydroDataManager):
    def __init__(
        self,
        data_config: StochasticConfig,
        smallflex_input_schema: SmallflexInputSchema,
        hydro_power_mask: Optional[pl.Expr] = None,
    ):
        # Retrieve attributes from pipeline_data_manager
        super().__init__(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
        )
        self.data_config = data_config
        self.model: pyo.AbstractModel = stochastic_first_stage_model()
        self.model_instance: pyo.ConcreteModel
        self.scaled_model_instance: pyo.ConcreteModel
        self.model_scaler = TransformationFactory('core.scale_model')


    def create_model_instance(self):
        data: dict = {}
        # index
        data["T"] = {None: self.first_stage_timestep_index["T"].to_list()}
        data["H"] = {None: self.first_stage_hydro_power_state["H"].to_list()}
        data["B"] = {None: self.water_basin["B"].to_list()}
        data["DH"] = {
            None: self.hydro_power_plant.filter(c("control") == "discrete")["H"].to_list()
        }
        data["Ω"] = {None: self.scenario_list}
        data["HB"] = {
            None: list(map(tuple, self.first_stage_hydro_power_state["HB"].to_list()))
        }
        data["nb_hours"] = pl_to_dict(self.first_stage_timestep_index[["T", "n_index"]])

        # Water basin
        data["start_basin_volume"] = pl_to_dict(self.water_basin[["B", "start_volume"]])
        data["water_factor"] = pl_to_dict_with_tuple(
            self.water_flow_factor["BH", "water_factor"]
        )
        data["spilled_factor"] = pl_to_dict(
            self.basin_spilled_factor["B", "spilled_factor"]
        )

        data["min_basin_volume"] = pl_to_dict(
            self.first_stage_basin_state.select("B", "volume_min")
        )
        data["max_basin_volume"] = pl_to_dict(
            self.first_stage_basin_state.select("B", "volume_max")
        )
        # Hydro power plant
        data["max_flow"] = pl_to_dict(
            self.first_stage_hydro_power_state.select("H", "flow")
        )
        data["alpha"] = pl_to_dict(
            self.first_stage_hydro_power_state.select("H", "alpha")
        )

        data["total_positive_flex_power"] = {
            None: self.first_stage_hydro_flex_power["total_positive_flex_power"][0]
        }
        data["total_negative_flex_power"] = {
            None: self.first_stage_hydro_flex_power["total_negative_flex_power"][0]
        }

        # Timeseries
        data["discharge_volume"] = pl_to_dict_with_tuple(
            self.first_stage_discharge_volume[["TΩB", "discharge_volume"]]
        )
        data["market_price"] = pl_to_dict_with_tuple(
            self.first_stage_market_price[["TΩ", "market_price"]]
        )
        data["ancillary_market_price"] = pl_to_dict_with_tuple(
            self.first_stage_ancillary_market_price[["TΩ", "market_price"]]
        )

        data["unpowered_factor_price_pos"] = pl_to_dict_with_tuple(
            self.unpowered_factor_price["ΩB", "unpowered_factor_price_pos"]
        )
        data["unpowered_factor_price_neg"] = pl_to_dict_with_tuple(
            self.unpowered_factor_price["ΩB", "unpowered_factor_price_neg"]
        )

        # Configuration parameters
        data["max_powered_flow_ratio"] = {None: self.first_stage_max_powered_flow_ratio}
        

        self.model_instance = self.model.create_instance({None: data})  # type: ignore
    
    # def model_scaling(self):

    #     self.model_instance.scaling_factor = pyo.Suffix(direction=pyo.Suffix.EXPORT)
    #     # Scale volumes
    #     self.model_instance.scaling_factor[self.model_instance.basin_volume] = self.volume_factor
    #     self.model_instance.scaling_factor[self.model_instance.spilled_volume] = self.volume_factor
    #     self.model_instance.scaling_factor[self.model_instance.end_basin_volume_overage] = self.volume_factor
    #     self.model_instance.scaling_factor[self.model_instance.end_basin_volume_shortage] = self.volume_factor
        
    #     self.scaled_model_instance = self.model_scaler.create_using(self.model_instance) # type: ignore
        
    def results_propagation(self):
        self.model_scaler.propagate_solution(self.scaled_model_instance, self.model_instance) # type: ignore

    def solve_model(self):
        with tqdm.tqdm(
            total=2, desc="Instantiating first stage optimization problem", ncols=150
        ) as pbar:
            self.create_model_instance()
            pbar.update()
            pbar.set_description("Solving first stage optimization problem")
            _ = self.first_stage_solver.solve(self.model_instance, tee=self.verbose)
            pbar.update()
    
            
    # def solve_scaled_model(self):
    #     with tqdm.tqdm(
    #         total=4, desc="Instantiating first stage optimization problem", ncols=150
    #     ) as pbar:
    #         self.create_model_instance()
    #         pbar.set_description("Scaling first stage optimization problem")
    #         pbar.update()
    #         self.model_scaling()
    #         pbar.set_description("Solving first stage optimization problem")
    #         pbar.update()
    #         _ = self.first_stage_solver.solve(self.scaled_model_instance, tee=self.verbose)
    #         pbar.set_description("Propagating first stage optimization results")
    #         pbar.update()
    #         self.results_propagation()
    #         pbar.update()