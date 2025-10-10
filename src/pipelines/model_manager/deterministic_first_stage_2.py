
import polars as pl
import polars.selectors as cs
from polars import col as c
import pyomo.environ as pyo
import tqdm

from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log

from pipelines.data_manager import SecondStageStochasticDataManager

from optimization_model.deterministic_first_stage.model import deterministic_first_stage_model


log = generate_log(name=__name__)

class DeterministicFirstStage(SecondStageStochasticDataManager):
    def __init__(
        self, 
        pipeline_data_manager: SecondStageStochasticDataManager,
        ):
        # Retrieve attributes from pipeline_data_manager
        for key, value in vars(pipeline_data_manager).items():
            setattr(self, key, value)
            
        self.model: pyo.AbstractModel = deterministic_first_stage_model()
        self.model_instance: pyo.ConcreteModel
    
    def create_model_instance(self):
        data: dict = {}
        # index
        data["T"] = {None: self.first_stage_timeseries["T"].to_list()}
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
        data["HBS"] = {None: 
            list(map(
                tuple, 
                self.first_stage_hydro_power_state\
                    .drop_nulls("H")["HBS"].to_list()
            ))}
        data["nb_hours"] = pl_to_dict(self.first_stage_timeseries[["T", "n_index"]])
        data["start_basin_volume"] = {}
        for water_basin_data in self.water_basin.to_dicts():
            if water_basin_data["B"] in self.start_basin_volume_ratio.keys():
                data["start_basin_volume"][water_basin_data["B"]]  = water_basin_data["volume_max"] * self.start_basin_volume_ratio[water_basin_data["B"]]
            else:
                data["start_basin_volume"][water_basin_data["B"]] = water_basin_data["start_volume"]
                
                
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
    
        data["total_positive_flex_power"] = pl_to_dict(self.first_stage_hydro_flex_power["S", "total_positive_flex_power"])
        data["total_negative_flex_power"] = pl_to_dict(self.first_stage_hydro_flex_power["S", "total_negative_flex_power"])
        
        # Timeseries
        discharge_volume = self.first_stage_timeseries.unpivot(
            on=cs.starts_with("discharge_volume"), index="T", variable_name="B", value_name="discharge_volume"
        ).filter(~c("B").str.contains("forecast")).with_columns(
            pl.concat_list(["T", c("B").str.replace("discharge_volume_", "").cast(pl.UInt32)]).alias("TB")
        )

        data["discharge_volume"] = pl_to_dict_with_tuple(discharge_volume[["TB", "discharge_volume"]])
        data["market_price"] = pl_to_dict(self.first_stage_timeseries[["T", "market_price"]])
        data["ancillary_market_price"] = pl_to_dict(self.first_stage_timeseries[["T", "ancillary_market_price"]])

        # Configuration parameters
        data["max_powered_flow_ratio"] = {None: self.first_stage_max_powered_flow_ratio}

        self.model_instance = self.model.create_instance({None: data}) # type: ignore

    def solve_model(self):
        with tqdm.tqdm(
            total=1, desc="Solving first stage optimization problem", ncols=150,
            position=0, leave=False
        ) as pbar:
            self.create_model_instance()
            _ = self.first_stage_solver.solve(self.model_instance, tee=self.verbose)
            pbar.update()
