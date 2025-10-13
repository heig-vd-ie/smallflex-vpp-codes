import polars as pl
import polars.selectors as cs
from polars import col as c
import pyomo.environ as pyo
from pyparsing import Opt
from typing import Optional
import tqdm

from smallflex_data_schema import SmallflexInputSchema
from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log

from pipelines.data_configs import DeterministicConfig
from pipelines.data_manager import HydroDataManager


from optimization_model.deterministic_first_stage.model import (
    deterministic_first_stage_model,
)


log = generate_log(name=__name__)


class DeterministicFirstStage(HydroDataManager):
    def __init__(
        self,
        data_config: DeterministicConfig,
        smallflex_input_schema: SmallflexInputSchema,
        hydro_power_mask: Optional[pl.Expr] = None,
    ):

        super().__init__(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
        )

        self.data_config = data_config
        self.model: pyo.AbstractModel = deterministic_first_stage_model()
        self.timeseries: pl.DataFrame
        self.discharge_volume: pl.DataFrame
        self.model_instance: pyo.ConcreteModel

    def create_model_instance(self):
        data: dict = {}
        # index

        data["H"] = {None: self.first_stage_hydro_power_state["H"].to_list()}
        data["B"] = {None: self.water_basin["B"].to_list()}
        data["DH"] = {
            None: self.hydro_power_plant.filter(c("control") == "discrete")[
                "H"
            ].to_list()
        }

        data["S_B"] = pl_to_dict(
            self.first_stage_basin_state.group_by("B", maintain_order=True).agg("S")
        )
        data["S_H"] = pl_to_dict(
            self.first_stage_hydro_power_state.drop_nulls("H")
            .group_by("H", maintain_order=True)
            .agg("S")
        )
        data["HBS"] = {
            None: list(
                map(
                    tuple,
                    self.first_stage_hydro_power_state.drop_nulls("H")["HBS"].to_list(),
                )
            )
        }

        data["start_basin_volume"] = {}
        for water_basin_data in self.water_basin.to_dicts():
            if (
                water_basin_data["B"]
                in self.data_config.start_basin_volume_ratio.keys()
            ):
                data["start_basin_volume"][water_basin_data["B"]] = (
                    water_basin_data["volume_max"]
                    * self.data_config.start_basin_volume_ratio[water_basin_data["B"]]
                )
            else:
                data["start_basin_volume"][water_basin_data["B"]] = water_basin_data[
                    "start_volume"
                ]

        data["water_factor"] = pl_to_dict_with_tuple(
            self.water_flow_factor["BH", "water_factor"]
        )
        data["spilled_factor"] = pl_to_dict(
            self.basin_spilled_factor["B", "spilled_factor"]
        )

        data["min_basin_volume"] = pl_to_dict_with_tuple(
            self.first_stage_basin_state.select("BS", "volume_min")
        )
        data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.first_stage_basin_state.select("BS", "volume_max")
        )
        # Hydro power plant
        data["max_flow"] = pl_to_dict_with_tuple(
            self.first_stage_hydro_power_state.select("HS", "flow")
        )
        data["alpha"] = pl_to_dict_with_tuple(
            self.first_stage_hydro_power_state.select("HS", "alpha")
        )

        data["total_positive_flex_power"] = pl_to_dict(
            self.first_stage_hydro_flex_power["S", "total_positive_flex_power"]
        )
        data["total_negative_flex_power"] = pl_to_dict(
            self.first_stage_hydro_flex_power["S", "total_negative_flex_power"]
        )

        data["max_powered_flow_ratio"] = {
            None: self.data_config.first_stage_max_powered_flow_ratio
        }
        # Timeseries

        data["T"] = {None: self.timeseries["T"].to_list()}
        data["nb_hours"] = pl_to_dict(self.timeseries[["T", "nb_hours"]])

        

        data["discharge_volume"] = pl_to_dict_with_tuple(
            self.discharge_volume[["TB", "discharge_volume"]]
        )
        data["market_price"] = pl_to_dict(self.timeseries[["T", "market_price"]])
        data["ancillary_market_price"] = pl_to_dict(
            self.timeseries[["T", "ancillary_market_price"]]
        )

        self.model_instance = self.model.create_instance({None: data})  # type: ignore

    def set_timeseries(self, timeseries: pl.DataFrame):
        self.timeseries = (
            timeseries.group_by_dynamic(
                index_column="timestamp", start_by="datapoint", every=self.data_config.first_stage_timestep, 
                closed="left"
            ).agg(
                cs.starts_with("discharge_volume").sum(),
                cs.contains("market_price").mean(),
                c("timestamp").count().alias("nb_hours"),
            ).sort("timestamp").with_row_index("T")
        )
        
        self.discharge_volume = (
            self.timeseries.unpivot(
                on=cs.starts_with("discharge_volume"),
                index="T",
                variable_name="B",
                value_name="discharge_volume",
            )
            .filter(~c("B").str.contains("forecast"))
            .with_columns(
                c("B").str.replace("discharge_volume_", "").cast(pl.UInt32).alias("B")
            ).with_columns(
                pl.concat_list(["T","B"]).alias("TB")
            )
        )

    def solve_model(self):

        with tqdm.tqdm(
            total=1,
            desc="Solving first stage optimization problem",
            ncols=150,
            position=0,
            leave=False,
        ) as pbar:
            self.create_model_instance()
            _ = self.data_config.first_stage_solver.solve(
                self.model_instance, tee=self.data_config.verbose
            )
            pbar.update()
