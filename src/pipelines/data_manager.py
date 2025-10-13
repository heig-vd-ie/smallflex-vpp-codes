from typing import Optional
import polars as pl
import polars.selectors as cs
from datetime import timedelta
from polars import col as c

from smallflex_data_schema import SmallflexInputSchema

from utility.data_preprocessing import (
    generate_basin_volume_table,
    clean_hydro_power_performance_table,
    generate_hydro_power_state,
    generate_first_stage_basin_state_table,
)

from general_function import pl_to_dict
from pipelines.data_configs import DeterministicConfig


class HydroDataManager():
    def __init__(
        self,
        data_config: DeterministicConfig,
        smallflex_input_schema: SmallflexInputSchema,
        hydro_power_mask: Optional[pl.Expr] = None,
    ):
        if hydro_power_mask is None:
            hydro_power_mask = pl.lit(True)

        # Index table
        self.hydro_power_plant: pl.DataFrame
        self.water_basin: pl.DataFrame
        # hydro power plant table
        self.basin_volume_table: pl.DataFrame
        self.basin_spilled_factor: pl.DataFrame
        self.power_performance_table: pl.DataFrame
        self.water_flow_factor: pl.DataFrame
        self.first_stage_basin_state: pl.DataFrame
        self.first_stage_hydro_power_state: pl.DataFrame
        self.first_stage_hydro_flex_power: pl.DataFrame
        self.volume_buffer: dict[int, float]

        self.__build_hydro_power_plant_data(
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
            data_config=data_config,
        )

    def __build_hydro_power_plant_data(
        self, smallflex_input_schema: SmallflexInputSchema, hydro_power_mask: pl.Expr, data_config: DeterministicConfig
    ):

        water_volume_mapping = {"upstream_basin_fk": -1, "downstream_basin_fk": 1}

        hydro_type_mapping = {"turbine": 1, "pump": -1}

        self.hydro_power_plant = smallflex_input_schema.hydro_power_plant.filter(
            hydro_power_mask
        ).with_row_index(name="H")

        self.water_basin = smallflex_input_schema.water_basin.filter(
            c("uuid").is_in(
                self.hydro_power_plant["upstream_basin_fk"].to_list()
                + self.hydro_power_plant["downstream_basin_fk"].to_list()
            )
        ).with_row_index(name="B")

        basin_index_mapping = pl_to_dict(self.water_basin[["uuid", "B"]])

        self.hydro_power_plant = self.hydro_power_plant.with_columns(
            c(f"{col}_basin_fk")
            .replace_strict(basin_index_mapping, default=None)
            .alias(f"{col}_B")
            for col in ["upstream", "downstream"]
        )

        water_flow_factor = self.hydro_power_plant.unpivot(
            on=["upstream_basin_fk", "downstream_basin_fk"],
            index=["H", "type"],
            variable_name="basin_type",
            value_name="basin_fk",
        )

        water_flow_factor = water_flow_factor.with_columns(
            c("basin_fk").replace_strict(basin_index_mapping, default=None).alias("B"),
            (
                c("basin_type").replace_strict(water_volume_mapping, default=None)
                * c("type").replace_strict(hydro_type_mapping, default=None)
            ).alias("water_factor"),
        )

        self.basin_spilled_factor = (
            water_flow_factor.filter(c("basin_type") == "upstream_basin_fk")
            .select("B", pl.lit(data_config.spilled_factor).alias("spilled_factor"))
            .unique(subset="B")
        )

        self.water_flow_factor = water_flow_factor.select(
            "B", "H", pl.concat_list(["B", "H"]).alias("BH"), "water_factor"
        )

        self.basin_volume_table = generate_basin_volume_table(
            water_basin=self.water_basin,
            basin_height_volume_table=smallflex_input_schema.basin_height_volume_table,
            d_height=data_config.d_height,
        )

        self.power_performance_table = clean_hydro_power_performance_table(
            hydro_power_plant=self.hydro_power_plant,
            water_basin=self.water_basin,
            hydro_power_performance_table=smallflex_input_schema.hydro_power_performance_table.as_polars(),
            basin_volume_table=self.basin_volume_table,
        )

        self.first_stage_basin_state = generate_first_stage_basin_state_table(
            basin_volume_table=self.basin_volume_table,
            water_basin=self.water_basin,
            nb_state_dict=data_config.nb_state_dict,
        )
        self.first_stage_hydro_power_state = generate_hydro_power_state(
            power_performance_table=self.power_performance_table,
            basin_state=self.first_stage_basin_state,
        )

        self.first_stage_hydro_flex_power = (
            self.first_stage_hydro_power_state.filter(
                c("H").is_in(
                    self.hydro_power_plant.filter(c("control") == "continuous")[
                        "H"
                    ].to_list()
                )
            )
            .group_by("S", maintain_order=True)
            .agg(
                c("power")
                .filter(c("power") > 0)
                .min()
                .fill_null(0)
                .alias("total_positive_flex_power"),
                (-c("power").filter(c("power") < 0).max())
                .fill_null(0)
                .alias("total_negative_flex_power"),
            )
        )
        
        self.volume_buffer: dict[int, float] = pl_to_dict(
                self.hydro_power_plant.select(
                    c("H"),
                    c("rated_flow")
                    * data_config.second_stage_sim_horizon.total_seconds()
                    * data_config.volume_buffer_ratio,
                )
        )

        
