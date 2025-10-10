from typing import Optional
import polars as pl
import polars.selectors as cs
from datetime import timedelta
from polars import col as c

from smallflex_data_schema import SmallflexInputSchema

from utility.data_preprocessing import (
    generate_basin_volume_table,
    clean_hydro_power_performance_table,
    split_timestamps_per_sim,
    generate_hydro_power_state,
    generate_first_stage_basin_state_table,
    generate_clean_timeseries,
    generate_datetime_index,
)

from general_function import pl_to_dict
from pipelines.data_configs import DeterministicConfig


class DataManager(DeterministicConfig):
    def __init__(
        self,
        pipeline_config: DeterministicConfig,
        smallflex_input_schema: SmallflexInputSchema,
        hydro_power_mask: Optional[pl.Expr] = None,
        wind_power_mask: Optional[pl.Expr] = None,
        pv_power_mask: Optional[pl.Expr] = None,
    ):
        if hydro_power_mask is None:
            hydro_power_mask = pl.lit(True)

        if wind_power_mask is None:
            wind_power_mask = pl.lit(True)

        if pv_power_mask is None:
            pv_power_mask = pl.lit(True)

        # Retrieve attributes from pipeline_config
        for key, value in vars(pipeline_config).items():
            setattr(self, key, value)

        self.second_stage_nb_sim: int
        self.neg_unpowered_price: float
        self.pos_unpowered_price: float
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
        # Timeseries measurement table
        self.first_stage_timeseries: pl.DataFrame
        self.second_stage_timeseries: pl.DataFrame


        self.__build_hydro_power_plant_data(
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
        )

    def __build_hydro_power_plant_data(
        self, smallflex_input_schema: SmallflexInputSchema, hydro_power_mask: pl.Expr
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
            .select("B", pl.lit(self.spilled_factor).alias("spilled_factor"))
            .unique(subset="B")
        )

        self.water_flow_factor = water_flow_factor.select(
            "B", "H", pl.concat_list(["B", "H"]).alias("BH"), "water_factor"
        )

        self.basin_volume_table = generate_basin_volume_table(
            water_basin=self.water_basin,
            basin_height_volume_table=smallflex_input_schema.basin_height_volume_table,
            d_height=self.d_height,
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
            nb_state_dict=self.nb_state_dict,
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
                    * self.second_stage_sim_horizon.total_seconds()
                    * self.volume_buffer_ratio,
                )
        )

        

    def process_timeseries_input(
        self,
        first_stage_timeseries: pl.DataFrame,
        second_stage_timeseries: pl.DataFrame
    ):


        

        self.first_stage_timeseries = input_timeseries\
                .group_by_dynamic(
                    index_column="timestamp", start_by="datapoint", every=self.first_stage_timestep, closed="left"
                ).agg(
                    cs.contains("market_price").mean(),
                    cs.starts_with("discharge_volume").sum(),
                    c("timestamp").count().alias("n_index"),
                ).with_row_index(name="T")

        self.second_stage_timeseries = split_timestamps_per_sim(
                    data=input_timeseries, divisors=self.second_stage_nb_timestamp
                ).with_columns(
                    (c("T") // self.nb_timestamp_per_ancillary).cast(pl.UInt32).alias("F")
                ).with_columns(pl.concat_list(["T", "F"]).alias("TF"))

        self.second_stage_nb_sim = self.second_stage_timeseries["sim_idx"].max() # type: ignore
        
        
        self.neg_unpowered_price = self.first_stage_timeseries["market_price"].quantile(0.5 + self.second_stage_quantile)  # type: ignore
        self.pos_unpowered_price = self.first_stage_timeseries["market_price"].quantile(0.5 - self.second_stage_quantile)  # type: ignore

