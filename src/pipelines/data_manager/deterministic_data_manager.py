from typing import Optional
import polars as pl
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


class DeterministicDataManager(DeterministicConfig):
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
        self.first_stage_timestep_index: pl.DataFrame
        self.second_stage_timestep_index: pl.DataFrame
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
        self.first_stage_discharge_volume: pl.DataFrame
        self.second_stage_discharge_volume: pl.DataFrame
        self.first_stage_market_price: pl.DataFrame
        self.second_stage_market_price: pl.DataFrame
        self.first_stage_ancillary_market_price: pl.DataFrame
        self.second_stage_ancillary_market_price: pl.DataFrame
        self.second_stage_wind_production: pl.DataFrame
        self.second_stage_pv_production: pl.DataFrame

        self.__build_timestep_index()
        self.__build_hydro_power_plant_data(
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
        )

        self.__process_timeseries_input(
            smallflex_input_schema=smallflex_input_schema,
            wind_power_mask=wind_power_mask,
            pv_power_mask=pv_power_mask,
        )
        self.__calculate_power_volume_buffer()

    def __build_timestep_index(self):

        self.first_stage_timestep_index = generate_datetime_index(
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            sim_timestep=self.first_stage_timestep,
            real_timestep=self.second_stage_timestep,
        )

        second_stage_timestep_index = generate_datetime_index(
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            real_timestep=self.second_stage_timestep,
        )
        second_stage_timestep_index = split_timestamps_per_sim(
            data=second_stage_timestep_index, divisors=self.second_stage_nb_timestamp
        )

        self.second_stage_timestep_index = second_stage_timestep_index.with_columns(
            (c("T") // self.nb_timestamp_per_ancillary).cast(pl.UInt32).alias("F")
        ).with_columns(pl.concat_list(["T", "F"]).alias("TF"))

        self.second_stage_nb_sim = second_stage_timestep_index["sim_idx"].max()  # type: ignore

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

    def __calculate_power_volume_buffer(self):

        self.volume_buffer: dict[int, float] = pl_to_dict(
            self.hydro_power_plant.select(
                c("H"),
                c("rated_flow")
                * self.second_stage_sim_horizon.total_seconds()
                * self.volume_buffer_ratio,
            )
        )

    def __process_timeseries_input(
        self,
        smallflex_input_schema: SmallflexInputSchema,
        wind_power_mask: pl.Expr,
        pv_power_mask: pl.Expr,
    ):

        basin_index_mapping = pl_to_dict(self.water_basin[["uuid", "B"]])

        discharge_flow: pl.DataFrame = (
            smallflex_input_schema.discharge_flow_historical.with_columns(
                c("basin_fk")
                .replace_strict(basin_index_mapping, default=None)
                .alias("B")
            )
            .drop_nulls("B")
            .drop_nulls(subset="basin_fk")
            .with_columns(
                (c("value") * self.second_stage_timestep.total_seconds()).alias(
                    "discharge_volume"
                )
            )
        )

        market_price: pl.DataFrame = (
            smallflex_input_schema.market_price_measurement.filter(
                c("country") == self.market_country
            ).filter(c("market") == self.market)
        )

        ancillary_market_price: pl.DataFrame = (
            smallflex_input_schema.market_price_measurement.filter(
                c("country") == self.market_country
            )
            .filter(c("market") == self.ancillary_market)
            .filter(c("source") == self.market_source)
            .sort("timestamp")
        )

    
        pv_production: pl.DataFrame  = smallflex_input_schema.weather_historical.filter(pv_power_mask)

        pv_production = pv_production.with_columns(
            (c("irradiation")/pv_production["irradiation"].max() * self.pv_power_rated_power).alias("pv_power")
        )

        wind_production: pl.DataFrame  = smallflex_input_schema.weather_historical.filter(wind_power_mask)

        wind_production = wind_production.select(
            c("timestamp"),
            pl.when(c("wind").is_between(self.wind_speed_cut_in, self.wind_speed_cut_off))\
            .then(
                c("wind").pow(3)/(self.wind_speed_cut_off**3) * self.wind_turbine_rated_power
            ).otherwise(0).alias("wind_power")
)
        self.neg_unpowered_price = market_price["avg"].quantile(0.5 + self.second_stage_quantile)  # type: ignore
        self.pos_unpowered_price = market_price["avg"].quantile(0.5 - self.second_stage_quantile)  # type: ignore
        ### Discharge_flow ##############################################################################################

        self.first_stage_discharge_volume: pl.DataFrame = generate_clean_timeseries(
            data=discharge_flow,
            col_name="discharge_volume",
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            timestep=self.first_stage_timestep,
            agg_type="sum",
        ).with_columns(pl.concat_list(["T", pl.lit(0).alias("B")]).alias("TB"))

        second_stage_discharge_volume: pl.DataFrame = generate_clean_timeseries(
            data=discharge_flow,
            col_name="discharge_volume",
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            timestep=self.second_stage_timestep,
            agg_type="sum",
        )
        self.second_stage_discharge_volume = (
            split_timestamps_per_sim(
                data=second_stage_discharge_volume,
                divisors=self.second_stage_nb_timestamp,
            )
            .with_columns(pl.lit(0).alias("B"))
            .with_columns(pl.concat_list(["T", "B"]).alias("TB"))
        )
        ### Market price ###############################################################################################
        self.first_stage_market_price = generate_clean_timeseries(
            data=market_price,
            col_name="avg",
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            timestep=self.first_stage_timestep,
            agg_type="mean",
        )
        second_stage_market_price: pl.DataFrame = generate_clean_timeseries(
            data=market_price,
            col_name="avg",
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            timestep=self.second_stage_timestep,
            agg_type="mean",
        )
        self.second_stage_market_price = split_timestamps_per_sim(
            data=second_stage_market_price, divisors=self.second_stage_nb_timestamp
        )
        ### Ancillary Market price #####################################################################################

        self.first_stage_ancillary_market_price: pl.DataFrame = (
            generate_clean_timeseries(
                data=ancillary_market_price,
                col_name="avg",
                min_datetime=self.min_datetime,
                max_datetime=self.max_datetime,
                timestep=self.first_stage_timestep,
                agg_type="mean",
            )
        )
        second_stage_ancillary_market_price: pl.DataFrame = generate_clean_timeseries(
            data=ancillary_market_price,
            col_name="avg",
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            timestep=self.ancillary_market_timestep,
            agg_type="mean",
        )
        self.second_stage_ancillary_market_price = split_timestamps_per_sim(
            data=second_stage_ancillary_market_price,
            divisors=self.ancillary_nb_timestamp,
        ).rename({"T": "F"})

        ### PV production ##############################################################################################$
        
        
        second_stage_pv_production: pl.DataFrame = generate_clean_timeseries(
                data=pv_production,
                col_name="pv_power",
                min_datetime=self.min_datetime,
                max_datetime=self.max_datetime,
                timestep=self.second_stage_timestep,
                agg_type="sum",
            )

        self.second_stage_pv_production = split_timestamps_per_sim(
                    data=second_stage_pv_production,
                    divisors=self.second_stage_nb_timestamp,
                )

        ### Wind production ############################################################################################
        second_stage_wind_production: pl.DataFrame = generate_clean_timeseries(
                data=wind_production,
                col_name="wind_power",
                min_datetime=self.min_datetime,
                max_datetime=self.max_datetime,
                timestep=self.second_stage_timestep,
                agg_type="sum",
            )

        self.second_stage_wind_production = split_timestamps_per_sim(
            data=second_stage_wind_production, divisors=self.second_stage_nb_timestamp
        )
