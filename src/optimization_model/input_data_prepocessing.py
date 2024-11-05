
import polars as pl
from polars import col as c
from datetime import timedelta, datetime, timezone

from data_federation.input_model import SmallflexInputSchema
from typing_extensions import Optional
from utility.pyomo_preprocessing import generate_datetime_index, generate_clean_timeseries, process_performance_table
from utility.general_function import pl_to_dict


def generate_baseline_index(
    small_flex_input_schema: SmallflexInputSchema, year: int, first_time_delta: timedelta, 
    second_time_delta: timedelta=timedelta(hours=1), hydro_power_mask: Optional[pl.Expr]= None
    ):
    
    min_datetime: datetime = datetime(year, 1, 1, tzinfo=timezone.utc)
    max_datetime: datetime = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    if hydro_power_mask is None:
            hydro_power_mask= pl.lit(True)  
    index: dict[str, pl.DataFrame] = {}
    index["first_datetime"], index["second_datetime"] = generate_datetime_index(
            min_datetime=min_datetime, max_datetime=max_datetime, first_time_delta=first_time_delta, second_time_delta=second_time_delta)
    index["hydro_power_plant"] = small_flex_input_schema.hydro_power_plant\
            .filter(hydro_power_mask).with_row_index(name="H")
    index["water_basin"] = small_flex_input_schema.water_basin\
            .filter(c("power_plant_fk").is_in(index["hydro_power_plant"]["uuid"])).with_row_index(name="B")
    index["water_basin_state"] = index["water_basin"].select(
            c("B", "uuid").repeat_by("n_state_min")
    ).explode(pl.all()).with_row_index(name="S")
    return index

def generate_water_flow_factor(index: dict[str, pl.DataFrame]) -> pl.DataFrame:

    water_volume_mapping = {
        "upstream_basin_fk" : {"pumped_volume": 1, "turbined_volume": -1},
        "downstream_basin_fk" : {"pumped_volume": -1, "turbined_volume": 1}
    }

    water_flow_factor = index["hydro_power_plant"]\
        .unpivot(on=["upstream_basin_fk", "downstream_basin_fk"], index="H", value_name="uuid", variable_name="situation")\
        .with_columns(
                c("situation").replace_strict(water_volume_mapping, default=None).alias("water_volume")
        ).unnest("water_volume").join(index["water_basin"][["uuid", "B"]], on="uuid", how="left")

    return water_flow_factor

def generate_first_problem_input_data(
    small_flex_input_schema: SmallflexInputSchema, max_flow_factor: float, min_flow_factor: float,
    hydro_power_plant_name: str, year: int, first_time_delta: timedelta, add_wind_production: bool = False,
    with_pumping: bool = False, second_time_delta: timedelta = timedelta(minutes=60), market_country = "CH",
    market = "DA", n_segments = 5
    ) -> tuple[dict, dict]:
    
    min_datetime = datetime(year, 1, 1, tzinfo=timezone.utc)
    max_datetime = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    
    turbined_table_per_volume, turbined_table_per_state = process_performance_table(
        small_flex_input_schema=small_flex_input_schema, power_plant_name=hydro_power_plant_name, state=[True, False, False])
    
    pumped_table_per_volume, pumped_table_per_state = process_performance_table(
        small_flex_input_schema=small_flex_input_schema, power_plant_name=hydro_power_plant_name, state=[False, True, True])
            
    first_datetime_index, second_datetime_index = generate_datetime_index(
            min_datetime=min_datetime, max_datetime=max_datetime, first_time_delta=first_time_delta, second_time_delta=second_time_delta)

    market_price_measurement:pl.DataFrame = small_flex_input_schema.market_price_measurement\
        .filter(c("country") == market_country).filter(c("market") == market)
        
    discharge_flow_measurement: pl.DataFrame = small_flex_input_schema.discharge_flow_measurement\
        .filter(c("river") == "Griessee")\
        .with_columns(
            (c("value") * timedelta(hours=1).total_seconds()).alias("discharge_volume")
        )
        
    wind_production_measurement: pl.DataFrame = small_flex_input_schema.power_production_measurement.select(
        "avg_active_power", 
        c("timestamp").dt.year().alias("year"),
        c("timestamp").dt.to_string(format="%m-%d %H:%M").alias("date_str"),
    ).sort("year").pivot(on="year", values="avg_active_power", index="date_str").sort("date_str")\
    .with_columns(
        pl.coalesce("2021", "2024").alias("wind_data")
    ).select(
        (str(year) + "-" + c("date_str")).str.to_datetime(format="%Y-%m-%d %H:%M", time_zone="UTC").alias("timestamp"),
        -c("wind_data") * timedelta(minutes=15) / timedelta(hours=1) # form MW per 15 minutes to MWh
    )
        
    discharge_volume: pl.DataFrame = generate_clean_timeseries(
        data=discharge_flow_measurement, datetime_index=first_datetime_index,
        col_name="discharge_volume", min_datetime=min_datetime,
        max_datetime=max_datetime, time_delta=first_time_delta, agg_type="sum")

    market_price: pl.DataFrame = generate_clean_timeseries(
        data=market_price_measurement, datetime_index=first_datetime_index,
        col_name="avg", min_datetime=min_datetime, 
        max_datetime=max_datetime, time_delta=first_time_delta, agg_type="mean")
    
    wind_production: pl.DataFrame = generate_clean_timeseries(
        data=wind_production_measurement, datetime_index=first_datetime_index,
        col_name="wind_data", min_datetime=min_datetime,
        max_datetime=max_datetime, time_delta=first_time_delta, agg_type="sum")
        
    start_basin_volume: float = turbined_table_per_volume["volume"][-2]
    turbined_rated_flow: float = turbined_table_per_state["flow"][0][0]*3600 # m3/h
    pumped_rated_flow: float = -pumped_table_per_state["flow"][0][0]*3600 # m3/h
    
    data: dict = {}

    sets: dict = {
        "T": first_datetime_index["index"].to_list(),
        "H": turbined_table_per_state["index"].to_list(),
    }

    constant_params: dict =  {
        "t_max": first_datetime_index.height - 1,
        "h_max": n_segments - 1,
        "max_turbined_flow": turbined_rated_flow*max_flow_factor,
        "min_turbined_flow": turbined_rated_flow*min_flow_factor,
        "max_pumped_flow": pumped_rated_flow*max_flow_factor if with_pumping else 0,
        "min_pumped_flow": pumped_rated_flow*min_flow_factor if with_pumping else 0,
        "start_basin_volume": start_basin_volume,
    }
        
    set_params: dict = {
        "max_basin_volume": pl_to_dict(turbined_table_per_state.select("index", c("volume").list.get(1))),
        "min_basin_volume": pl_to_dict(turbined_table_per_state.select("index", c("volume").list.get(0))),
        "alpha_turbined": pl_to_dict(turbined_table_per_state[["index", "mean_alpha"]]),
        "alpha_pumped": pl_to_dict(pumped_table_per_state[["index", "mean_alpha"]]),
        "market_price": pl_to_dict(market_price[["index", "avg"]]),
        "max_market_price": pl_to_dict(market_price[["index", "max_avg"]]),
        "min_market_price": pl_to_dict(market_price[["index", "min_avg"]]),
        "nb_hours": pl_to_dict(first_datetime_index[["index", "n_index"]]),
        "discharge_volume": pl_to_dict(discharge_volume[["index", "discharge_volume"]])
    }
    if add_wind_production:
        set_params["wind_energy"] = pl_to_dict(wind_production[["index", "wind_data"]])     
    data.update(dict(map(lambda set: (set[0], {None: set[1]}), sets.items())))
    data.update(dict(map(lambda constant_param: (constant_param[0], {None: constant_param[1]}), constant_params.items())))
    data.update(set_params)
    
    input_table: dict[str, pl.DataFrame] = {
        "first_datetime_index": first_datetime_index,
        "second_datetime_index": second_datetime_index,
        "turbined_table_per_volume": turbined_table_per_volume,
        "turbined_table_per_state": turbined_table_per_state,
        "pumped_table_per_volume": pumped_table_per_volume,
        "pumped_table_per_state": pumped_table_per_state,
    }
        
    return data, input_table