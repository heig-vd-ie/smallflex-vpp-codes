
import polars as pl
from polars import col as c
from datetime import timedelta, datetime, timezone

from data_federation.input_model import SmallflexInputSchema

from utility.pyomo_preprocessing import generate_datetime_index, generate_clean_timeseries, generate_segments
from utility.general_function import pl_to_dict


def generate_first_problem_input_data(
    small_flex_input_schema: SmallflexInputSchema, max_flow_factor: float, min_flow_factor: float, hydro_power_plant_name: str, year: int, first_time_delta: timedelta,
    second_time_delta: timedelta = timedelta(minutes=60), market_country = "CH", market = "DA", n_segments = 5):
    
    min_datetime = datetime(year, 1, 1, tzinfo=timezone.utc)
    max_datetime = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    
    power_plant_metadata = small_flex_input_schema.hydro_power_plant\
    .filter(c("name") == hydro_power_plant_name).to_dicts()[0]
    water_basin_uuid = power_plant_metadata["upstream_basin_fk"]
    power_plant_uuid = power_plant_metadata["uuid"]

    first_datetime_index, second_datetime_index = generate_datetime_index(
        min_datetime=min_datetime, max_datetime=max_datetime, first_time_delta=first_time_delta, second_time_delta=second_time_delta)

    market_price_measurement:pl.DataFrame = small_flex_input_schema.market_price_measurement\
        .filter(c("country") == market_country).filter(c("market") == market)
        
    discharge_flow_measurement: pl.DataFrame = small_flex_input_schema.discharge_flow_measurement\
        .filter(c("river") == "Griessee")\
        .with_columns(
            (c("value") * timedelta(hours=1).total_seconds()).alias("discharge_volume")
        )
        
    basin_metadata = small_flex_input_schema.water_basin.filter(c("uuid") == water_basin_uuid).to_dicts()[0]

    down_stream_height = small_flex_input_schema.water_basin.filter(c("uuid") == power_plant_metadata["downstream_basin_fk"])["height_max"][0]
    
    basin_height_volume_table: pl.DataFrame = small_flex_input_schema\
        .basin_height_volume_table\
        .filter(c("water_basin_fk") == water_basin_uuid)
        
    if basin_metadata["height_min"] is not None:
        basin_height_volume_table = basin_height_volume_table.filter(c("height") >= basin_metadata["height_min"])
    if basin_metadata["height_max"] is not None:
        basin_height_volume_table = basin_height_volume_table.filter(c("height") <= basin_metadata["height_max"])
        
    discharge_volume: pl.DataFrame = generate_clean_timeseries(
    data=discharge_flow_measurement, datetime_index=first_datetime_index,
    col_name="discharge_volume", min_datetime=min_datetime,
    max_datetime=max_datetime, time_delta=first_time_delta, agg_type="sum")

    market_price: pl.DataFrame = generate_clean_timeseries(
        data=market_price_measurement, datetime_index=first_datetime_index,
        col_name="avg", min_datetime=min_datetime, 
        max_datetime=max_datetime, time_delta=first_time_delta, agg_type="mean")

    basin_height : pl.DataFrame = generate_segments(
        data=basin_height_volume_table, x_col="height", y_col="volume",
        min_x=basin_metadata["height_min"], max_x=basin_metadata["height_max"],
        n_segments=n_segments)

    resource_fk_list = small_flex_input_schema.resource\
    .filter(c("uuid").is_in(power_plant_metadata["resource_fk_list"]))\
    .filter(c("type") == "hydro_turbine")["uuid"].to_list()
    
    not_selected_resource = [idx for idx, item in enumerate(power_plant_metadata["resource_fk_list"]) if item not in resource_fk_list]

    power_plant_state = small_flex_input_schema.power_plant_state.filter(c("power_plant_fk") == power_plant_uuid)
    if not_selected_resource:
        power_plant_state = power_plant_state\
        .filter(
            pl.all_horizontal(~c("resource_state_list").list.get(i) for i in not_selected_resource)
        ).with_row_index(name="index")

    power_performance_table: pl.DataFrame = small_flex_input_schema.hydro_power_performance_table\
        .join(
            power_plant_state[["uuid", "index"]], left_on="power_plant_state_fk", right_on="uuid", how="inner"
        ).sort("index", "head")
    
    alpha = power_performance_table.select((c("electrical_power")/c("flow")).drop_nans().alias("alpha")).mean()["alpha"][0]
    max_flow = max_flow_factor * power_performance_table.filter(c("flow") > 0)["flow"].min()*3600 # type: ignore
    min_flow = min_flow_factor * power_performance_table.filter(c("flow") > 0)["flow"].min()*3600 # type: ignore
    start_height = small_flex_input_schema.basin_height_measurement.filter(c("timestamp") == min_datetime)["height"][0]
    
    data: dict = {}

    sets: dict = {
        "T": first_datetime_index["index"].to_list(),
        "H": basin_height["index"].to_list(),
    }

    constant_params: dict =  {
        "t_max": first_datetime_index.height - 1,
        "alpha": alpha,
        "max_flow": max_flow,
        "min_flow": min_flow,
        "start_basin_height": start_height,
        "basin_dH_dV_2": basin_height["dx_dy"][0]
    }

    set_params: dict = {
        "max_basin_height": pl_to_dict(basin_height[["index", "max_height"]]),
        "min_basin_height": pl_to_dict(basin_height[["index", "min_height"]]),
        "basin_dH_dV": pl_to_dict(basin_height[["index", "dx_dy"]]),
        "market_price": pl_to_dict(market_price[["index", "avg"]]),
        "nb_hours": pl_to_dict(first_datetime_index[["index", "n_index"]]),
        "discharge_volume": pl_to_dict(discharge_volume[["index", "discharge_volume"]])
    }
    
    data.update(dict(map(lambda set: (set[0], {None: set[1]}), sets.items())))
    data.update(dict(map(lambda constant_param: (constant_param[0], {None: constant_param[1]}), constant_params.items())))
    data.update(set_params)
    
    return data