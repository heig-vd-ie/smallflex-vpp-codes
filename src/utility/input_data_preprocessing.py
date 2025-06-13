
import polars as pl
from polars import col as c
from polars import selectors as cs
from datetime import timedelta

from typing_extensions import Optional
from utility.pyomo_preprocessing import (
    arange_float, filter_data_with_next,
    linear_interpolation_for_bound, linear_interpolation_using_cols,
    generate_state_index_using_errors,
    filter_by_index, get_min_avg_max_diff, define_state)

from general_function import pl_to_dict, generate_log


log = generate_log(name=__name__)
# This module provides utility functions for preprocessing input data related to hydro power plants and water basins.
# It uses the Polars library for efficient DataFrame operations and includes functions for generating water flow factors,
# creating basin volume tables, cleaning hydro power performance tables, generating hydro power states, splitting timestamps,
# and generating second stage states for simulation or optimization purposes.

# The main functionalities include:
# - Mapping water flow factors based on the relationship between basins and power plants.
# - Interpolating and constructing volume tables for water basins.
# - Cleaning and restructuring hydro power performance tables for further analysis.
# - Generating state indices for hydro power plants based on performance and error thresholds.
# - Splitting timestamps for simulation runs.
# - Generating second stage states for advanced modeling, considering start volumes and discharge volumes.

# The code relies on several helper functions imported from other modules, such as interpolation, filtering, and logging utilities.

def generate_water_flow_factor(index: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Generates a water flow factor DataFrame for hydro power plants based on their upstream and downstream basins.
    This function processes the 'hydro_power_plant' DataFrame from the provided index, unpivots the upstream and downstream basin columns,
    maps each situation ('upstream_basin_fk' or 'downstream_basin_fk') to corresponding pumped and turbined factors, and joins with the
    'water_basin' DataFrame to associate basin information. It then concatenates basin and hydro plant identifiers to create a unique key.
    Args:
        index (dict[str, pl.DataFrame]): 
            A dictionary containing at least the following DataFrames:
                - "hydro_power_plant": DataFrame with columns including 'upstream_basin_fk', 'downstream_basin_fk', and 'H'.
                - "water_basin": DataFrame with columns including 'uuid' and 'B'.
    Returns:
        pl.DataFrame: 
            A DataFrame with water flow factors, including columns for the situation, mapped pumped/turbined factors, 
            associated basin information, and a concatenated basin-hydro identifier.
    """

    water_volume_mapping = {
        "upstream_basin_fk" : {"pumped_factor": 1, "turbined_factor": -1},
        "downstream_basin_fk" : {"pumped_factor": -1, "turbined_factor": 1}
    }

    water_flow_factor = index["hydro_power_plant"]\
        .unpivot(on=["upstream_basin_fk", "downstream_basin_fk"], index="H", value_name="uuid", variable_name="situation")\
        .with_columns(
                c("situation").replace_strict(water_volume_mapping, default=None).alias("water_volume")
        ).unnest("water_volume").join(index["water_basin"][["uuid", "B"]], on="uuid", how="left").with_columns(
            pl.concat_list("B", "H").alias("BH")
        )

    return water_flow_factor

def generate_basin_volume_table(
    index: dict[str, pl.DataFrame], basin_height_volume_table: pl.DataFrame, volume_factor: float, d_height: float = 1
    ) -> dict[int, Optional[pl.DataFrame]]:
    """
    Generates a dictionary mapping water basin identifiers to interpolated height-volume tables.
    For each water basin in the provided index, this function filters the given basin_height_volume_table
    to the relevant basin, scales the volume by the specified volume_factor, and interpolates the volume
    values over a range of heights with the specified step (d_height). The resulting table contains
    'height' and 'volume' columns, with missing values interpolated linearly. If no data is available
    for a basin, the corresponding value in the output dictionary is set to None.
    Args:
        index (dict[str, pl.DataFrame]): Dictionary containing water basin metadata, including unique identifiers
            and height bounds for each basin.
        basin_height_volume_table (pl.DataFrame): DataFrame containing height and volume data for all basins.
        volume_factor (float): Factor by which to scale the volume values.
        d_height (float, optional): Step size for height interpolation. Defaults to 1.
    Returns:
        dict[int, Optional[pl.DataFrame]]: Dictionary mapping basin identifiers to their interpolated
            height-volume tables, or None if no data is available for a basin.
    """

    volume_table: dict[int, Optional[pl.DataFrame]] = {}

    for water_basin_index in index["water_basin"].to_dicts():

        basin_height_volume_table = basin_height_volume_table\
                .filter(c("water_basin_fk") == water_basin_index["uuid"])\
                .with_columns(
                    (c("volume") * volume_factor).alias("volume")
                )
        if basin_height_volume_table.is_empty():
            volume_table[water_basin_index["B"]] = None
            continue
        height_min: float= water_basin_index["height_min"] if water_basin_index["height_min"] is not None else basin_height_volume_table["height"].min() # type: ignore
        height_max: float= water_basin_index["height_max"] if water_basin_index["height_max"] is not None else basin_height_volume_table["height"].max() # type: ignore

        volume_table[water_basin_index["B"]] = arange_float(height_max, height_min, d_height)\
            .to_frame(name="height")\
            .join(basin_height_volume_table, on ="height", how="full", coalesce=True)\
            .sort("height")\
            .interpolate()\
            .with_columns(
                linear_interpolation_for_bound(x_col=c("height"), y_col=c("volume")).alias("volume")
            ).drop_nulls("height")[["height", "volume"]]\
            .filter(c("height").ge(height_min).and_(c("height").le(height_max)))\
            
            
    return volume_table

def clean_hydro_power_performance_table(
    schema_dict: dict[str, pl.DataFrame], index: dict[str, pl.DataFrame], 
    basin_volume_table: dict[int, Optional[pl.DataFrame]]
    ) -> list[dict]:
    """
    Cleans and processes the hydro power performance table for each hydro power plant.
    This function iterates over all hydro power plants, retrieves relevant basin and state information,
    and constructs a cleaned and interpolated power performance table for each plant. The resulting
    tables are joined with basin volume data, interpolated, and augmented with calculated efficiency
    columns.
    Args:
        schema_dict (dict[str, pl.DataFrame]): Dictionary containing schema tables as Polars DataFrames,
            including "power_plant_state", "resource", and "hydro_power_performance_table".
        index (dict[str, pl.DataFrame]): Dictionary containing index tables as Polars DataFrames,
            including "hydro_power_plant" and "water_basin".
        basin_volume_table (dict[int, Optional[pl.DataFrame]]): Dictionary mapping basin IDs to their
            corresponding volume tables as Polars DataFrames. If a basin has no volume table, its value is None.
    Returns:
        list[dict]: A list of dictionaries, each containing:
            - "H": The head value for the power plant.
            - "B": The upstream basin identifier.
            - "power_performance": The cleaned and interpolated power performance table as a Polars DataFrame.
    """
    
    power_performance_table: list[dict] = []
    for power_plant_data in index["hydro_power_plant"].to_dicts():
        downstream_basin = index["water_basin"].filter(c("uuid") == power_plant_data["downstream_basin_fk"]).to_dicts()[0]
        upstream_basin = index["water_basin"].filter(c("uuid") == power_plant_data["upstream_basin_fk"]).to_dicts()[0]

        power_plant_state: pl.DataFrame = schema_dict["power_plant_state"]\
            .filter(c("power_plant_fk") == power_plant_data["uuid"])

        volume_table = basin_volume_table[upstream_basin["B"]]
        if volume_table is None:
            continue
        
        state_dict = {}
        
        turbine_fk = schema_dict["resource"]\
            .filter(c("uuid").is_in(power_plant_data["resource_fk_list"]))\
            .filter(c("type") == "hydro_turbine")["uuid"].to_list()

        state_list = [
                ("turbined", list(map(lambda uuid: uuid in turbine_fk, power_plant_data["resource_fk_list"]))), 
                ("pumped", list(map(lambda uuid: uuid not in turbine_fk, power_plant_data["resource_fk_list"])))
            ]
        for name, state in state_list:
            state_dict[power_plant_state.filter(c("resource_state_list") == state)["uuid"][0]] = name
            
        power_performance: pl.DataFrame = schema_dict["hydro_power_performance_table"]\
            .with_columns(
                (c("head") + downstream_basin["height_max"]).alias("height"),
            )

        power_performance = power_performance.with_columns(
                c("power_plant_state_fk").replace_strict(state_dict, default=None).alias("state_name")
            ).drop_nulls("state_name").pivot(
                values=["flow" , "electrical_power"], on="state_name", index="height"
            ).join(volume_table, on ="height", how="full", coalesce=True)\
            .sort("height")
        
        power_performance = linear_interpolation_using_cols(
            df=power_performance, 
            x_col="height", 
            y_col=power_performance.select(pl.all().exclude("height", "volume")).columns
            ).filter(c("height").ge(volume_table["height"].min()).and_(c("height").le(volume_table["height"].max())))

        power_performance = power_performance.with_columns(
            (c("electrical_power_" + state[0]) / c("flow_" + state[0])).alias("alpha_" + state[0]) 
            for state in state_list
        )
            
        power_performance_table.append({
            "H": power_plant_data["H"], "B": upstream_basin["B"], "power_performance": power_performance
            })
            
    return  power_performance_table
            
def generate_hydro_power_state(
    power_performance_table: list[dict], index: dict[str, pl.DataFrame], error_percent: float
    ) -> pl.DataFrame:
    
    """
    Generates a state index DataFrame for hydro power plants based on their power performance data and error thresholds.
    This function processes a list of power performance tables for multiple hydro power plants, segments their performance
    states using a specified error percentage, and aggregates the results into a single DataFrame. It also ensures that
    all water basins from the provided index are represented, adding missing basins with default volume statistics.
    Args:
        power_performance_table (list[dict]): 
            A list of dictionaries, each containing power performance data for a hydro power plant. 
            Each dictionary must include a "power_performance" key with a Polars DataFrame, and "H" and "B" identifiers.
        index (dict[str, pl.DataFrame]): 
            A dictionary of index DataFrames, must include a "water_basin" DataFrame with basin information.
        error_percent (float): 
            The error percentage used to segment the power performance data into states.
    Returns:
        pl.DataFrame: 
            A Polars DataFrame containing the state index for all hydro power plants and basins, 
            with additional columns for state identifiers and volume statistics.
    """

    state_index: pl.DataFrame = pl.DataFrame()
    for data in power_performance_table: 
        power_performance: pl.DataFrame = data["power_performance"]
        y_cols: list[str] = power_performance.select(cs.starts_with("alpha")).columns
        
        segments = generate_state_index_using_errors(data=power_performance, column_list=y_cols, error_percent=error_percent)
        if len(segments) > 10:
            log.warning(f"{len(segments)} states found in {data["H"]} hydro power plant")
        state_performance_table = filter_by_index(data=power_performance, index_list=segments)\
        .with_columns(
            c(col).abs().pipe(get_min_avg_max_diff).alias(col) for col in power_performance.columns
        ).slice(offset=0, length=len(segments)-1)
        
        state_index = pl.concat([
            state_index, 
            state_performance_table.with_columns(
                pl.lit(data["H"]).alias("H"),
                pl.lit(data["B"]).alias("B"))
            ], how="diagonal")
        # Add every downstream basin
    missing_basin: pl.DataFrame = index["water_basin"].filter(~c("B").is_in(state_index["B"])).select(
        c("B"),
        pl.struct(
            c("volume_min").fill_null(0.0).alias("min"), 
            c("volume_max").fill_null(0.0).alias("max"),
            (c("volume_max").fill_null(0.0) + c("volume_min").fill_null(0.0)/2).alias("avg"),
            (c("volume_max").fill_null(0.0) - c("volume_min").fill_null(0.0)).alias("diff"),
        ).alias("volume")
    )
    state_index = pl.concat([state_index, missing_basin], how="diagonal_relaxed")\
        .with_row_index(name="S").with_columns(
            pl.concat_list("H", "B", "S", "S").alias("S_BH"),
            pl.concat_list("B", "S").alias("BS"),
            pl.concat_list("H", "S").alias("HS")
        )
        
    return state_index

def split_timestamps_per_sim(data: pl.DataFrame, divisors: int, col_name: str = "T") -> pl.DataFrame:
    """
    Splits the timestamps in the given DataFrame into groups (simulations) based on the specified divisor.
    Each group is assigned a simulation number ("sim_nb") such that the timestamps are evenly distributed.
    If the number of rows in the DataFrame is not divisible by `divisors`, an offset is added to ensure even splitting.
    The function also resets the timestamp column (`col_name`) within each simulation group to be a cumulative count.
    Args:
        data (pl.DataFrame): The input DataFrame containing timestamp data.
        divisors (int): The number of groups (simulations) to split the data into.
        col_name (str, optional): The name of the timestamp column to split and reset. Defaults to "T".
    Returns:
        pl.DataFrame: A new DataFrame with an added "sim_nb" column indicating the simulation number,
        and the timestamp column reset within each simulation group.
    """

    offset = data.height%divisors
    if offset != 0:
        offset: int = divisors - data.height%divisors
    return(
        data.with_columns(
            ((c(col_name) + offset)//divisors).alias("sim_nb")
        ).with_columns(
            c(col_name).cum_count().over("sim_nb").alias(col_name)
        )
    )
    
def generate_second_stage_state(
    index: dict[str, pl.DataFrame], power_performance_table: list[dict],
    start_volume_dict: dict, discharge_volume:dict,
    timestep: timedelta, error_threshold: float, volume_factor: float
    ) -> dict[str, pl.DataFrame]:
    """
    Generates and updates the state index for the second stage of a hydro power plant optimization process.
    This function processes power performance tables for each hydro power plant, defines state boundaries based on
    rated flow and discharge volumes, filters and structures the data, and concatenates the results into a unified
    state index. It also handles missing basins by adding their minimum and maximum volumes as states.
    Args:
        index (dict[str, pl.DataFrame]): Dictionary containing input dataframes, including "hydro_power_plant" and "water_basin".
        power_performance_table (list[dict]): List of dictionaries, each containing performance data for a hydro power plant.
        start_volume_dict (dict): Dictionary mapping basin identifiers to their starting volumes.
        discharge_volume (dict): Dictionary mapping basin identifiers to their discharge volumes.
        timestep (timedelta): Time step used for calculating rated volumes.
        error_threshold (float): Error threshold for state definition.
        volume_factor (float): Factor to convert flow to volume.
    Returns:
        dict[str, pl.DataFrame]: Updated index dictionary with the new "state" dataframe containing the generated states.
    """
    

    rated_flow_dict = pl_to_dict(index["hydro_power_plant"][["H", "rated_flow"]])
    state_index: pl.DataFrame = pl.DataFrame()
    start_state: int = 0
    for performance_table in power_performance_table:
        
        start_volume = start_volume_dict[performance_table["B"]]
        rated_volume = rated_flow_dict[performance_table["H"]] * timestep.total_seconds() * volume_factor
        
        boundaries = (
            start_volume - rated_volume, start_volume + rated_volume + discharge_volume[performance_table["B"]]
        )
        data: pl.DataFrame = filter_data_with_next(
            data=performance_table["power_performance"], col="volume", boundaries=boundaries)
        y_cols = data.select(cs.starts_with(name) for name in ["flow", "electrical"]).columns
        state_name_list = list(set(map(lambda x : x.split("_")[-1], y_cols)))
        
        data = define_state(data=data, x_col="volume", y_cols=y_cols, error_threshold=error_threshold)
        
        data = data\
            .with_row_index(offset=start_state, name="S")\
            .with_columns(
                    pl.lit(performance_table["H"]).alias("H"),
                    pl.lit(performance_table["B"]).alias("B")
            ).with_columns(
                pl.struct(cs.ends_with(col_name)).name.map_fields(lambda x: "_".join(x.split("_")[:-1])).alias(col_name)
                for col_name in state_name_list
            ).unpivot(
                on=state_name_list, index= ["volume", "S", "H", "B"], value_name="data", variable_name="state"
            ).unnest("data").drop("state")
            
        state_index = pl.concat([state_index, data], how="diagonal")
        start_state = state_index["S"].max() + 1 # type: ignore
        
    state_index = state_index.with_row_index(name="S_Q")    
    missing_basin: pl.DataFrame = index["water_basin"]\
        .filter(~c("B").is_in(state_index["B"]))\
        .select(
            c("B"),
            pl.struct(
                c("volume_min").fill_null(0.0).alias("min"), 
                c("volume_max").fill_null(0.0).alias("max"),
            ).alias("volume")
        ).with_row_index(offset=start_state, name="S")

    index["state"] = pl.concat([state_index, missing_basin], how="diagonal_relaxed")\
        .with_columns(
        pl.concat_list("H", "B", "S", "S").alias("S_BH"),
        pl.concat_list("B", "S").alias("BS"),
        pl.concat_list("H", "S").alias("HS"),
        pl.concat_list("H", "S", "S_Q").alias("HQS")
    )
    return index
