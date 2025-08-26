
import polars as pl
from polars import col as c
from polars import selectors as cs
from datetime import timedelta
import numpy as np


from smallflex_data_schema import SmallflexInputSchema
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


def generate_basin_volume_table(
    water_basin: pl.DataFrame, basin_height_volume_table: pl.DataFrame, volume_factor: float, d_height: float = 1
    ) -> pl.DataFrame:
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

    basin_volume_table: pl.DataFrame = pl.DataFrame()

    for water_basin_index in water_basin.to_dicts():

        basin_height_volume_table = basin_height_volume_table\
                .filter(c("water_basin_fk") == water_basin_index["uuid"])\
                .with_columns(
                    (c("volume") * volume_factor).alias("volume")
                )
        if basin_height_volume_table.is_empty():
            continue
        height_min: float= water_basin_index["height_min"] if water_basin_index["height_min"] is not None else basin_height_volume_table["height"].min() # type: ignore
        height_max: float= water_basin_index["height_max"] if water_basin_index["height_max"] is not None else basin_height_volume_table["height"].max() # type: ignore

        new_volume_table = arange_float(height_max, height_min, d_height)\
            .to_frame(name="height")\
            .join(basin_height_volume_table, on ="height", how="full", coalesce=True)\
            .sort("height")\
            .interpolate()\
            .with_columns(
                linear_interpolation_for_bound(x_col=c("height"), y_col=c("volume")).alias("volume")
            ).drop_nulls("height")[["height", "volume"]]\
            .filter(c("height").ge(height_min).and_(c("height").le(height_max)))\
            .with_columns(
                pl.lit(water_basin_index["B"]).alias("B")
            )
        basin_volume_table = pl.concat([basin_volume_table, new_volume_table], how="diagonal_relaxed")    
            
    return basin_volume_table

def clean_hydro_power_performance_table(
    hydro_power_plant: pl.DataFrame, water_basin: pl.DataFrame, hydro_power_performance_table: pl.DataFrame,
    basin_volume_table: pl.DataFrame
    ) -> pl.DataFrame:
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
    
    power_performance_table: pl.DataFrame = pl.DataFrame()
    for power_plant_data in hydro_power_plant.to_dicts():
        
        
        volume_table = basin_volume_table.filter(c("B") == power_plant_data["upstream_B"])

        upstream_basin = water_basin.filter(c("B") == power_plant_data["upstream_B"]).to_dicts()[0]
        downstream_basin = water_basin.filter(c("B") == power_plant_data["downstream_B"]).to_dicts()[0]
        if volume_table is None:
            continue
        new_performance_table: pl.DataFrame = hydro_power_performance_table\
            .filter(
                c("power_plant_fk") == power_plant_data["uuid"]
            ).with_columns(
                (c("head") + downstream_basin["height_max"]).alias("height"),
            )
            
        new_performance_table = new_performance_table.join(volume_table, on ="height", how="full", coalesce=True).drop("power_plant_fk")

        new_performance_table = linear_interpolation_using_cols(
            df=new_performance_table, 
            x_col="height", 
            y_col=new_performance_table.select(pl.all().exclude("height", "volume")).columns
            ).filter(c("height").ge(volume_table["height"].min()).and_(c("height").le(volume_table["height"].max())))

        new_performance_table = new_performance_table.with_columns(
            pl.lit(power_plant_data["H"]).alias("H"),
            pl.lit(upstream_basin["B"]).alias("B"),
        )
        power_performance_table = pl.concat([power_performance_table, new_performance_table], how="diagonal_relaxed")       

            
    return  power_performance_table

def generate_hydro_power_state(power_performance_table: pl.DataFrame, basin_state: pl.DataFrame) -> pl.DataFrame:

    state_index: pl.DataFrame = pl.DataFrame()
    for hydro_power_index in power_performance_table["H"].unique(): 
        power_performance: pl.DataFrame = power_performance_table.filter(c("H") == hydro_power_index)

        new_state_index = power_performance.sort("volume").join(
            basin_state.filter(c("B") == power_performance["B"][0])["volume_min", "S"], 
            left_on="volume",  right_on="volume_min", how="left")
        
        new_state_index = new_state_index.fill_null(strategy ="forward")\
            .group_by("S").agg(c("H", "B").first(), c("flow", "power").mean()).sort("S")\
            .with_columns(
                c("flow").abs().alias("flow")
            ).with_columns(
                (c("power")/c("flow")).alias("alpha"),
            )
        state_index = pl.concat([state_index, new_state_index], how="diagonal_relaxed")

    state_index = state_index.with_columns(
        pl.concat_list("H", "B", "S").alias("BHS"),
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
            (c(col_name).cum_count() - 1).over("sim_nb").alias(col_name)
        )
    )
    
    
    
def generate_seconde_stage_basin_state(
    index: dict[str, pl.DataFrame], water_flow_factor: pl.DataFrame, 
    basin_volume_table: dict, start_volume_dict: dict[str, float], 
    discharge_volume_tot: dict[str, float],
    timestep: timedelta, volume_factor: float, nb_state: int = 5):


    basin_volume = pl.DataFrame()
    basin_state = pl.DataFrame()
    initial_nb = 0

    water_flow = index["hydro_power_plant"]["H", "rated_flow"].join(water_flow_factor, on="H", how="left")
    water_flow = water_flow.with_columns(
    (c("rated_flow") * c("water_factor")).alias("water_flow")
    ).group_by("B").agg(
        c("water_flow").filter(c("water_flow") > 0).sum().alias("water_flow_in"),
        c("water_flow").filter(c("water_flow") < 0).sum().alias("water_flow_out")
    ).with_columns(
        c("water_flow_in", "water_flow_out") * timestep.total_seconds() * volume_factor,
        c("B").replace_strict(start_volume_dict, default=None).alias("start_volume"),
        c("B").replace_strict(discharge_volume_tot, default=0.0).alias("discharge_volume")
    ).sort("B")

    water_flow = water_flow.with_columns(
        (c("start_volume") + c("water_flow_in") + c("discharge_volume")).alias("max_volume"),
        (c("start_volume") +  c("water_flow_out")).alias("min_volume")
    ).with_columns(
        pl.concat_list("min_volume", "max_volume").alias("boundaries")
    )

    for index_b, data  in basin_volume_table.items():

        if data is None:
            new_basin_state = index["water_basin"].filter(c("B") == index_b).select(
                "B", "volume_max", "volume_min", pl.lit(initial_nb).alias("S_b")
            )
            
        else:
            boundaries = water_flow.filter(c("B") ==  index_b)["boundaries"].to_list()[0]
            new_basin_volume = filter_data_with_next(
                data=data, col="volume", boundaries=boundaries)

            new_basin_volume = arange_float(new_basin_volume["height"].max(), new_basin_volume["height"].min(), 0.1)\
                        .to_frame(name="height")\
                        .join(new_basin_volume, on="height", how="left")\
                        .sort("height").interpolate()
                        
            new_basin_volume = new_basin_volume.with_row_index(name="S_b").with_columns(
                (c("S_b") * nb_state) // new_basin_volume.height + initial_nb,
                pl.lit(index_b).alias("B")
                
            )
            new_basin_state = new_basin_volume.group_by("S_b").agg(
                c("volume").min().alias("volume_min"),
                pl.lit(index_b).alias("B")
            ).sort("S_b").with_columns(
                c("volume_min").shift(-1).fill_null(new_basin_volume["volume"].max()).alias("volume_max")
            )

            
        basin_volume = pl.concat([basin_volume, new_basin_volume], how="diagonal_relaxed")
        basin_state = pl.concat([basin_state, new_basin_state], how="diagonal_relaxed")
        
        initial_nb = basin_state["S_b"].max() + 1 # type: ignore
    basin_state = basin_state.with_columns(pl.concat_list("B", "S_b").alias("BS"))
    return basin_state, basin_volume
    
    
def generate_second_stage_hydro_power_state(
    power_performance_table: list[dict], basin_volume: pl.DataFrame
    ) -> pl.DataFrame:
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

    power_performance_table = power_performance_table
    hydro_state = pl.DataFrame()
    for performance_table in power_performance_table:
        
        new_hydro_state: pl.DataFrame = basin_volume\
            .filter(c("B") == performance_table["B"])\
            .select("height", "S_b")\
            .join(performance_table["power_performance"], on="height", how="left")


        new_hydro_state = new_hydro_state.unique().sort("height").interpolate()\
            .with_columns(
                (c("power")/ c("flow")).alias("alpha")
            ).group_by("S_b").agg(c("flow").mean(), c("alpha").mean()).sort("S_b"). with_columns(
                c("S_b").cast(pl.Int32).alias("S_b"),
                pl.lit(performance_table["H"]).alias("H"),
                pl.lit(performance_table["B"]).alias("B"),
            )

        hydro_state = pl.concat([hydro_state, new_hydro_state], how="diagonal_relaxed")
        
    hydro_state = hydro_state.with_row_index(name="S_h").with_columns(
        pl.concat_list("H", "B", "S_h", "S_b").alias("S_BH"),
        pl.concat_list("H", "S_h").alias("HS"),
        pl.when(c("flow") < 0).then(-c("flow")).otherwise(c("flow")).alias("flow"),
        pl.when(c("flow") < 0).then(-c("alpha")).otherwise(c("alpha")).alias("alpha"),
    )
    return hydro_state


def generate_basin_state_table(
    basin_volume_table: pl.DataFrame, 
    water_basin: pl.DataFrame, 
    nb_state_dict: dict[int, int] = {}
    ) -> pl.DataFrame:
    """
    Generates a table of basin states based on the volume table in the baseline input.
    
    Args:
        baseline_input (BaseLineInput): The baseline input containing basin volume data.
        nb_state (int): The number of states to generate for each basin.
        
    Returns:
        pl.DataFrame: A DataFrame containing the basin states with their respective min and max volumes.
    """

    basin_state = pl.DataFrame()

    for basin_index in basin_volume_table["B"].unique():
        data = basin_volume_table.filter(c("B") == basin_index)

        if basin_index in nb_state_dict.keys():
            nb_state = nb_state_dict[basin_index] 
        else:
            nb_state = water_basin.filter(c("B")== basin_index)["n_state_min"][0]
            
        bin = np.linspace(
            data.select(c("height").min()).item(), 
            data.select(c("height").max()).item(), 
            nb_state + 1, 
            dtype=np.int64
        )

        basin_state = pl.concat([
            basin_state,
            data.filter(c("height").is_in(bin))\
            .select(
                "height",
                c("volume").alias("volume_min"),
                c("volume").shift(-1).alias("volume_max"),
                pl.lit(basin_index).alias("B")
            ).drop_nulls()
        ], how="diagonal_relaxed")
    
    
    basin_state = pl.concat([
        basin_state, 
        water_basin.filter(~c("B").is_in(basin_volume_table["B"]))["B", "volume_max", "volume_min"]
    ], how="diagonal_relaxed")

    
    basin_state = basin_state.with_row_index(name="S").with_columns(
        pl.concat_list("B", "S").alias("BS")
    )
    
    
    return basin_state