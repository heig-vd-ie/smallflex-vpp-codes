
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
    water_basin: pl.DataFrame, basin_height_volume_table: pl.DataFrame, volume_factor: float, d_height: float
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
        actual_basin_state = basin_state.filter(c("B") == power_performance["B"][0])
        new_state_index = power_performance.sort("volume").filter(
            c("volume").is_between(
                actual_basin_state["hydro_volume_min"].min(), 
                actual_basin_state["hydro_volume_max"].max())
            ).join(
                actual_basin_state["hydro_volume_min", "S"], 
                left_on="volume",  right_on="hydro_volume_min", 
                how="left"
            )
        
        new_state_index = new_state_index.fill_null(strategy ="forward")\
            .group_by("S").agg(c("H", "B").first(), c("flow", "power").mean()).sort("S")\
            .with_columns(
                c("flow").abs().alias("flow")
            ).with_columns(
                (c("power")/c("flow")).alias("alpha"),
            )
        state_index = pl.concat([state_index, new_state_index], how="diagonal_relaxed")

    state_index = state_index.with_columns(
        pl.concat_list("H", "B", "S").alias("HBS"),
        pl.concat_list("H", "S").alias("HS")
    )
    return state_index
            

def split_timestamps_per_sim(data: pl.DataFrame, divisors: int, col_name: str = "T") -> pl.DataFrame:
    """
    Splits the timestamps in the given DataFrame into groups (simulations) based on the specified divisor.
    Each group is assigned a simulation number ("sim_idx") such that the timestamps are evenly distributed.
    If the number of rows in the DataFrame is not divisible by `divisors`, an offset is added to ensure even splitting.
    The function also resets the timestamp column (`col_name`) within each simulation group to be a cumulative count.
    Args:
        data (pl.DataFrame): The input DataFrame containing timestamp data.
        divisors (int): The number of groups (simulations) to split the data into.
        col_name (str, optional): The name of the timestamp column to split and reset. Defaults to "T".
    Returns:
        pl.DataFrame: A new DataFrame with an added "sim_idx" column indicating the simulation number,
        and the timestamp column reset within each simulation group.
    """

    offset = data.height%divisors
    if offset != 0:
        offset: int = divisors - data.height%divisors
    return(
        data.with_columns(
            ((c(col_name) + offset)//divisors).alias("sim_idx")
        ).with_columns(
            (c(col_name).cum_count() - 1).over("sim_idx").alias(col_name)
        )
    )
    
    
def generate_first_stage_basin_state_table(
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
            
        new_basin_state = generate_basin_state(
            basin_volume_table=basin_volume_table.filter(c("B") == data["B"]), 
            nb_state=nb_state)
        
        basin_state = pl.concat([
            basin_state, 
            new_basin_state.with_columns(pl.lit(basin_index).alias("B"))
        ], how="diagonal_relaxed")

    
    basin_state = pl.concat([
        basin_state, 
        water_basin.filter(~c("B").is_in(basin_volume_table["B"]))["B", "volume_max", "volume_min"]
    ], how="diagonal_relaxed")

    
    basin_state = basin_state.with_row_index(name="S").with_columns(
        pl.concat_list("B", "S").alias("BS")
    )
    
    return basin_state




def generate_basin_state(basin_volume_table: pl.DataFrame, nb_state: int, boundaries: Optional[tuple[float, float]] = None) -> pl.DataFrame:
    
    if boundaries is None:
        basin_state = basin_volume_table
    else:    
        basin_state = filter_data_with_next(
                data=basin_volume_table, 
                col="volume", boundaries=boundaries)

    basin_state = basin_state.with_columns(
        ((c("B").cum_count() - 1) *nb_state //basin_state.height).alias("S")
    ).group_by("S", maintain_order=True).agg(
        c("volume").last().alias("hydro_volume_max"),
    ).with_columns(
        c("hydro_volume_max").shift(1).fill_null(basin_state["volume"].min()).alias("hydro_volume_min")
    ).drop("S")


    basin_state = basin_state.with_columns(
        pl.when(c("hydro_volume_min") == c("hydro_volume_min").min())
        .then(pl.lit(basin_volume_table["volume"].min()))
        .otherwise(c("hydro_volume_min")).alias("volume_min"),
        pl.when(c("hydro_volume_max") == c("hydro_volume_max").max())
        .then(pl.lit(basin_volume_table["volume"].max()))
        .otherwise(c("hydro_volume_max")).alias("volume_max")
    )
    
    return basin_state