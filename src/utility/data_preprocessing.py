from typing import Optional, Union, Literal
import polars as pl
from polars import col as c
import numpy as np
import math
from datetime import datetime, date, timedelta
import pyomo.environ as pyo

from general_function import pl_to_dict, generate_log


log = generate_log(name=__name__)


def extract_result_table(model_instance: pyo.Model, var_name: str) -> pl.DataFrame:
    
    index_list: list[str] = list(map(lambda x: x.name, getattr(model_instance, var_name).index_set().subsets()))
        
    if len(index_list) == 1:
        data_pl: pl.DataFrame = pl.DataFrame(
            map(list, getattr(model_instance, var_name).extract_values().items()), 
            schema= [index_list[0], var_name]
        )
    else:
        data_pl: pl.DataFrame = pl.DataFrame(
            map(list, getattr(model_instance, var_name).extract_values().items()), 
            schema= ["index", var_name]
        ).with_columns(
            c("index").list.to_struct(fields=index_list)
        ).unnest("index")
    return data_pl.with_columns(c(index_list).cast(pl.UInt32))

def pivot_result_table(
    df: pl.DataFrame, on: str, index: Union[list[str], str], values: str, reindex: bool= False,
    shift: bool = False
    ) -> pl.DataFrame:
    df = df.with_columns(
        (values + "_" + c(on).cast(pl.Utf8)).alias(on)
    ).pivot(on=on, index=index, values=values)
    
    if shift:
        df = df.with_columns(
            pl.all().exclude(index).shift(-1)
        ).drop_nulls()
    
    if reindex:
        df = df.drop(index).with_row_index(name="real_index")
    
    return df

def arange_float(high, low, step):
    return pl.arange(
        start=0,
        end=math.floor((high-low)/step) + 1,
        step=1,
        eager=True
    ).cast(pl.Float64)*step + low

def filter_data_with_next(data: pl.DataFrame, col: str, boundaries: tuple[float, float]) -> pl.DataFrame:
    lower = data.filter(c(col).lt(boundaries[0]))[col].max()
    larger = data.filter(c(col).gt(boundaries[1]))[col].min()
    if lower is None:
        lower = data[col].min()
    if larger is None:
        larger = data[col].max()

    return data.sort(col).filter(c(col).ge(lower).and_(c(col).le(larger)))

def linear_interpolation_for_bound(x_col: pl.Expr, y_col: pl.Expr) -> pl.Expr:
    
    a_diff: pl.Expr = y_col.diff()/x_col.diff()
    x_diff: pl.Expr = x_col.diff().backward_fill()
    y_diff: pl.Expr = pl.coalesce(
        pl.when(y_col.is_null().or_(y_col.is_nan()))
        .then(a_diff.forward_fill()*x_diff)
        .otherwise(pl.lit(0)).cum_sum(),
        pl.when(y_col.is_null().or_(y_col.is_nan()))
        .then(-a_diff.backward_fill()*x_diff)
        .otherwise(pl.lit(0)).cum_sum(reverse=True)
    )

    return y_col.backward_fill().forward_fill() + y_diff

def linear_interpolation_using_cols(
    df: pl.DataFrame, x_col: str, y_col: Union[list[str], str]
    ) -> pl.DataFrame:
    df = df.sort(x_col)
    x = df[x_col].to_numpy()
    if isinstance(y_col, str):
        y_col = [y_col]
    for col in y_col:
        y = df[col].to_numpy()
        mask = ~np.isnan(y)
        df = df.with_columns(
            pl.Series(np.interp(x, x[mask], y[mask], left=np.nan, right=np.nan)).fill_nan(None).alias(col)
        ).with_columns(
            linear_interpolation_for_bound(x_col=c(x_col), y_col=c(col)).alias(col)
        )
    return df

def limit_column(
    col: pl.Expr, lower_bound: Optional[int | datetime | float | date] = None, 
    upper_bound: Optional[int | datetime | float | date] = None
) -> pl.Expr:
    if lower_bound is not None:
        col = pl.when(col < lower_bound).then(lower_bound).otherwise(col)
    if upper_bound is not None:
        col = pl.when(col > upper_bound).then(upper_bound).otherwise(col)
    return col    

def generate_datetime_index(
    min_datetime: datetime, max_datetime: datetime, 
    real_timestep: timedelta, sim_timestep: Optional[timedelta] = None,
    ) ->  pl.DataFrame:

    datetime_index: pl.DataFrame = pl.datetime_range(
        start=min_datetime, end=max_datetime,
        interval=real_timestep, eager=True, closed="left", time_zone="UTC"
    ).to_frame(name="timestamp").with_row_index(name="T")
    
    if sim_timestep:
        datetime_index: pl.DataFrame = datetime_index.group_by_dynamic(
            every=sim_timestep, index_column="timestamp", start_by="datapoint", closed="left"
            ).agg(
                c("T").count().alias("n_index")
            ).with_row_index(name="T")
    else:
        datetime_index = datetime_index.with_columns(pl.lit(1).alias("n_index"))

    return datetime_index

def generate_clean_timeseries_scenarios(
    data: pl.DataFrame, col_name : str,  timestep: timedelta, 
    max_value: Optional[int] = None, min_value: Optional[int] = None,
    agg_type: Literal["mean", "sum", "first"] = "first", grouping_columns: list[str] = ["year"]
    ) -> pl.DataFrame:

    if data.group_by(grouping_columns).agg(c(col_name).count()).n_unique(col_name) > 1:
        raise ValueError(f"There is not same amount of timestamps per year method")
        
    cleaned_data = data.with_columns(
            c(col_name).clip(lower_bound=min_value, upper_bound=max_value).alias(col_name)
        )
    
    cleaned_data = cleaned_data.with_columns(
        ((c("timestamp") - pl.datetime(c("year"),1,1,time_zone="UTC"))/timestep).floor().cast(pl.Int32).alias("T")
    ).group_by(grouping_columns + ["T"]).agg(
        c(col_name).mean() if agg_type=="mean" else c(col_name).sum() if agg_type=="sum" else c(col_name).first()
    ).sort(grouping_columns + ["T"]).with_columns(
        pl.concat_list("T", "year").alias("TΩ")
    )
        
    return cleaned_data


def generate_clean_timeseries(
    data: pl.DataFrame, col_name : str, min_datetime: datetime, max_datetime: datetime, timestep: timedelta, 
    max_value: Optional[int] = None, min_value: Optional[int] = None,
    agg_type: Literal["mean", "sum", "first"] = "first", timestamp_col: str = "timestamp"
    ) -> pl.DataFrame:

    datetime_index = generate_datetime_index(
            min_datetime=min_datetime, max_datetime=max_datetime, real_timestep=timestep)
    
    cleaned_data: pl.DataFrame = data\
        .filter(c(timestamp_col).ge(min_datetime).and_(c(timestamp_col).lt(max_datetime)))\
        .sort(timestamp_col)\
        .with_columns(
            c(col_name).clip(lower_bound=min_value, upper_bound=max_value).alias(col_name)
        )
    
    
    cleaned_data = cleaned_data\
        .group_by_dynamic(
            index_column=timestamp_col, start_by="datapoint", every=timestep, closed="left"
        ).agg(
            c(col_name).mean() if agg_type=="mean" else c(col_name).sum() if agg_type=="sum" else c(col_name).first(),
            c(col_name).max().name.prefix("max_"),
            c(col_name).min().name.prefix("min_"),
        )
    return (
        datetime_index["T", "timestamp"]\
            .join(cleaned_data, left_on="timestamp", right_on=timestamp_col, how="left")\
            .with_columns(c(col_name).interpolate().forward_fill().backward_fill())
    )

def generate_basin_volume_table(
    water_basin: pl.DataFrame, basin_height_volume_table: pl.DataFrame, d_height: float
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
        d_height (float, optional): Step size for height interpolation. Defaults to 1.
    Returns:
        dict[int, Optional[pl.DataFrame]]: Dictionary mapping basin identifiers to their interpolated
            height-volume tables, or None if no data is available for a basin.
    """

    basin_volume_table: pl.DataFrame = pl.DataFrame()

    for water_basin_index in water_basin.to_dicts():

        basin_height_volume_table = basin_height_volume_table\
                .filter(c("water_basin_fk") == water_basin_index["uuid"])\
             
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