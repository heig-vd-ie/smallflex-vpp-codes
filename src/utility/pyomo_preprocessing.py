import datetime
import numpy as np
from datetime import timedelta, date, datetime
import polars as pl
from polars import col as c
import polars.selectors as cs
from typing_extensions import Optional, Literal
import pyomo.environ as pyo
from typing import Union
import math

from data_federation.input_model import SmallflexInputSchema


def arange_float(high, low, step):
    return pl.arange(
        start=0,
        end=math.floor((high-low)/step) + 1,
        step=1,
        eager=True
    ).cast(pl.Float64)*step + low

def calculate_segment_error(x, y):
    """Fit a line to the points (x, y) and calculate the mean squared error."""
    A = np.vstack([x, np.ones(len(x))]).T
    m, c = np.linalg.lstsq(A, y, rcond=None)[0]  # Line fit
    y_pred = m * x + c
    mse = np.mean((y - y_pred) ** 2)  # Mean squared error
    return mse

def optimal_segments(x, y, n_segments: int):

    n = len(x)
    errors = np.full((n, n), np.inf)  # Error table between each pair of points
    for i in range(n):
        for j in range(i + 1, n):
            errors[i][j] = calculate_segment_error(x[i:j+1], y[i:j+1])

    dp = np.full((n, n_segments + 1), np.inf)
    dp[0][0] = 0  # Starting point, no error
    splits = np.zeros((n, n_segments + 1), dtype=int)  # Stores split points
    # Fill the DP table
    for j in range(1, n):
        for k in range(1, n_segments + 1):
            for i in range(j):
                current_error = dp[i][k - 1] + errors[i][j]
                if current_error < dp[j][k]:
                    dp[j][k] = current_error
                    splits[j][k] = i
    # Retrieve the optimal segment intervals
    segments = [len(x) - 1]
    idx = n - 1
    for k in range(n_segments, 0, -1):
        i = splits[idx][k]
        segments = [int(i)] + segments
        idx = i
    return segments

def generate_clean_timeseries(
    data: pl.DataFrame, datetime_index: pl.DataFrame, 
    col_name : str, min_datetime: datetime, max_datetime: datetime, time_delta: timedelta, 
    max_value: Optional[int] = None, min_value: Optional[int] = None,
    agg_type: Literal["mean", "sum", "first"] = "first", timestamp_col: str = "timestamp"
    ) -> pl.DataFrame:


    cleaned_data = data\
        .filter(c(timestamp_col).ge(min_datetime).and_(c(timestamp_col).lt(max_datetime)))\
        .sort(timestamp_col)\
        .with_columns(
            c(col_name).pipe(limit_column, lower_bound=min_value, upper_bound=max_value).alias(col_name)
        )
    
    cleaned_data = cleaned_data\
        .group_by_dynamic(
            index_column=timestamp_col, start_by="datapoint", every=time_delta, closed="left"
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
    
def generate_segments(
        data: pl.DataFrame, x_col: str, y_cols: Union[str, list[str]], n_segments: int):
    data = data.sort(x_col)
    segments: list = []
    
    if isinstance(y_cols, str):
        y_cols = [y_cols]
        
    columns: list[str] = [x_col] + y_cols
    
    for y_col in y_cols:
    
        segments.append(
            optimal_segments(
            x=np.array(data[x_col]),
            y=np.array(data[y_col]),
            n_segments=n_segments
        ))
    # Get the mean of the segments index founded for each segmentation
    segments = list(np.array(segments).mean(axis=0, dtype=int))
    
    return(
        data
        .with_row_index(name= "val_index").filter(pl.col("val_index").is_in(segments))
        .drop("val_index")[[x_col] + y_cols]
        .with_columns(
            # Define max and min values for each segment
            pl.concat_list(c(col), c(col).shift(-1)).alias(col)  for col in columns)
        .slice(offset=0, length=n_segments) # Remove last segment not bounded
        .with_columns(
            # Calculate the slope of the segments
            (pl.all().exclude(x_col)
            .list.eval(pl.element().get(1) - pl.element().get(0)).list.get(0) /
            c(x_col).list.eval(pl.element().get(1) - pl.element().get(0)).list.get(0)).name.prefix("d_"),
        )
    )

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
    first_time_delta: timedelta, second_time_delta: timedelta
    ) -> tuple[pl.DataFrame, pl.DataFrame]:

    second_datetime_index: pl.DataFrame = pl.datetime_range(
        start=min_datetime, end=max_datetime,
        interval= second_time_delta, eager=True, closed="left", time_zone="UTC"
    ).to_frame(name="timestamp").with_row_index(name="T")

    first_datetime_index: pl.DataFrame = second_datetime_index.group_by_dynamic(
        every=first_time_delta, index_column="timestamp", start_by="datapoint", closed="left"
        ).agg(
            c("T").min().alias("T_min"),
            c("T").max().alias("T_max"),
            c("T").count().alias("n_index")
        ).with_row_index(name="T")

    return first_datetime_index, second_datetime_index

def extract_optimization_results(model_instance: pyo.Model, var_name: str) -> pl.DataFrame:
    index_list = [set_.name for set_ in getattr(model_instance, var_name).index_set().subsets()]
    
    if len(index_list) == 1:
        return pl.DataFrame(map(list, getattr(model_instance, var_name).extract_values().items()), schema= [index_list[0], var_name])
    else:
        return pl.DataFrame(map(list, getattr(model_instance, var_name).extract_values().items()), schema= ["index", var_name]).with_columns(
            c("index").list.to_struct(fields=index_list)
        ).unnest("index")
        
# def process_performance_table(
#     small_flex_input_schema: SmallflexInputSchema, power_plant_name: str, state: list[bool], d_height: float = 1, n_segments: int = 5 ):

#     power_plant_metadata = small_flex_input_schema.hydro_power_plant\
#         .filter(c("name") == power_plant_name).to_dicts()[0]
#     water_basin_uuid = power_plant_metadata["upstream_basin_fk"]
#     power_plant_uuid = power_plant_metadata["uuid"]
#     basin_metadata = small_flex_input_schema.water_basin.filter(c("uuid") == water_basin_uuid).to_dicts()[0]

#     basin_height_volume_table: pl.DataFrame = small_flex_input_schema\
#             .basin_height_volume_table\
#             .filter(c("water_basin_fk") == water_basin_uuid)

#     down_stream_height = small_flex_input_schema.water_basin.filter(c("uuid") == power_plant_metadata["downstream_basin_fk"])["height_max"][0]

#     power_plant_state = small_flex_input_schema.power_plant_state.filter(c("power_plant_fk") == power_plant_uuid)
#     power_performance_table: pl.DataFrame = small_flex_input_schema.hydro_power_performance_table.with_columns((c("head") + down_stream_height).alias("height"))

#     state_uuid = power_plant_state.filter(c("resource_state_list") == state)["uuid"][0]
#     height_min: float= basin_metadata["height_min"] if basin_metadata["height_min"] is not None else basin_height_volume_table["height"].min() # type: ignore
#     height_max: float= basin_metadata["height_max"] if basin_metadata["height_max"] is not None else basin_height_volume_table["height"].max() # type: ignore

#     volume_table: pl.DataFrame = arange_float(height_max, height_min, d_height)\
#         .to_frame(name="height")\
#         .join(basin_height_volume_table, on ="height", how="full", coalesce=True)\
#         .sort("height").interpolate()\
#         .with_columns(
#             c("volume").pipe(linear_interpolation_for_bound).alias("volume")
#         ).drop_nulls("height")[["height", "volume"]]
        
#     performance_table_per_volume = volume_table.join(
#         power_performance_table.filter(c("power_plant_state_fk") == state_uuid)[["height", "flow", "electrical_power"]],
#         on ="height", how="full", coalesce=True
#         ).sort("height").interpolate()\
#         .with_columns(
#             c("flow", "electrical_power").pipe(linear_interpolation_for_bound)
#         ).filter(c("height").ge(height_min).and_(c("height").le(height_max)))\
#         .with_columns((c("electrical_power")/c("flow")).alias("alpha"))\
#         [["height", "volume", "flow", "electrical_power", "alpha"]]
        
#     performance_table_per_state = generate_segments(performance_table_per_volume, x_col="volume", y_col="alpha", n_segments=n_segments)
    
#     return performance_table_per_volume, performance_table_per_state

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