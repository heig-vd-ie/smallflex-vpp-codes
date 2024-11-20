import datetime
import numpy as np
from datetime import timedelta, date, datetime
import polars as pl
from polars import col as c
import polars.selectors as cs
from typing_extensions import Optional, Literal
from itertools import chain
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
        data.with_columns(
            cs.exclude(cs.starts_with("alpha")).abs()
        ).with_row_index(name= "val_index").filter(pl.col("val_index").is_in(segments))
        .drop("val_index")
        .with_columns(
            # Define max and min values for each segment
            pl.concat_list(c(col), c(col).shift(-1)).alias(col)  for col in data.columns)
        .slice(offset=0, length=n_segments) 
        .with_columns(
            # Calculate the slope of the segments
            (cs.starts_with("alpha")
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

def extract_optimization_results(model_instance: pyo.Model, var_name: str, subset_mapping: dict) -> pl.DataFrame:
    index_list = list(chain(*map(lambda x: subset_mapping[x.name], getattr(model_instance, var_name).index_set().subsets())))
    
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

def join_index(index_list: list[str]) -> pl.Expr:
    return pl.concat_list(index_list).cast(pl.List(pl.Utf8)).list.join(separator="_").alias("_".join(index_list))

def explode_index(index_name) -> pl.Expr:
    return c(index_name).str.split("_").cast(pl.List(pl.UInt32)).list.to_struct(fields=index_name.split("_"))

def join_pyomo_variables(
    model_instance: pyo.Model, subset_mapping: dict, index_list: list[str], var_list: list[str]
    ) -> pl.DataFrame:
    
    index_name: str = "_".join(index_list)
    results: pl.DataFrame = pl.DataFrame(schema=[(index_name, pl.Utf8)])
    for var_name in var_list:
        results = results.join(
            extract_optimization_results(
                model_instance=model_instance, var_name=var_name, subset_mapping=subset_mapping
            ).select(var_name, join_index(index_list)), 
            on=index_name, how="full", coalesce=True)
        
    return results.with_columns(explode_index(index_name)).unnest(index_name)


def numerical_curviness(d: str, d2: str) -> pl.Expr:
    return c(d2).abs()/((1 + c(d)**2)**(3/2))


def numerical_derivative(x_col: str, y_col: str):
    d_ff: pl.Expr = c(y_col).diff(1)/c(x_col).diff(1)
    d_bf: pl.Expr = c(y_col).diff(-1)/c(x_col).diff(-1)
    return (pl.coalesce(d_ff, d_bf) + pl.coalesce(d_bf, d_ff))/2


def calculate_curviness(data: pl.DataFrame, x_col: str, y_col_list: list[str]): 
    x  = (data[x_col]/data[x_col].max()).to_numpy()
    # x  = data[x_col].to_numpy()
    for y_col in y_col_list:
        y = (data[y_col]/data[y_col].max()).to_numpy()
        # y  = data[y_col].to_numpy()
        d1 = np.gradient(y, x)
        d2 = np.gradient(d1, x)
        data = data.with_columns(
            pl.Series(np.abs(d2)/((1 + d1**2)**(3/2))).alias(f"k_{y_col}")
        )
    return data

def max_curviness(df: pl.DataFrame, x_col: str, y_col_list: list[str]):
    result: pl.DataFrame = calculate_curviness(df=df, x_col=x_col, y_col_list=y_col_list)
    return (
        result.select(pl.max_horizontal(cs.starts_with("k_")).alias("max")).max().row(0)[0]
    )