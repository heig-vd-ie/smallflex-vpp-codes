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


from pyomo.util.infeasible import find_infeasible_constraints

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
            c(col_name).pipe(limit_column, lower_bound=min_value, upper_bound=max_value).alias(col_name)
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
    return list(np.array(segments).mean(axis=0, dtype=int))
    
def define_state(data: pl.DataFrame, x_col: str, y_cols: Union[str, list[str]], error_threshold: float= 0.1):
    nb = 1
    if isinstance(y_cols, str):
        y_cols = [y_cols]
        
    data = data[[x_col] + y_cols].with_row_index(name="index")
    while True:
        row_idx: list = generate_segments(data=data, x_col=x_col, y_cols=y_cols, n_segments=nb)
        new_data: pl.DataFrame = data.with_columns(
            pl.when(c("index").is_in(row_idx))
            .then(c(col)).otherwise(pl.lit(None))
            for col in y_cols)
        
        new_data = linear_interpolation_using_cols(new_data, x_col=x_col, y_col=y_cols )
        
        error: float = ((new_data[y_cols] - data[y_cols])/ data[y_cols])\
            .select(
                pl.max_horizontal(pl.all().abs()*100).alias("max")
            )["max"].mean() # type: ignore
        if error < error_threshold :
            break
        nb += 1
    return (
        data.filter(c("index").is_in(row_idx)).drop("index")
        .with_columns(
            (c(col).diff()/c(x_col).diff()).shift(-1).alias("d_" + col) for col in y_cols
        ).with_columns(
            pl.struct(c(x_col).alias("min"), c(x_col).shift(-1).alias("max")).alias(x_col),
        ).slice(0, -1)
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

def extract_optimization_results(model_instance: pyo.Model, var_name: str) -> pl.DataFrame:
    
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
    """
    Splits a string column into multiple columns based on an underscore delimiter and casts the result to a struct.

    Args:
        index_name (str): The name of the column to be split. The column should contain strings with underscore-separated values.

    Returns:
        pl.Expr: An expression that splits the specified column into multiple columns, casts them to unsigned 32-bit integers, 
        and combines them into a struct with field names derived from the original column name.
    """
    return c(index_name).str.split("_").cast(pl.List(pl.UInt32)).list.to_struct(fields=index_name.split("_"))

def join_pyomo_variables(
    model_instance: pyo.Model, index_list: list[str], var_list: list[str]
    ) -> pl.DataFrame:
    
    index_name: str = "_".join(index_list)
    results: pl.DataFrame = pl.DataFrame(schema=[(index_name, pl.Utf8)])
    for var_name in var_list:
        results = results.join(
            extract_optimization_results(
                model_instance=model_instance, var_name=var_name
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
    result: pl.DataFrame = calculate_curviness(data=df, x_col=x_col, y_col_list=y_col_list)
    return (
        result.select(pl.max_horizontal(cs.starts_with("k_")).alias("max")).max().row(0)[0]
    )
    
def get_nb_states(col: pl.Expr, error_percent: float) -> pl.Expr:
    return (
        100 / error_percent * (col.max() - col.min()) / (col.max() +  col.min())
    )
    
def digitize_col(col: pl.Expr,  data: pl.DataFrame, nb_state: int):
    bin = np.linspace(data.select(col.min()).item(), data.select(col.max()).item(), nb_state + 1)
    return (
        col.map_elements(lambda x: np.digitize(x, bin), return_dtype=pl.Int64)
    )
def generate_state_index_using_errors(
    data: pl.DataFrame, column_list: Optional[list[str]] = None, error_percent: float = 2
    ) -> list[int]:  
    if column_list is None:
        column_list = data.columns
    nb_state: int = int(np.ceil(
        data.select(
            pl.max_horizontal(c(col).pipe(get_nb_states, error_percent=error_percent).alias(col)
            for col in column_list
        )).rows()[0][0]
    ))

    segments: list[int] = data.select(
        c(cols).pipe(digitize_col,data=data, nb_state=nb_state).alias(cols + "_bin") for cols in column_list
    ).with_row_index(name="index")\
    .select(
        c("index").first().over(cols + "_bin").alias(cols).unique() for cols in column_list
    ).select(   
        pl.mean_horizontal(pl.all()).cast(pl.UInt32).alias("state_index"),
    )["state_index"].to_list()

    return segments

def filter_by_index(data: pl.DataFrame, index_list: list[int]) -> pl.DataFrame:
    return data.with_row_index(name= "val_index").filter(pl.col("val_index").is_in(index_list)).drop("val_index")

def get_min_avg_max_diff(col: pl.Expr) -> pl.Expr:
    return (
        pl.concat_list(col, (col + col.shift(-1))/2, col.shift(-1), col.diff().shift(-1))
        .list.to_struct(fields=["min", "avg", "max", "diff"])
    )
    


def filter_data_with_next(data: pl.DataFrame, col: str, boundaries: tuple[float, float]) -> pl.DataFrame:
    lower = data.filter(c(col).lt(boundaries[0]))[col].max()
    larger = data.filter(c(col).gt(boundaries[1]))[col].min()
    if lower is None:
        lower = data[col].min()
    if larger is None:
        larger = data[col].max()

    return data.filter(c(col).ge(lower).and_(c(col).le(larger)))


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

def check_infeasible_constraints(model: pyo.Model) -> pl.DataFrame:
    infeasible_constraints: pl.DataFrame = pl.DataFrame(
        map(lambda x: [x[0].name, x[1], x[2]], find_infeasible_constraints(model)),
        schema = ["constraint", "value", "bound"]
    ).with_columns(
        c("constraint").str.replace("]", "").str.split("[").list.to_struct(fields=["name", "index"])
    ).unnest("constraint")\
    .with_columns(
        c("index").str.split(",").cast(pl.List(pl.Int32)).alias("index")
    )
    
    return infeasible_constraints


def remove_suffix(struct: pl.Expr) -> pl.Expr:
    return struct.name.map_fields(lambda x: "_".join(x.split("_")[:-1]))


    def extract_powered_volume_quota(self, first_stage_results: pl.DataFrame):
        offset = self.first_stage_nb_timestamp - first_stage_results.height%self.first_stage_nb_timestamp
        self.powered_volume_quota = first_stage_results\
            .select(
                c("T"),
                cs.starts_with("powered_volume").name.map(lambda c: c.replace("powered_volume_", "")),
            ).group_by(((c("T") + offset)//self.first_stage_nb_timestamp).alias("sim_nb"), maintain_order=True)\
            .agg(pl.all().exclude("sim_nb", "T").sum())\
            .unpivot(
                index="sim_nb", variable_name="H", value_name="powered_volume"
            ).with_columns(
                c("H").cast(pl.UInt32).alias("H")
            )  
