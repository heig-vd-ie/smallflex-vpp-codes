import datetime
import numpy as np
from datetime import timedelta, date, datetime
import polars as pl
from polars import col as c
import polars.selectors as cs
from typing_extensions import Optional, Literal

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
        )

    return (
        datetime_index["index", "timestamp"]\
            .join(cleaned_data, left_on="timestamp", right_on=timestamp_col, how="left")\
            .with_columns(c(col_name).interpolate().forward_fill().backward_fill())
    )
    
def generate_segments(
        data: pl.DataFrame, x_col: str, y_col: str, min_x: float, max_x: float, n_segments: int):
    data = data.sort(x_col)
    segments = optimal_segments(
        x=np.array(data[x_col]), 
        y=np.array(data[y_col]), 
        n_segments=n_segments
    )

    return (
        data\
        .with_row_index(name= "val_index").filter(pl.col("val_index").is_in(segments))\
        .with_columns(
            c(x_col, y_col).name.prefix("min_"),
            c(x_col, y_col).shift(-1).name.prefix("max_")
        ).drop_nulls()\
        .with_row_index(name = "index")\
        .with_columns(
            ((c("max_" + y_col) - c("min_" + y_col))/
            (c("max_" + x_col) - c("min_" + x_col))).alias("dy_dx"),
        ).select(
            "index",
            pl.when(c("index") == 0).then(pl.lit(min_x)).otherwise(c("min_" + x_col)).alias("min_" + x_col),
            pl.when(c("index") == n_segments-1).then(pl.lit(max_x)).otherwise(c("max_" + x_col)).alias("max_" + x_col),
            "dy_dx",
            (1/c("dy_dx")).alias("dx_dy")
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
    ).to_frame(name="timestamp").with_row_index(name="index")

    first_datetime_index: pl.DataFrame = second_datetime_index.group_by_dynamic(
        every=first_time_delta, index_column="timestamp", start_by="datapoint", closed="left"
        ).agg(
            c("index").min().alias("index_min"),
            c("index").max().alias("index_max"),
            c("index").count().alias("n_index")
        ).with_row_index(name="index")

    return first_datetime_index, second_datetime_index
import sys
print(sys.path)