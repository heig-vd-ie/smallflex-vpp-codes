from auxiliary.auxiliary import build_non_existing_dirs
from plotly.subplots import make_subplots
from datetime import datetime
import polars as pl
import plotly as plt
import plotly.offline
import plotly.express as px
import plotly.graph_objs as go
import webbrowser
import os


def plot_time_series(data, fig_path=None, to_be_open=False):
    # non_existing dirs
    if fig_path is not None:
        build_non_existing_dirs(os.path.dirname(fig_path))
        if os.path.isfile(fig_path):
            os.remove(fig_path)
    fig = []
    for index, (name, df) in enumerate(data.items()):
        df = df.select(["timestamp", "value"])
        if df["timestamp"].dtype == pl.Utf8:
            df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime("ns"), format="%Y-%m-%d %H:%M:%S.000000"))
        arbitrary_date = pl.col("timestamp").dt.strftime("2030-%m-%d %H:%M:%S").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
        df = df.with_columns(arbitrary_date.alias("arbitrary_date")).sort("arbitrary_date").with_columns(pl.col("arbitrary_date").dt.strftime("%Y-%m-%d %H:%M:%S"))
        df = df.with_columns(pl.col("timestamp").dt.year().cast(pl.Int64).alias("year")).sort("timestamp")
        fig.append(px.line(df, x="arbitrary_date", y="value", color="year", height=400, labels={"arbitrary_date": "datetime", "value": name}))
        fig[index].update_traces(opacity=.4)
        fig[index].update_layout(xaxis=dict(tickformat="%B"))

    return fig
