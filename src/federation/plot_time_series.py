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
        if fig_path is not None:
            with open(fig_path, "a") as f:
                f.write(fig[index].to_html(full_html=False, include_plotlyjs='cdn'))
    if to_be_open:
        webbrowser.open("file://" + os.path.realpath(fig_path))
    return fig


def plot_scenarios(data, fig_path, to_be_open=False, horizon="RT"):
    # non_existing dirs
    build_non_existing_dirs(os.path.dirname(fig_path))
    if os.path.isfile(fig_path):
        os.remove(fig_path)
    fig = []
    with open(fig_path, "a") as f:
        for index, (name, df) in enumerate(data.items()):
            if "timestamp" not in df.columns:
                df = df.with_columns(pl.struct(["week", "time_step"]).map_elements(lambda x: datetime.strptime("2030-{}-{} {}:00".format(x["week"] -1, int(x["time_step"] / 24) + 1, x["time_step"] - int(x["time_step"] / 24) * 24), "%Y-%W-%u %H:%M")).alias("timestamp")).sort("timestamp")
            if "horizon" in df.columns:
                df = df.with_columns(pl.struct(["scenario", "horizon"]).map_elements(lambda x: x["scenario"] + "_" + x["horizon"]).alias("scenario")).select(["timestamp", "scenario", "value"])
            else:
                df = df.select(["timestamp", "scenario", "value"])
            arbitrary_date = pl.col("timestamp").dt.strftime("2030-%m-%d %H:%M:%S").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
            df = df.with_columns(arbitrary_date.alias("arbitrary_date")).sort("arbitrary_date").with_columns(pl.col("arbitrary_date").dt.strftime("%Y-%m-%d %H:%M:%S"))
            fig.append(px.line(df, x="arbitrary_date", y="value", color="scenario", height=400, labels={"arbitrary_date": "datetime", "value": name}))
            fig[index].update_traces(opacity=.4)
            fig[index].update_layout(xaxis=dict(tickformat="%B"))
            f.write(fig[index].to_html(full_html=False, include_plotlyjs='cdn'))
    if to_be_open:
        webbrowser.open("file://" + os.path.realpath(fig_path))
    return fig
