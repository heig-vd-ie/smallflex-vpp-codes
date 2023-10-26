from auxiliary.auxiliary import build_non_existing_dirs
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from schema.schema import Irradiation, WindSpeed, Temperature, DischargeFlow
import os
import polars as pl
import plotly.express as px
import webbrowser

def plot_meteo_data(fig_path, data):
    # non_existing dirs
    build_non_existing_dirs(os.path.dirname(fig_path))
    os.remove(fig_path)
    fig = []
    with open(fig_path, "a") as f:
        for index, (name, df) in enumerate(data.items()):
            arbitrary_date = pl.col("timestamp").dt.strftime("2030-%m-%d %H:%M:%S").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
            df = df.with_columns(arbitrary_date.alias("arbitrary_date")).sort("arbitrary_date").with_columns(pl.col("arbitrary_date").dt.strftime("%Y-%m-%d %H:%M:%S"))
            fig.append(px.line(df, x="arbitrary_date", y="value", color="year", height=400, labels={"arbitrary_date": "datetime", "value": name}))
            fig[index].update_traces(opacity=.4)
            fig[index].update_layout(xaxis=dict(tickformat="%B"))
            f.write(fig[index].to_html(full_html=False, include_plotlyjs='cdn'))
    webbrowser.open("file://" + os.path.realpath(fig_path))
    return fig


def query_time_series_data(db_cache_file, alt=None, river=None, tables=None):
    if tables is None:
        tables = [Irradiation, WindSpeed, Temperature, DischargeFlow]
    engine = create_engine(f"sqlite+pysqlite:///" + db_cache_file, echo=False)
    con = engine.connect()
    data = {}
    for table_schema in tables:
        table_schema_name = table_schema.__tablename__
        if table_schema_name in ["DischargeFlow"]:
            if river is not None:
                data[table_schema_name] = pl.read_database(query="""SELECT {f1}.timestamp, {f1}.value FROM {f1} where {f1}.river = \"{f2}\"""".format(f1=table_schema.__tablename__, f2=river), connection=con)
        else:
            if alt is not None:
                alts = pl.read_database(query="""SELECT DISTINCT {f1}.alt FROM {f1}""".format(f1=table_schema.__tablename__), connection=con)
                target_alt = min(alts["alt"].to_list(), key=lambda x: abs(alt - x))
                data[table_schema_name] = pl.read_database(query="""SELECT {f1}.timestamp, {f1}.value FROM {f1} where {f1}.alt = {f2}""".format(f1=table_schema.__tablename__, f2=target_alt), connection=con)
    return data

def fill_null_remove_outliers(data, d_time, z_score):
    d_time_int = int(d_time.split("h")[0]) if "h" in d_time else int(d_time.split("m")[0]) / 60 if "m" in d_time else RuntimeError
    for table_schema_name in data.keys():
        # outliers
        avg = data[table_schema_name].mean().get_column("value")[0]
        std = data[table_schema_name].std().get_column("value")[0]
        data[table_schema_name] = data[table_schema_name].with_columns(((pl.col("value") - avg) / std).alias("z_score"))
        data[table_schema_name] = data[table_schema_name].filter((pl.col("z_score") < z_score) & (pl.col("z_score") > -z_score))
        # fill missing values with data of previous day
        data[table_schema_name] = data[table_schema_name].with_columns(pl.col("timestamp").str.strptime(dtype=pl.Datetime, format="%Y-%m-%d %H:%M:%S%.6f")).set_sorted("timestamp", descending=False).upsample(time_column="timestamp", every=d_time)
        data[table_schema_name] = data[table_schema_name].with_columns(pl.col("timestamp").dt.hour().alias("hour")).sort("hour").with_columns(pl.all().forward_fill()).select(["timestamp", "value"]).sort("timestamp")
        # define week and year
        arbitrary_year = pl.col("timestamp").dt.strftime("2030-%m-%d %H:%M:%S").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
        data[table_schema_name] = data[table_schema_name].with_columns([arbitrary_year.dt.week().alias("week"), pl.col("timestamp").dt.year().alias("year")])
        # define time step
        data[table_schema_name] = data[table_schema_name].group_by(["week", "year"], maintain_order=True).agg([pl.col("timestamp"), pl.col("value")]).with_columns(pl.col("value").map_elements(lambda x: range(len(x))).alias("time_step"))
        data[table_schema_name] = data[table_schema_name].explode(["timestamp", "time_step", "value"]).select(["timestamp", "year", "week", "time_step", "value"])
        # remove first and end weeks to have consistent years
        data[table_schema_name] = data[table_schema_name].filter((pl.col("week") < 53) & (pl.col("time_step") < int(168 / d_time_int)))
    return data
