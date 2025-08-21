from altair import Opacity
import polars as pl
from polars import col as c



from plotly.subplots import make_subplots
import plotly.graph_objs as go
import plotly.express as px


from sklearn_extra.cluster import KMedoids
from plotly_calplot import calplot
import pandas as pd

from general_function import build_non_existing_dirs
from datetime import timedelta, date

def plot_discharge_flow_analysis(data: pl.DataFrame, kmedoid: KMedoids, plot_folder: str, year: int, n_clusters: int):
    fig = make_subplots(rows=n_clusters + 4, cols=1, vertical_spacing=0.02)
    colors = px.colors.qualitative.Plotly[:n_clusters]
    
    fig.add_trace(go.Scatter(x=data["timestamp"], y=data["value"], mode='lines', name='measurement'), row=1, col=1)
    fig.add_trace(go.Scatter(x=data["timestamp"], y=data["trend"], mode='lines', name='trend'), row=2, col=1)
    fig.add_trace(go.Scatter(x=data["timestamp"], y=data["resid"], mode='lines', name='resid'), row=3, col=1)
    fig.add_trace(go.Scatter(x=data["timestamp"], y=data["diff_resid"], mode='lines', name='diff_resid'), row=4, col=1)
    day_typical = pl.DataFrame(kmedoid.cluster_centers_.T, schema=[str(i) for i in range(n_clusters)]) # type: ignore
    
    for cluster in day_typical.columns:
        day_data = data.filter(c("cluster") == int(cluster))
        for day in day_data["day"].unique().to_list():
            fig.add_trace(
                go.Scatter(
                    x=list(range(24)), y=data.filter(c("day") == day).sort("timestamp")["diff_resid"].to_list(), 
                    mode='lines', name=cluster, 
                    marker=dict(color="grey"), opacity=0.3, showlegend=False
                ), row=int(cluster)+5, col=1
            )
            
        fig.add_trace(
            go.Scatter(
                x=list(range(24)), y=day_typical[cluster], mode='lines', 
                name=cluster, marker=dict(color=colors[int(cluster)])
            ), row=int(cluster)+5, col=1
        )
    fig.update_layout(
            margin=dict(t=60, l=65, r= 10, b=60), 
            width=1000,   # Set the width of the figure
            height=n_clusters*300,
        )

    fig.write_html(f"{plot_folder}/{year}_decomposition.html")

    df = pd.DataFrame({
        "scenario":kmedoid.labels_,
        "date": pd.date_range(start=f"{year}-01-01", periods=len(kmedoid.labels_), freq="D")
        })
    fig = calplot(df, x="date", y="scenario", colorscale=colors) # type: ignore
    fig.write_html(f"{plot_folder}/{year}_cluster_calendar.html")

    
def plot_syn_profile(syn_profile: pl.DataFrame, trend_data: pl.DataFrame, plot_folder: str, year: int=2018):
    
    build_non_existing_dirs(plot_folder)
    nb_profile  = len(syn_profile.columns)
    fig = make_subplots(rows=nb_profile, cols=1)
    
    

    timestamps = pl.datetime_range(
        start=date(2018, 1, 1), 
        end=date(2019, 1, 1), 
        interval=timedelta(hours=1), 
        eager=True, closed="left").to_list()

    for i, col in enumerate(syn_profile.columns):
        fig.add_trace(
            go.Scatter(
                x=timestamps, y=syn_profile[col], mode='lines', name=col,
                marker=dict(color="grey"), opacity=0.8, showlegend=False), row=i+1, col=1)
        fig.add_trace(
                go.Scatter(
                    x=timestamps, y=trend_data["mean"], mode='lines', name="mean",
                    marker=dict(color="red"), showlegend=False, opacity = 0.5), row=1 + i, col=1
            )
        fig.add_trace(
                go.Scatter(
                    x=timestamps, y=trend_data["min"], mode='lines', name="min",
                    marker=dict(color="red"), showlegend=False), row=1 + i, col=1
            )
    fig.update_layout(
                margin=dict(t=60, l=65, r= 10, b=60), 
                width=1000,   # Set the width of the figure
                height=nb_profile*300,
            )

    fig.write_html(f"{plot_folder}/discharge_flow_synthesized.html")