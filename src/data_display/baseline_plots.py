import plotly.graph_objs as go
import plotly.express as px
import polars as pl
from polars import col as c
import pyomo.environ as pyo
from polars import selectors as cs
from plotly.subplots import make_subplots
from plotly.graph_objects import Figure

from utility.pyomo_preprocessing import extract_optimization_results, linear_interpolation_using_cols

from general_function import build_non_existing_dirs

COLORS = px.colors.qualitative.Plotly



def plot_basin_volume(
    simulation_results: pl.DataFrame,  fig: Figure, row: int, time_divider: int, **kwargs
    ) -> Figure:
    name = ["Upstream basin", "Down stream basin"]
    for i, name in enumerate(simulation_results.select(cs.starts_with("basin_volume")).columns):
        fig.add_trace(
        go.Scatter(
            x=(simulation_results["T"]/time_divider).to_list(), y=simulation_results[name].to_list(),
            mode='lines', line=dict(color=COLORS[i]), showlegend=True, name=name[i],
            legendgroup="basin_volume",
        ), row=row, col=1)
    fig.update_traces(selector=dict(legendgroup="basin_volume"), legendgrouptitle_text="<b>Basin volume [%]<b>")
    return fig


def plot_first_stage_market_price(
    simulation_results: pl.DataFrame, fig: Figure, row: int, time_divider: int, **kwargs
    ) -> Figure:


    fig.add_trace(
            go.Scatter(
                x=(simulation_results["T"]/time_divider).to_list(), 
                y=simulation_results["market_price"].to_list(), 
                legendgroup="market_price", name= "Market price",
                mode='lines', line=dict(color=COLORS[0]), showlegend=True
            ), row=row, col=1
        ) 
    fig.add_trace(
            go.Scatter(
                x=(simulation_results["T"]/time_divider).to_list(), 
                y=simulation_results["max_market_price"].to_list(), 
                legendgroup="market_price", name= "Boundaries",
                mode='lines',line=dict(color="red"), showlegend=True
            ), row=row, col=1
        ) 
    fig.add_trace(
            go.Scatter(
                x=(simulation_results["T"]/time_divider).to_list(), 
                y=simulation_results["min_market_price"].to_list(), 
                legendgroup="market_price", name= "Boundaries",
                mode='lines',line=dict(color="red"), showlegend=False
            ), row=row, col=1
        ) 
    fig.update_traces(selector=dict(legendgroup="market_price"), legendgrouptitle_text="DA market price")
    return fig



def plot_first_stage_powered_volume(
    simulation_results: pl.DataFrame, fig: Figure, row: int, time_divider: int, **kwargs
    ) -> Figure:
    hydro_name = simulation_results.select(cs.starts_with("powered_volume")).columns
    for i, name in enumerate(hydro_name):

        data = simulation_results.select(c("T"), c(name))\
            .group_by(c("T")//time_divider).agg(pl.all().exclude("T").sum()).sort("T")
        fig.add_trace(
            go.Bar(
                x=data["T"].to_list(), y=(data[name]).to_list(), showlegend=True,
                marker=dict(color=COLORS[i]), width=1, name=name,
                legendgroup="powered_volume"
            ), row=row, col=1
        )
        
    fig.update_traces(selector=dict(legendgroup="powered_volume"), legendgrouptitle_text="Powered volume")

    return fig

def plot_first_stage_result(
    simulation_results: pl.DataFrame,  time_divider: int) -> Figure:


    fig: Figure = make_subplots(
            rows=3, cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
            row_titles= ["DA price [EUR/MWh]", "Basin water volume [%]", "Powered volume [Mm3]"])

    kwargs: dict = {
            "simulation_results": simulation_results, 
            "fig": fig,
            "time_divider": time_divider}

    kwargs["fig"] = plot_first_stage_market_price(row=1, **kwargs)
    kwargs["fig"] = plot_basin_volume(row=2, **kwargs)
    fig = plot_first_stage_powered_volume(row=3, **kwargs)
    fig.update_layout(
            barmode='relative',
            margin=dict(t=60, l=65, r= 10, b=60), 
            width=1200,   # Set the width of the figure
            height=800,
            legend_tracegroupgap=170

    )
    return fig

def plot_second_stage_market_price(
    simulation_results: pl.DataFrame, fig: Figure, row: int, time_divider: int, **kwargs
    ) -> Figure:


    fig.add_trace(
            go.Scatter(
                x=(simulation_results["T"]/time_divider).to_list(), 
                y=simulation_results["market_price"].to_list(), 
                legendgroup="market_price", name= "Market price",
                mode='lines', line=dict(color=COLORS[0]), showlegend=True
            ), row=row, col=1
        ) 

    fig.update_traces(selector=dict(legendgroup="market_price"), legendgrouptitle_text="<b>DA market price [EUR/MW]<b>")
    return fig

def plot_second_stage_powered_volume(
    simulation_results: pl.DataFrame, fig: Figure, row: int, time_divider: int, **kwargs
    ):
    hydro_name = simulation_results.select(cs.starts_with("volume")).columns
    name = ["Discrete turbine", "Continuous turbine", "Pump"]
    for i, col in enumerate(hydro_name):
        
        data = simulation_results.select(
            c("T"), c(col)
        ).group_by(c("T")//time_divider).agg(pl.all().exclude("T").sum()).sort("T")
        
            
        fig.add_trace(
            go.Bar(
                x=data["T"].to_list(), y=(data[col]).to_list(), showlegend=True,
                marker=dict(color=COLORS[i]), width=1, name= name[i],
                legendgroup="Powered volume"
            ), row=row, col=1
        )
    fig.update_traces(selector=dict(legendgroup="Powered volume"), legendgrouptitle_text="<b>Powered volume [Mm3]<b>")
    return fig

def plot_second_stage_result(
    simulation_results: pl.DataFrame,  time_divider: int) -> Figure:

    fig: Figure = make_subplots(
            rows=3, cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
            row_titles= ["<b>DA price<b>", "<b>Basin water volume<b>", "<b>Powered volume<b>"])

    kwargs: dict = {
            "simulation_results": simulation_results.rename({"real_index": "T"}), 
            "fig": fig,
            "time_divider": time_divider}

    kwargs["fig"] = plot_second_stage_market_price(row=1, **kwargs)
    kwargs["fig"] = plot_basin_volume(row=2, **kwargs)
    fig = plot_second_stage_powered_volume(row=3, **kwargs)
    fig.update_layout(
            barmode='relative',
            margin=dict(t=60, l=65, r= 10, b=60), 
            width=1200,   # Set the width of the figure
            height=800,
            legend_tracegroupgap=180
        )
    
    return fig