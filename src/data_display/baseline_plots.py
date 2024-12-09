import plotly.graph_objs as go
import plotly.express as px
import polars as pl
from polars import col as c
import pyomo.environ as pyo
from polars import selectors as cs
from plotly.subplots import make_subplots
from plotly.graph_objects import Figure

from utility.pyomo_preprocessing import extract_optimization_results, linear_interpolation_using_cols

from utility.general_function import build_non_existing_dirs

COLORS = px.colors.qualitative.Plotly



def plot_basin_volume(
    simulation_results: pl.DataFrame,  fig: Figure, row: int, time_divider: int, **kwargs
    ) -> Figure:

    for i, name in enumerate(simulation_results.select(cs.starts_with("basin_volume")).columns):
        fig.add_trace(
        go.Scatter(
            x=(simulation_results["T"]/time_divider).to_list(), y=simulation_results[name].to_list(),
            mode='lines', line=dict(color=COLORS[i]), showlegend=True, name=name.replace("_", " "),
            legendgroup="basin_volume",
        ), row=row, col=1)
    fig.update_traces(selector=dict(legendgroup="basin_volume"), legendgrouptitle_text="Basin volume")
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
    hydro_name = simulation_results.select(cs.starts_with("hydro")).columns
    for fig_idx, name in enumerate(hydro_name):

        data = simulation_results.select(c("T"), c(name)).unnest(name)\
            .group_by(c("T")//time_divider).agg(pl.all().exclude("T").sum()).sort("T")
        for i, var_name in enumerate(["turbined_volume", "pumped_volume"]):
            factor = 1e-6 if i == 0 else -1e-6
            fig.add_trace(
                go.Bar(
                    x=data["T"].to_list(), y=(factor*data[var_name]).to_list(), showlegend=True,
                    marker=dict(color=COLORS[i]), width=1, name= var_name.replace("_", " ") + " " + name.replace("_", " "),
                    legendgroup=name
                ), row=row +fig_idx, col=1
            )
    fig.update_traces(selector=dict(legendgroup=name), legendgrouptitle_text= name )

    return fig

def plot_first_stage_result(
    simulation_results: pl.DataFrame,  time_divider: int) -> Figure:

        hydro_name = list(map(
            lambda x: x.replace("_", "") + " volume [Mm3]", 
            simulation_results.select(cs.starts_with("hydro")).columns))

        fig: Figure = make_subplots(
                rows=2 + len(hydro_name), cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
                row_titles= ["DA price [EUR/MWh]", "Basin water volume [%]"] + hydro_name)

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
                legend=dict(title_text="Legend"),
                legend_tracegroupgap=50
            )
        return fig
    
def plot_curviness_results(df: pl.DataFrame, x_col: str, y_col_list: list[str]) -> Figure:

    fig: Figure = make_subplots(
        rows=2, cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title=x_col, 
        row_titles= ["value", "k"])

    for i, y_col in enumerate(y_col_list):
        fig.add_trace(
            go.Scatter(
                x=df[x_col].to_list(), 
                y=df[y_col].to_list(), 
                legendgroup=y_col, name= y_col,
                mode='lines',line=dict(color=COLORS[i]), showlegend=True
            )
            ,row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=df[x_col].to_list(), 
                y=df[f"k_{y_col}"].to_list(), 
                legendgroup=y_col, name= y_col,
                mode='lines',line=dict(color=COLORS[i]), showlegend=False
            )
            ,row=2, col=1
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

    fig.update_traces(selector=dict(legendgroup="market_price"), legendgrouptitle_text="DA market price")
    return fig

def plot_second_stage_powered_volume(
    simulation_results: pl.DataFrame, fig: Figure, row: int, time_divider: int, **kwargs
    ):
    hydro_name = simulation_results.select(cs.starts_with("volume")).columns
    for fig_idx, col in enumerate(hydro_name):
        name = col.replace("volume_", "hydro ") 
        data = simulation_results.select(
            c("T"),
            pl.when(c(col) <= 0).then(0).otherwise(c(col)).alias("turbined_volume"),
            pl.when(c(col) >= 0).then(0).otherwise(- c(col)).alias("pumped_volume"),
        ).group_by(c("T")//time_divider).agg(pl.all().exclude("T").sum()).sort("T")
        
        for i, var_name in enumerate(["turbined_volume", "pumped_volume"]):
            factor = 1e-6 if i == 0 else -1e-6
            fig.add_trace(
                go.Bar(
                    x=data["T"].to_list(), y=(factor*data[var_name]).to_list(), showlegend=True,
                    marker=dict(color=COLORS[i]), width=1, name= var_name.replace("_", " ") + " " + name.replace("_", " "),
                    legendgroup=col
                ), row=row +fig_idx, col=1
            )
    return fig
def plot_second_stage_result(
    simulation_results: pl.DataFrame,  time_divider: int) -> Figure:

        hydro_name = list(map(
            lambda x: x.replace("_", "") + " volume [Mm3]", 
            simulation_results.select(cs.starts_with("volume")).columns))

        fig: Figure = make_subplots(
                rows=2 + len(hydro_name), cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
                row_titles= ["DA price [EUR/MWh]", "Basin water volume [%]"] + hydro_name)

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
                legend=dict(title_text="Legend"),
                legend_tracegroupgap=50
            )
        return fig