import plotly.graph_objs as go
import plotly.express as px
import polars as pl

from polars import selectors as cs
from plotly.subplots import make_subplots

COLORS = px.colors.qualitative.Plotly

def plot_second_stage_market_price(
    results: pl.DataFrame, fig: go.Figure, row: int, **kwargs
    ) -> go.Figure:


    fig.add_trace(
            go.Scatter(
                x=(results["timestamp"]).to_list(), 
                y=results["market_price"].to_list(), 
                legendgroup="market_price", name= "DA market price [EUR/MWh]",
                mode='lines', line=dict(color=COLORS[0]), showlegend=True
            ), row=row, col=1
        ) 
    
    fig.add_trace(
            go.Scatter(
                x=(results["timestamp"]).to_list(), 
                y=results["ancillary_market_price"].to_list(), 
                legendgroup="market_price", name= "Ancillary market_price price [EUR/MW]",
                mode='lines', line=dict(color=COLORS[1]), showlegend=True
            ), row=row, col=1
        ) 

    fig.update_traces(selector=dict(legendgroup="market_price"), legendgrouptitle_text="<b>Market prices<b>")
    return fig

def plot_basin_volume(
    results: pl.DataFrame,  fig: go.Figure, row: int
    ) -> go.Figure:
    name = ["Upstream basin", "Down stream basin"]
    for i, col in enumerate(results.select(cs.starts_with("basin_volume")).columns):
        fig.add_trace(
        go.Scatter(
            x=results["timestamp"].to_list(), y=results[col].to_list(),
            mode='lines', line=dict(color=COLORS[i]), showlegend=True, name=name[i],
            legendgroup="basin_volume",
        ), row=row, col=1)
    fig.update_traces(selector=dict(legendgroup="basin_volume"), legendgrouptitle_text="<b>Basin volume [%]<b>")
    return fig

def plot_hydro_power(
    results: pl.DataFrame, fig: go.Figure, row: int
    ):
    hydro_name = results.select(cs.starts_with("hydro_power")).columns
    name = ["Continuous turbine", "Pump"]

    for i, col in enumerate(hydro_name):
        
    
        fig.add_trace(
        go.Scatter(
                x=results["timestamp"].to_list(), y=results[col].to_list(),
                mode='lines', line=dict(color=COLORS[i]), showlegend=True, name=name[i],
                legendgroup="hydro_power",
            ), row=row, col=1)
    fig.update_traces(selector=dict(legendgroup="hydro_power"), legendgrouptitle_text="<b>Hydro power [MW]<b>")
    return fig

def plot_ancillary_power(
    results: pl.DataFrame, fig: go.Figure, row: int
    ):

        
    
    fig.add_trace(
    go.Scatter(
            x=results["timestamp"].to_list(), y=results["ancillary_power"].to_list(),
            mode='lines', line=dict(color=COLORS[0]), showlegend=True, name="ancillary_power",
            legendgroup="ancillary_power",
        ), row=row, col=1)
    fig.update_traces(selector=dict(legendgroup="ancillary_power"), legendgrouptitle_text="<b>Anxiliary power [MW]<b>")
    return fig

def plot_result(
    results: pl.DataFrame) -> go.Figure:

    fig = make_subplots(
            rows=4, cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
            row_titles= ["<b>Price<b>", "<b>Basin water volume<b>", "<b>Hydro power<b>", "<b>Anxiliary power<b>"])


    fig = plot_second_stage_market_price(results=results, fig=fig, row=1)
    fig = plot_basin_volume(results=results, fig=fig, row=2)
    fig = plot_hydro_power(results=results, fig=fig, row=3)
    fig = plot_ancillary_power(results=results, fig=fig, row=4)


    fig.update_layout(
            barmode='relative',
            margin=dict(t=60, l=65, r= 10, b=60), 
            width=1200,   # Set the width of the figure
            height=300*4,
            legend_tracegroupgap=215
        )
        
    
    return fig

# def plot_basin_volume(
#     simulation_results: pl.DataFrame,  fig: go.Figure, row: int, time_divider: int, **kwargs
#     ) -> go.Figure:
#     name = ["Upstream basin", "Down stream basin"]
#     for i, col in enumerate(simulation_results.select(cs.starts_with("basin_volume")).columns):
#         fig.add_trace(
#         go.Scatter(
#             x=simulation_results["T"].to_list(), y=simulation_results[col].to_list(),
#             mode='lines', line=dict(color=COLORS[i]), showlegend=True, name=name[i],
#             legendgroup="basin_volume",
#         ), row=row, col=1)
#     fig.update_traces(selector=dict(legendgroup="basin_volume"), legendgrouptitle_text="<b>Basin volume [%]<b>")
#     return fig


# def plot_first_stage_market_price(
#     simulation_results: pl.DataFrame, fig: go.Figure, row: int, time_divider: int, **kwargs
#     ) -> go.Figure:


#     fig.add_trace(
#             go.Scatter(
#                 x=(simulation_results["T"]/time_divider).to_list(), 
#                 y=simulation_results["market_price"].to_list(), 
#                 legendgroup="market_price", name= "Market price",
#                 mode='lines', line=dict(color=COLORS[0]), showlegend=True
#             ), row=row, col=1
#         ) 
#     fig.add_trace(
#             go.Scatter(
#                 x=(simulation_results["T"]/time_divider).to_list(), 
#                 y=simulation_results["max_market_price"].to_list(), 
#                 legendgroup="market_price", name= "Boundaries",
#                 mode='lines',line=dict(color="red"), showlegend=True
#             ), row=row, col=1
#         ) 
#     fig.add_trace(
#             go.Scatter(
#                 x=(simulation_results["T"]/time_divider).to_list(), 
#                 y=simulation_results["min_market_price"].to_list(), 
#                 legendgroup="market_price", name= "Boundaries",
#                 mode='lines',line=dict(color="red"), showlegend=False
#             ), row=row, col=1
#         ) 
#     fig.update_traces(selector=dict(legendgroup="market_price"), legendgrouptitle_text="DA market price")
#     return fig



# def plot_first_stage_powered_volume(
#     simulation_results: pl.DataFrame, fig: go.Figure, row: int, time_divider: int, **kwargs
#     ) -> go.Figure:
#     hydro_name = simulation_results.select(cs.starts_with("powered_volume")).columns
#     for i, name in enumerate(hydro_name):

#         data = simulation_results.select(c("T"), c(name))\
#             .group_by(c("T")//time_divider).agg(pl.all().exclude("T").sum()).sort("T")
#         fig.add_trace(
#             go.Bar(
#                 x=data["T"].to_list(), y=(data[name]).to_list(), showlegend=True,
#                 marker=dict(color=COLORS[i]), width=1, name=name,
#                 legendgroup="powered_volume"
#             ), row=row, col=1
#         )
        
#     fig.update_traces(selector=dict(legendgroup="powered_volume"), legendgrouptitle_text="Powered volume")

#     return fig

# def plot_first_stage_result(
#     simulation_results: pl.DataFrame,  time_divider: int) -> go.Figure:


#     fig: go.Figure = make_subplots(
#             rows=3, cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
#             row_titles= ["DA price [EUR/MWh]", "Basin water volume [%]", "Powered volume [Mm3]"])

#     kwargs: dict = {
#             "simulation_results": simulation_results, 
#             "fig": fig,
#             "time_divider": time_divider}

#     kwargs["fig"] = plot_first_stage_market_price(row=1, **kwargs)
#     kwargs["fig"] = plot_basin_volume(row=2, **kwargs)
#     fig = plot_first_stage_powered_volume(row=3, **kwargs)
#     fig.update_layout(
#             barmode='relative',
#             margin=dict(t=60, l=65, r= 10, b=60), 
#             width=1200,   # Set the width of the go.Figure
#             height=800,
#             legend_tracegroupgap=170

#     )
#     return fig

# def plot_second_stage_market_price(
#     simulation_results: pl.DataFrame, fig: go.Figure, row: int, **kwargs
#     ) -> go.Figure:


#     fig.add_trace(
#             go.Scatter(
#                 x=(simulation_results["timestamp"]).to_list(), 
#                 y=simulation_results["market_price"].to_list(), 
#                 legendgroup="market_price", name= "Market price",
#                 mode='lines', line=dict(color=COLORS[0]), showlegend=True
#             ), row=row, col=1
#         ) 

#     fig.update_traces(selector=dict(legendgroup="market_price"), legendgrouptitle_text="<b>DA market price [EUR/MWh]<b>")
#     return fig

# def plot_second_stage_powered_volume(
#     simulation_results: pl.DataFrame, fig: go.Figure, row: int, time_divider: int, **kwargs
#     ):
#     hydro_name = simulation_results.select(cs.starts_with("volume")).columns
#     name = ["Discrete turbine", "Continuous turbine", "Pump"]
#     for i, col in enumerate(hydro_name):
        
#         data = simulation_results.select(
#             c("T"), c(col)
#         ).group_by(c("T")//time_divider).agg(pl.all().exclude("T").sum()).sort("T")
        
            
#         fig.add_trace(
#             go.Bar(
#                 x=data["T"].to_list(), y=(data[col]).to_list(), showlegend=True,
#                 marker=dict(color=COLORS[i]), width=1, name= name[i],
#                 legendgroup="Powered volume"
#             ), row=row, col=1
#         )
#     fig.update_traces(selector=dict(legendgroup="Powered volume"), legendgrouptitle_text="<b>Powered volume [Mm3]<b>")
#     return fig

