import plotly.graph_objs as go
import plotly.express as px
import polars as pl
from polars import col as c
import numpy as np
from polars import selectors as cs
from plotly.subplots import make_subplots

COLORS = px.colors.qualitative.Plotly


def plot_second_stage_market_price(
    results: pl.DataFrame, fig: go.Figure, row: int
) -> go.Figure:

    fig.add_trace(
        go.Scatter(
            x=(results["timestamp"]).to_list(),
            y=results["market_price"].to_list(),
            legendgroup="market_price",
            name="DA market price [EUR/MWh]",
            mode="lines",
            line=dict(color=COLORS[0]),
            showlegend=True,
        ),
        row=row,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=(results["timestamp"]).to_list(),
            y=results["ancillary_market_price"].to_list(),
            legendgroup="market_price",
            name="Ancillary market_price price [EUR/MW]",
            mode="lines",
            line=dict(color=COLORS[1]),
            showlegend=True,
        ),
        row=row,
        col=1,
    )

    fig.update_traces(
        selector=dict(legendgroup="market_price"),
        legendgrouptitle_text="<b>Market prices<b>",
    )
    return fig


def plot_basin_volume(results: pl.DataFrame, fig: go.Figure, row: int) -> go.Figure:
    name = ["Upstream basin", "Down stream basin"]
    for i, col in enumerate(results.select(cs.starts_with("basin_volume")).columns):
        fig.add_trace(
            go.Scatter(
                x=results["timestamp"].to_list(),
                y=results[col].to_list(),
                mode="lines",
                line=dict(color=COLORS[i]),
                showlegend=True,
                name=name[i],
                legendgroup="basin_volume",
            ),
            row=row,
            col=1,
        )
    fig.update_traces(
        selector=dict(legendgroup="basin_volume"),
        legendgrouptitle_text="<b>Basin volume [%]<b>",
    )
    return fig


def plot_hydro_power(results: pl.DataFrame, fig: go.Figure, row: int):
    hydro_name = results.select(cs.starts_with("hydro_power")).columns
    name = ["Continuous turbine", "Pump"]

    for i, col in enumerate(hydro_name):

        fig.add_trace(
            go.Scatter(
                x=results["timestamp"].to_list(),
                y=results[col].to_list(),
                mode="lines",
                line=dict(color=COLORS[i]),
                showlegend=True,
                name=name[i],
                legendgroup="hydro_power",
            ),
            row=row,
            col=1,
        )
    fig.update_traces(
        selector=dict(legendgroup="hydro_power"),
        legendgrouptitle_text="<b>Hydro power [MW]<b>",
    )
    return fig


def plot_ancillary_power(results: pl.DataFrame, fig: go.Figure, row: int):

    fig.add_trace(
        go.Scatter(
            x=results["timestamp"].to_list(),
            y=results["ancillary_power"].to_list(),
            mode="lines",
            line=dict(color=COLORS[0]),
            showlegend=True,
            name="ancillary_power",
            legendgroup="ancillary_power",
        ),
        row=row,
        col=1,
    )
    fig.update_traces(
        selector=dict(legendgroup="ancillary_power"),
        legendgrouptitle_text="<b>Ancillary power [MW]<b>",
    )
    return fig


def plot_result(
    results: pl.DataFrame, max_volume_mapping: dict[int, float]
) -> go.Figure:

    results = results.with_columns(
        (
            c(f"basin_volume_{basin}")
            / max_volume_mapping[basin]
            * 100
        )
        .clip(0, 100)
        .alias(f"basin_volume_{basin}")
        for basin in max_volume_mapping.keys()
    )

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        x_title="<b>Weeks<b>",
        row_titles=[
            "<b>Price<b>",
            "<b>Basin water volume<b>",
            "<b>Hydro power<b>",
            "<b>Ancillary power<b>",
        ],
    )

    fig = plot_second_stage_market_price(results=results, fig=fig, row=1)
    fig = plot_basin_volume(results=results, fig=fig, row=2)
    fig = plot_hydro_power(results=results, fig=fig, row=3)
    fig = plot_ancillary_power(results=results, fig=fig, row=4)

    fig.update_layout(
        barmode="relative",
        margin=dict(t=60, l=65, r=10, b=60),
        width=1200,  # Set the width of the figure
        height=300 * 4,
        legend_tracegroupgap=215,
    )

    return fig




def plot_scenario_results(optimization_results: pl.DataFrame, max_volume_mapping: dict[int, float]) -> go.Figure:

    optimization_results = optimization_results.with_columns(
        (c(f"spilled_volume_{basin}").cum_sum().over("Ω") - c(f"spilled_volume_{basin}")).alias(f"spilled_volume_{basin}")
        for basin in max_volume_mapping.keys()
        ).with_columns(
            ((c(f"basin_volume_{basin}") + c(f"spilled_volume_{basin}"))/max_volume_mapping[basin]*100).clip(0, 100).alias(f"basin_volume_{basin}")
            for basin in max_volume_mapping.keys()
        )

    fig = make_subplots(
                rows=4, cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
                row_titles= ["<b>Price [CHF/MWh]<b>", "<b>Discharge volume [m³/s]<b>", "<b>Basin level [%]<b>", "<b>Hydropower [MW]<b>"])

    for idx, col_name in enumerate(["market_price", "discharge_volume_0", "basin_volume_0"]):

        data = optimization_results.pivot(
                index="T", on="Ω", values=col_name
            )

        for i in data.drop("T").columns:    
                fig.add_trace(
                    go.Scatter(
                        x=data["T"].to_list(), 
                        y=data[i].to_list(), mode="lines", opacity=.3, name=f"Scenario {i}", line=dict(color="grey"),

                        showlegend=False, hovertemplate = f"Scenario {i}<br>" + "Time: %{x}<br>" + "Market price: %{y:.2f} €/MWh"
                    ),
                    row=idx+1, col=1
                )
                
        stat_data = data.drop("T").select(
            pl.concat_list(pl.all()).map_elements(lambda x: np.quantile(np.array(x), 0.9)).alias("90-quantile"),
            pl.concat_list(pl.all()).map_elements(lambda x: np.quantile(np.array(x), 0.1)).alias("10-quantile"),
            pl.concat_list(pl.all()).map_elements(lambda x: np.quantile(np.array(x), 0.5)).alias("median")
        )

        for i in stat_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data["T"].to_list(), 
                    y=stat_data[i].to_list(), mode="lines", name=f"{i}", line=dict(color="red" if i=="median" else "orange"),
                    
                    showlegend=False
                ),
                row=idx+1, col=1
            )

    hydro_name = optimization_results.select(cs.starts_with("hydro_power")).columns

    for i, col in enumerate(hydro_name):
        

        fig.add_trace(
            go.Bar(
                x=optimization_results.filter(c("Ω") == optimization_results["Ω"][0])["T"].to_list(), 
                y=optimization_results.filter(c("Ω") == optimization_results["Ω"][0])[col].to_list(),
                marker=dict(color=COLORS[i]), showlegend=False, width=1
            ), row=4, col=1)

    fig.update_layout(
            barmode='relative',
            margin=dict(t=60, l=65, r= 10, b=60), 
            width=1200,   # Set the width of the figure
            height=300*4,
        )
    return fig