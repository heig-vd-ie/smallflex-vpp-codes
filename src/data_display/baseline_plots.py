from datetime import timedelta
import plotly.graph_objs as go
import plotly.express as px
import polars as pl
from polars import col as c
import numpy as np
from polars import selectors as cs
from plotly.subplots import make_subplots

import numpy as np

COLORS = px.colors.qualitative.Plotly

def cumsum_clip(a: np.ndarray, xmin=-np.inf, xmax=np.inf):
    res = np.empty_like(a)
    if a.ndim == 1:
        a = a[:, np.newaxis]
    res = np.empty_like(a)
    c = np.zeros(a.shape[1])
    for i in range(len(a)):
        c = np.minimum(np.maximum(c + a[i, :], xmin), xmax)
        res[i, :] = c
    if a.ndim == 1:
        res = res[:, 0]
    return res

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
            go.Bar(
                x=results["timestamp"].to_list(),
                y=results[col].to_list(),
                marker=dict(
                    color=COLORS[i],
                    line=dict(width=0)
                ), 
                width=timedelta(hours=1).total_seconds()*1000,
                showlegend=True,
                name=name[i],
                legendgroup="hydro_power",
                offsetgroup="hydro_power_group"
            ),
            row=row,
            col=1,
        )
    fig.update_traces(
        selector=dict(legendgroup="hydro_power"),
        legendgrouptitle_text="<b>Hydro power [MW]<b>",
    )
    return fig


def plot_ancillary_reserve(results: pl.DataFrame, fig: go.Figure, row: int, with_battery: bool = True) -> go.Figure:
    
    
    fig.add_trace(
        go.Bar(
            x=results["timestamp"].to_list(),
            y=results["hydro_ancillary_reserve"].to_list(),
            marker=dict(
                color=COLORS[0],
                line=dict(width=0)
            ),
            showlegend=True,
            width=timedelta(hours=1).total_seconds()*1000,
            name="hydro reserve",
            legendgroup="ancillary_reserve",
        ),
        row=row,
        col=1,
    )
    
    if with_battery:
        fig.add_trace(
            go.Bar(
                x=results["timestamp"].to_list(),
                y=results["battery_ancillary_reserve"].to_list(),
                marker=dict(
                    color=COLORS[1],
                    line=dict(width=0)
                ),
                width=timedelta(hours=1).total_seconds()*1000,
                showlegend=True,
                name="Battery reserve",
                legendgroup="ancillary_reserve",
            ),
            row=row,
            col=1,
        )
    fig.update_traces(
        selector=dict(legendgroup="ancillary_reserve"),
        legendgrouptitle_text="<b>Ancillary power [MW]<b>",
    )
    return fig


def plot_battery_soc(results: pl.DataFrame, fig: go.Figure, row: int) -> go.Figure:
    fig.add_trace(
        go.Scatter(
            x=results["timestamp"].to_list(),
            y=results["battery_soc"].to_list(),
            mode="lines",
            line=dict(color=COLORS[0]),
            showlegend=True,
            name="Battery SOC",
            legendgroup="battery_soc",
        ),
        row=row,
        col=1,
    )
    fig.update_traces(
        selector=dict(legendgroup="battery_soc"),
        legendgrouptitle_text="<b>Battery SOC [%]<b>",
    )
    return fig

def plot_battery_power(results: pl.DataFrame, fig: go.Figure, row: int) -> go.Figure:
    
    fig.add_trace(
        go.Bar(
            x=results["timestamp"].to_list(),
            y=results["battery_discharging_power"].to_list(),
            marker=dict(
                color=COLORS[0],
                line=dict(width=0)
            ),
            showlegend=True,
            width=timedelta(hours=1).total_seconds()*1000,
            name="Battery discharging power",
            legendgroup="battery_power",
        ),
        row=row,
        col=1,
    )
    
    fig.add_trace(
        go.Bar(
            x=results["timestamp"].to_list(),
            y=results["battery_charging_power"].to_list(),
            marker=dict(
                color=COLORS[1],
                line=dict(width=0)
            ),
            showlegend=True,
            width=timedelta(hours=1).total_seconds()*1000,
            name="Battery charging power",
            legendgroup="battery_power",
        ),
        row=row,
        col=1,
    )
    
    fig.update_traces(
        selector=dict(legendgroup="battery_power"),
        legendgrouptitle_text="<b>Battery power [MW]<b>",
    )
    return fig

def plot_result(
    results: pl.DataFrame,
    max_volume_mapping: dict[int, float],
    start_volume_mapping: dict[int, float],
    with_battery: bool = False,
) -> go.Figure:
    nb_subplot = 6 if with_battery else 4
    optimization_results = results.sort(["timestamp"]).with_columns(
        (
            (
                c(f"spilled_volume_{col}").shift(1) + c(f"basin_volume_{col}").diff()
            ).fill_null(start_volume_mapping[col])
            / max_volume_mapping[col]
            * 100
        ).alias(f"basin_volume_{col}")
        for col in start_volume_mapping.keys()
    )

    basin_volume_df = optimization_results.select(cs.starts_with("basin_volume_"))

    new_basin_volume = pl.DataFrame(
        cumsum_clip(basin_volume_df.to_numpy(), xmin=0, xmax=100),
        schema=basin_volume_df.columns,
    ).with_columns(optimization_results["timestamp"])
    optimization_results = optimization_results.drop(
        cs.starts_with("basin_volume_")
    ).join(new_basin_volume, on="timestamp", how="left")
    
    row_titles = [
        "<b>Price]<b>",
        "<b>Basin level [%]<b>",
        "<b>Hydro power [%]<b>",
        "<b>Ancillary reserve [MW]<b>",
    ]
    if with_battery:
        row_titles.append("<b>Battery power [MW]<b>")
        row_titles.append("<b>Battery SOC [%]<b>")

    fig = make_subplots(
        rows=nb_subplot,
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

    fig = plot_second_stage_market_price(results=optimization_results, fig=fig, row=1)
    fig = plot_basin_volume(results=optimization_results, fig=fig, row=2)
    fig = plot_hydro_power(results=optimization_results, fig=fig, row=3)
    fig = plot_ancillary_reserve(results=optimization_results, fig=fig, row=4, with_battery=with_battery)
    if with_battery:
        fig = plot_battery_power(results=optimization_results, fig=fig, row=5)
        fig = plot_battery_soc(results=optimization_results, fig=fig, row=6)


    fig.update_layout(
        margin=dict(t=60, l=65, r=10, b=60),
        width=1200,  # Set the width of the figure
        height=300 * nb_subplot,
        legend_tracegroupgap=215,
        barmode = "stack"
    )

    return fig


def plot_scenario_results(
    optimization_results: pl.DataFrame,
    max_volume_mapping: dict[int, float],
    start_volume_mapping: dict[int, float],
) -> go.Figure:

    optimization_results = optimization_results.sort(["Ω", "T"]).with_columns(
        (
            (
                c(f"spilled_volume_{col}").shift(1)
                + c(f"basin_volume_{col}").diff().over("Ω")
            ).fill_null(start_volume_mapping[col])
            / max_volume_mapping[col]
            * 100
        ).alias(f"basin_volume_{col}")
        for col in start_volume_mapping.keys()
    )
    col = list(start_volume_mapping.keys())[0]

    new_optimization_results = optimization_results.drop(
        [f"basin_volume_{col}" for col in start_volume_mapping.keys()]
    )
    for col in start_volume_mapping.keys():
        basin_volume_df = optimization_results.pivot(
            on="Ω", values=f"basin_volume_{col}", index="T"
        )

        new_basin_volume_df = (
            pl.DataFrame(
                cumsum_clip(basin_volume_df.drop("T").to_numpy(), xmin=0, xmax=100),
                schema=basin_volume_df.drop("T").columns,
            )
            .with_columns(basin_volume_df["T"])
            .unpivot(variable_name="Ω", value_name=f"basin_volume_{col}", index="T")
            .with_columns(c("Ω").cast(pl.UInt32))
        )
        new_optimization_results = new_optimization_results.join(
            new_basin_volume_df, on=["T", "Ω"], how="left"
        )
    optimization_results = new_optimization_results

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        x_title="<b>Weeks<b>",
        row_titles=[
            "<b>Price [CHF/MWh]<b>",
            "<b>Discharge volume [m³/s]<b>",
            "<b>Basin level [%]<b>",
            "<b>Hydropower [MW]<b>",
        ],
    )

    for idx, col_name in enumerate(
        ["market_price", "discharge_volume_0", "basin_volume_0"]
    ):

        data = optimization_results.pivot(index="T", on="Ω", values=col_name)

        for i in data.drop("T").columns:
            fig.add_trace(
                go.Scatter(
                    x=data["T"].to_list(),
                    y=data[i].to_list(),
                    mode="lines",
                    opacity=0.3,
                    name=f"Scenario {i}",
                    line=dict(color="grey"),
                    showlegend=False,
                    hovertemplate=f"Scenario {i}<br>"
                    + "Time: %{x}<br>"
                    + "Market price: %{y:.2f} €/MWh",
                ),
                row=idx + 1,
                col=1,
            )

        stat_data = data.drop("T").select(
            pl.concat_list(pl.all())
            .map_elements(lambda x: np.quantile(np.array(x), 0.9))
            .alias("90-quantile"),
            pl.concat_list(pl.all())
            .map_elements(lambda x: np.quantile(np.array(x), 0.1))
            .alias("10-quantile"),
            pl.concat_list(pl.all())
            .map_elements(lambda x: np.quantile(np.array(x), 0.5))
            .alias("median"),
        )

        for i in stat_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data["T"].to_list(),
                    y=stat_data[i].to_list(),
                    mode="lines",
                    name=f"{i}",
                    line=dict(color="red" if i == "median" else "orange"),
                    showlegend=False,
                ),
                row=idx + 1,
                col=1,
            )

    hydro_name = optimization_results.select(cs.starts_with("hydro_power")).columns

    for i, col in enumerate(hydro_name):

        fig.add_trace(
            go.Bar(
                x=optimization_results.filter(c("Ω") == optimization_results["Ω"][0])[
                    "T"
                ].to_list(),
                y=optimization_results.filter(c("Ω") == optimization_results["Ω"][0])[
                    col
                ].to_list(),
                marker=dict(color=COLORS[i]),
                showlegend=False,
                width=1,
            ),
            row=4,
            col=1,
        )

    fig.update_layout(
        barmode="relative",
        margin=dict(t=60, l=65, r=10, b=60),
        width=1200,  # Set the width of the figure
        height=300 * 4,
    )
    return fig
