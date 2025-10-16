from datetime import timedelta
from matplotlib.pylab import f
import plotly.graph_objs as go
import plotly.express as px
import polars as pl
from polars import col as c
import numpy as np
from polars import selectors as cs
from plotly.subplots import make_subplots
from numpy_function import clipped_cumsum
from general_function import pl_to_dict
import numpy as np


COLORS = px.colors.qualitative.Plotly


def plot_second_stage_market_price(
    results: pl.DataFrame, market_price_quantiles: pl.DataFrame, fig: go.Figure, row: int
) -> go.Figure:
    
    fig.add_trace(
        go.Scatter(
            x=(market_price_quantiles["timestamp"]).to_list(),
            y=market_price_quantiles["market_price_lower_quantile"].to_list(),
            legendgroup="market_price",
            name="quantiles",
            mode="lines",
            line=dict(color="purple"),
            showlegend=True,
            opacity=0.7,
        ),
        row=row,
        col=1,
    )
    
    fig.add_trace(
        go.Scatter(
            x=(market_price_quantiles["timestamp"]).to_list(),
            y=market_price_quantiles["market_price_upper_quantile"].to_list(),
            legendgroup="market_price",
            name="upper_quantile",
            mode="lines",
            line=dict(color="purple"),
            opacity=0.7,
            showlegend=False,
        ),
        row=row,
        col=1,
    )

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
            name="Ancillary market price [EUR/MWh]",
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

def plot_second_stage_basin_volume(
    results: pl.DataFrame, basin_volume_expectation: pl.DataFrame, 
    water_basin: pl.DataFrame, fig: go.Figure, row: int) -> go.Figure:
    
    basin_idx = water_basin["B"].to_list()
    
    max_volume_mapping = pl_to_dict(water_basin["B", "volume_max"])
    min_volume_mapping = pl_to_dict(water_basin["B", "volume_min"])
    start_volume_mapping = pl_to_dict(water_basin["B", "start_volume"])
    
    basin_volume_expectation = basin_volume_expectation.join(
        results.filter(c("sim_idx").is_first_distinct())["timestamp", "sim_idx"],
        on="sim_idx", how="inner"
    ).with_columns(
        (c("mean", "lower_quantile", "upper_quantile") - c("B").replace(min_volume_mapping)) 
        / (c("B").replace(max_volume_mapping) - c("B").replace(min_volume_mapping))* 100
    ).sort("timestamp")
    
    
    basin_volume_raw = results.select(
        "timestamp", cs.contains("basin_volume_"), cs.contains("spilled_volume_")
    )


    basin_volume_raw = basin_volume_raw.sort(["timestamp"]).select(
        "timestamp",
        *[(
            (
                c(f"spilled_volume_{col}").shift(1)
                + c(f"basin_volume_{col}").diff()
            ).fill_null(start_volume_mapping[col])
            / (max_volume_mapping[col] - min_volume_mapping[col])
            * 100
        ).alias(f"basin_volume_{col}")
        for col in basin_idx]
    )


    cleaned_basin_volume = (
        pl.DataFrame(
            clipped_cumsum(basin_volume_raw.drop("timestamp").to_numpy(), xmin=0, xmax=100),
            schema=basin_volume_raw.drop("timestamp").columns,
        ).with_columns(basin_volume_raw["timestamp"])
    )
    
    print(cleaned_basin_volume)

    

    fig.add_trace(
        go.Scatter(
            x=cleaned_basin_volume["timestamp"].to_list(),
            y=cleaned_basin_volume["basin_volume_0"].to_list(),
            mode="lines",
            line=dict(color=COLORS[0]),
            showlegend=True,
            name="basin_volume",
            legendgroup="basin_volume",
        ),
        row=row,
        col=1,
    )
    
    fig.add_trace(go.Scatter(
        x=basin_volume_expectation["timestamp"],
        y=basin_volume_expectation["mean"],
        mode='lines',
        line=dict(color='red'),
        opacity=0.5,
        name='Scheduled basin volume',
        legendgroup="basin_volume",
    ),row=row,
    col=1)

    fig.add_trace(go.Scatter(
        x=basin_volume_expectation["timestamp"],
        y=basin_volume_expectation["upper_quantile"],
        mode='lines',
        line=dict(width=0.5, color='red'),
        name='Lower Bound',
        showlegend=False,
        legendgroup="basin_volume",
    ), row=row,
    col=1)
    fig.add_trace(go.Scatter(
        x=basin_volume_expectation["timestamp"],
        y=basin_volume_expectation["lower_quantile"],
        mode='lines',
        fill='tonexty',
        line=dict(width=0.5, color='red'),
        fillcolor='rgba(255, 0, 0, 0.2)',
        name='Scheduled bounds',
        legendgroup="basin_volume",
    ), row=row,
    col=1)
    
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
        clipped_cumsum(basin_volume_df.to_numpy(), xmin=0, xmax=100),
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
    water_basin: pl.DataFrame
) -> go.Figure:

    max_volume_mapping = pl_to_dict(water_basin["B", "volume_max"])
    min_volume_mapping = pl_to_dict(water_basin["B", "volume_min"])
    start_volume_mapping = pl_to_dict(water_basin["B", "start_volume"])
    optimization_results = optimization_results.sort(["Ω", "T"]).with_columns(
        (
            (
                c(f"spilled_volume_{col}").shift(1)
                + c(f"basin_volume_{col}").diff().over("Ω")
            ).fill_null(start_volume_mapping[col])
            / (max_volume_mapping[col] - min_volume_mapping[col])
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
                clipped_cumsum(basin_volume_df.drop("T").to_numpy(), xmin=0, xmax=100),
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

        
        stat_data = optimization_results.group_by("T")\
        .agg(
            c(col_name).median().alias("median"),
            c(col_name).quantile(0.1).alias("10_quantile"),
            c(col_name).quantile(0.9).alias("90_quantile")
        ).sort("T")

        if col_name == "basin_volume_0":
            stat_data = stat_data.with_columns(
                c("10_quantile").clip(upper_bound=c("median") - 10).clip(lower_bound=0),
                c("90_quantile").clip(lower_bound=c("median") + 10).clip(upper_bound=100)
            )

            mean_stat_data = stat_data.with_columns(
                c("median", "10_quantile", "90_quantile").rolling_mean(window_size=7).shift(-4)
            )
            
            stat_data = pl.concat([
                stat_data.slice(0,1), #first row
                mean_stat_data.slice(1,mean_stat_data.height-2), #middle rows
                stat_data.slice(-1, 1) #last row
            ], how='diagonal_relaxed').interpolate()
            

        for i in stat_data.drop("T").columns:
            fig.add_trace(
                go.Scatter(
                    x=stat_data["T"].to_list(),
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


def plot_second_stage_result(
    results: pl.DataFrame,
    water_basin: pl.DataFrame,
    market_price_quantiles: pl.DataFrame,
    basin_volume_expectation: pl.DataFrame,
    with_battery: bool = False,
) -> go.Figure:

        
    nb_subplot = 6 if with_battery else 4

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

    fig = plot_second_stage_market_price(results=results, market_price_quantiles=market_price_quantiles, fig=fig, row=1)
    fig = plot_second_stage_basin_volume(
        results=results, 
        basin_volume_expectation=basin_volume_expectation,
        water_basin=water_basin, 
        fig=fig, row=2)
    
    fig = plot_hydro_power(results=results, fig=fig, row=3)
    fig = plot_ancillary_reserve(results=results, fig=fig, row=4, with_battery=with_battery)
    if with_battery:
        fig = plot_battery_power(results=results, fig=fig, row=5)
        fig = plot_battery_soc(results=results, fig=fig, row=6)


    fig.update_layout(
        margin=dict(t=60, l=65, r=10, b=60),
        width=1200,  # Set the width of the figure
        height=300 * nb_subplot,
        legend_tracegroupgap=215,
        barmode = "overlay"
    )

    return fig
