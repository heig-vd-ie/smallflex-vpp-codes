from datetime import timedelta, datetime

from typing import Optional
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
from pipelines.data_configs import DataConfig
import numpy as np


COLORS = px.colors.qualitative.Plotly


def remove_spillage_effects(
    results: pl.DataFrame, water_basin: pl.DataFrame
) -> pl.DataFrame:

    basin_idx = water_basin["B"].to_list()

    volume_range = pl_to_dict(water_basin["B", "volume_range"])
    start_volume_mapping = pl_to_dict(water_basin["B", "start_volume"])

    basin_volume_raw = results.select(
        "timestamp", cs.contains("basin_volume_"), cs.contains("spilled_volume_")
    )

    basin_volume_raw = basin_volume_raw.sort(["timestamp"]).select(
        "timestamp",
        *[
            (
                (
                    (c(f"spilled_volume_{col}") / volume_range[col]).shift(1)
                    + c(f"basin_volume_{col}").diff()
                ).fill_null(start_volume_mapping[col])
            ).alias(f"basin_volume_{col}")
            for col in basin_idx
        ],
    )

    cleaned_basin_volume = pl.DataFrame(
        clipped_cumsum(basin_volume_raw.drop("timestamp").to_numpy(), xmin=0, xmax=1),
        schema=basin_volume_raw.drop("timestamp").columns,
    ).with_columns(basin_volume_raw["timestamp"])

    return cleaned_basin_volume


def plot_second_stage_market_price(
    results: pl.DataFrame,
    with_ancillary: bool,
    fig: go.Figure,
    row: int,
    showlegend: bool,
    col: int = 1,
) -> go.Figure:
    if not results.select(cs.contains("quantile")).is_empty():
        fig.add_trace(
            go.Scatter(
                x=results["timestamp"].to_list(),
                y=results["market_price_lower_quantile"].to_list(),
                legendgroup="market_price",
                name="Market price quantiles",
                mode="lines",
                line=dict(color="red"),
                showlegend=showlegend,
                opacity=0.7,
            ),
            row=row,
            col=col,
        )

        fig.add_trace(
            go.Scatter(
                x=results["timestamp"].to_list(),
                y=results["market_price_upper_quantile"].to_list(),
                legendgroup="market_price",
                name="quantile",
                mode="lines",
                line=dict(color="red"),
                opacity=0.7,
                showlegend=False,
            ),
            row=row,
            col=col,
        )

    fig.add_trace(
        go.Scatter(
            x=(results["timestamp"]).to_list(),
            y=results["market_price"].to_list(),
            legendgroup="market_price",
            name="Day ahead",
            mode="lines",
            line=dict(color=COLORS[0]),
            showlegend=showlegend,
        ),
        row=row,
        col=col,
    )
    if with_ancillary:
        fig.add_trace(
            go.Scatter(
                x=(results["timestamp"]).to_list(),
                y=results["ancillary_market_price"].to_list(),
                legendgroup="market_price",
                name="FCR weighted average bides",
                mode="lines",
                line=dict(color="darkgreen"),
                showlegend=showlegend,
            ),
            row=row,
            col=col,
        )

    fig.update_traces(
        selector=dict(legendgroup="market_price"),
        legendgrouptitle_text="<b>Market prices [EUR]<b>",
        legendgrouptitle=dict(font=dict(size=20)),
    )
    return fig


def plot_basin_volume(
    results: pl.DataFrame,
    water_basin: pl.DataFrame,
    fig: go.Figure,
    row: int,
    showlegend: bool,
    col: int = 1,
) -> go.Figure:
    name = ["Upstream reservoir", "Downstream reservoir"]

    results = remove_spillage_effects(results=results, water_basin=water_basin)

    for i, col_name in enumerate(
        results.select(cs.starts_with("basin_volume")).columns
    ):
        fig.add_trace(
            go.Scatter(
                x=results["timestamp"].to_list(),
                y=(results[col_name] * 100).to_list(),
                mode="lines",
                line=dict(color=COLORS[i]),
                showlegend=showlegend,
                name=name[i],
                legendgroup="basin_volume",
            ),
            row=row,
            col=col,
        )

    fig.update_yaxes(
        range=[-5, 105],  # Set your desired y-axis limits here
        row=row,
        col=col,
    )

    fig.update_traces(
        selector=dict(legendgroup="basin_volume"),
        legendgrouptitle_text="<b>Reservoir level [%]<b>",
        legendgrouptitle=dict(font=dict(size=20)),
    )
    return fig


def plot_second_stage_basin_volume(
    results: pl.DataFrame,
    basin_volume_expectation: pl.DataFrame,
    water_basin: pl.DataFrame,
    fig: go.Figure,
    row: int,
    showlegend: bool,
    col: int = 1,
    
) -> go.Figure:

    basin_idx = water_basin["B"].to_list()

    volume_range = pl_to_dict(water_basin["B", "volume_range"])
    start_volume_mapping = pl_to_dict(water_basin["B", "start_volume"])

    basin_volume_raw = results.select(
        "timestamp", cs.contains("basin_volume_"), cs.contains("spilled_volume_")
    )

    basin_volume_raw = basin_volume_raw.sort(["timestamp"]).select(
        "timestamp",
        *[
            (
                (
                    (c(f"spilled_volume_{col_name}") / volume_range[col_name]).shift(1)
                    + c(f"basin_volume_{col_name}").diff()
                ).fill_null(start_volume_mapping[col_name])
            ).alias(f"basin_volume_{col_name}")
            for col_name in basin_idx
        ],
    )

    cleaned_basin_volume = pl.DataFrame(
        clipped_cumsum(basin_volume_raw.drop("timestamp").to_numpy(), xmin=0, xmax=1),
        schema=basin_volume_raw.drop("timestamp").columns,
    ).with_columns(basin_volume_raw["timestamp"])

    ### Add quantile limit ###################################################################################
    basin_volume_expectation = (
        basin_volume_expectation.filter(c("B") == 0)
        .join(
            results.filter(c("sim_idx").is_first_distinct())["timestamp", "sim_idx"],
            on="sim_idx",
            how="inner",
        )
        .sort("timestamp").slice(0, 365)
    )
    
    inner_quantiles = basin_volume_expectation.select(
        cs.contains("quantile_").and_(~cs.contains(f"quantile_0"))
    ).columns
    for col_name in inner_quantiles:
        fig.add_trace(
            go.Scatter(
                x=basin_volume_expectation["timestamp"].to_list(),
                y=(basin_volume_expectation[col_name] * 100).to_list(),
                mode="lines",
                name=col,
                line=dict(width=0.5, color="red", dash="dash"),
                # opacity=0.5,
                showlegend=False,
                legendgroup="basin_volume",
            ),
            row=row,
            col=col,
        )
    
    for i, col_name in enumerate(
        basin_volume_expectation.select(cs.contains(f"quantile_0")).columns
    ):
        fig.add_trace(
            go.Scatter(
                x=basin_volume_expectation["timestamp"].to_list(),
                y=(basin_volume_expectation[col_name] * 100).to_list(),
                mode="lines",
                line=dict(width=0.5, color="red", dash="dash"),
                showlegend=(i == 1) & showlegend,
                name="Scheduled limits",
                fill="tonexty" if i == 1 else None,
                legendgroup="basin_volume",
                opacity=0.5,
            ),
            row=row,
            col=col,
        )

    fig.add_trace(
        go.Scatter(
            x=basin_volume_expectation["timestamp"].to_list(),
            y=(basin_volume_expectation["mean"] * 100).to_list(),
            mode="lines",
            name="Scheduled reservoir level",
            legendgroup="basin_volume",
            line=dict(color="red"),
            showlegend=showlegend,
            opacity=0.5,
        ),
        row=row,
        col=col,
    )
    fig.add_trace(
        go.Scatter(
            x=cleaned_basin_volume["timestamp"].to_list(),
            y=(cleaned_basin_volume["basin_volume_0"] * 100).to_list(),
            mode="lines",
            line=dict(color=COLORS[0]),
            showlegend=showlegend,
            name="Real reservoir level",
            legendgroup="basin_volume",
        ),
        row=row,
        col=col,
    )

    fig.update_yaxes(
        range=[-5, 105],  # Set your desired y-axis limits here
        row=row,
        col=col,
    )

    fig.update_traces(
        selector=dict(legendgroup="basin_volume"),
        legendgrouptitle_text="<b>Reservoir level [%]<b>",
        legendgrouptitle=dict(font=dict(size=20)),
    )

    return fig


def plot_hydro_power(
    results: pl.DataFrame,
    fig: go.Figure,
    row: int,
    timestep: timedelta,
    showlegend: bool,
    col: int = 1,
) -> go.Figure:
    hydro_name = results.select(
        cs.starts_with("hydro_power").and_(~cs.contains("forecast"))
    ).columns
    name = ["Turbine", "Pump"]

    for i, col_name in enumerate(hydro_name):

        fig.add_trace(
            go.Bar(
                x=results["timestamp"].to_list(),
                y=results[col_name].to_list(),
                marker=dict(color=COLORS[i], line=dict(color=COLORS[i])),
                width=timestep.total_seconds() * 1000,
                showlegend=showlegend,
                name=name[i],
                legendgroup="hydro_power",
                offsetgroup="hydro_power_group",
            ),
            row=row,
            col=col,
        )
    fig.update_traces(
        selector=dict(legendgroup="hydro_power"),
        legendgrouptitle_text="<b>Hydro power [MW]<b>",
        legendgrouptitle=dict(font=dict(size=20)),
    )
    return fig


def plot_ancillary_reserve(
    results: pl.DataFrame,
    fig: go.Figure,
    row: int,
    timestep: timedelta,
    showlegend: bool,
    with_battery: bool = True,
    col: int = 1,
) -> go.Figure:

    if results["hydro_ancillary_reserve"].sum() > 0:
        fig.add_trace(
            go.Bar(
                x=results["timestamp"].to_list(),
                y=results["hydro_ancillary_reserve"].to_list(),
                marker=dict(color=COLORS[0], line=dict(color=COLORS[0])),
                showlegend=showlegend,
                width=timestep.total_seconds() * 1000,
                name="hydro reserve",
                legendgroup="ancillary_reserve",
            ),
            row=row,
            col=col,
        )

    if with_battery:
        fig.add_trace(
            go.Bar(
                x=results["timestamp"].to_list(),
                y=results["battery_ancillary_reserve"].to_list(),
                marker=dict(color=COLORS[1], line=dict(color=COLORS[1])),
                width=timestep.total_seconds() * 1000,
                showlegend=showlegend,
                name="Battery reserve",
                legendgroup="ancillary_reserve",
            ),
            row=row,
            col=col,
        )
    fig.update_traces(
        selector=dict(legendgroup="ancillary_reserve"),
        legendgrouptitle_text="<b>Ancillary reserve [MW]<b>",
        legendgrouptitle=dict(font=dict(size=20)),
    )
    return fig


def plot_battery_soc(
    results: pl.DataFrame, fig: go.Figure, row: int, showlegend: bool, col: int = 1
) -> go.Figure:
    fig.add_trace(
        go.Scatter(
            x=results["timestamp"].to_list(),
            y=(100*results["battery_soc"]).to_list(),
            mode="lines",
            line=dict(color=COLORS[0]),
            showlegend=showlegend,
            name="Battery SOC",
            legendgroup="battery_soc",
        ),
        row=row,
        col=col,
    )
    fig.update_traces(
        selector=dict(legendgroup="battery_soc"),
        legendgrouptitle_text="<b>Battery SOC [%]<b>",
        legendgrouptitle=dict(font=dict(size=20)),
    )
    return fig


def plot_battery_power(
    results: pl.DataFrame, fig: go.Figure, row: int, timestep: timedelta, showlegend: bool, col: int = 1
) -> go.Figure:

    fig.add_trace(
        go.Bar(
            x=results["timestamp"].to_list(),
            y=results["battery_discharging_power"].to_list(),
            marker=dict(color=COLORS[0], line=dict(color=COLORS[0])),
            showlegend=showlegend,
            width=timestep.total_seconds() * 1000,
            name="Battery discharging power",
            legendgroup="battery_power",
        ),
        row=row,
        col=col,
    )

    fig.add_trace(
        go.Bar(
            x=results["timestamp"].to_list(),
            y=results["battery_charging_power"].to_list(),
            marker=dict(color=COLORS[1], line=dict(color=COLORS[1])),
            showlegend=showlegend,
            width=timedelta(hours=1).total_seconds() * 1000,
            name="Battery charging power",
            legendgroup="battery_power",
        ),
        row=row,
        col=col,
    )

    fig.update_traces(
        selector=dict(legendgroup="battery_power"),
        legendgrouptitle_text="<b>Battery power [MW]<b>",
        legendgrouptitle=dict(font=dict(size=20)),
    )
    return fig


def plot_scenario_results(
    optimization_results: pl.DataFrame,
    water_basin: pl.DataFrame,
    fig: Optional[go.Figure] = None,
    col: int = 1,
    tick_size: int = 20
) -> go.Figure:
    row_titles=[
        "<b>Price [Euro]<b>",
        "<b>Discharge volume [m³/day]<b>",
        "<b>Basin level [%]<b>",
        "<b>Hydro power [MW]<b>",
    ]
    if fig is None:
        fig = make_subplots(
                rows=4,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.02,
                row_titles=row_titles
            )
        fig.update_layout(
            margin=dict(t=10, l=25, r=8, b=60),
            width=900,  # Set the width of the figure
            height=300 * 4,
            legend_tracegroupgap=720,
            barmode="overlay",
        )

    _, cols = fig._get_subplot_rows_columns()
    showlegend = col == len(cols)
    
    volume_range = pl_to_dict(water_basin["B", "volume_range"])
    start_volume_mapping = pl_to_dict(water_basin["B", "start_volume"])
    
    name_mapping = {
        "market_price": "<b>Market Price [Euro]</b>",
        "discharge_volume_0": "<b>Discharge Volume [Mm³/day]</b>",
        "basin_volume_0": "<b>Reservoir level [%]</b>"
    }

    optimization_results = optimization_results.with_columns(
        (c("discharge_volume_0") / 1e6).alias("discharge_volume_0")
    )
    
    optimization_results = optimization_results.sort(["Ω", "T"]).with_columns(
        (
            (
                (c(f"spilled_volume_{col_name}") / volume_range[col_name]).shift(1)
                + c(f"basin_volume_{col_name}").diff().over("Ω")
            ).fill_null(start_volume_mapping[col_name])
        ).alias(f"basin_volume_{col_name}")
        for col_name in start_volume_mapping.keys()
    )
    
    new_optimization_results = optimization_results.drop(
        [f"basin_volume_{col_name}" for col_name in start_volume_mapping.keys()]
    )
    for col_name in start_volume_mapping.keys():
        basin_volume_df = optimization_results.pivot(
            on="Ω", values=f"basin_volume_{col_name}", index="T"
        )

        new_basin_volume_df = (
            pl.DataFrame(
                clipped_cumsum(basin_volume_df.drop("T").to_numpy(), xmin=0, xmax=1),
                schema=basin_volume_df.drop("T").columns,
            )
            .with_columns(basin_volume_df["T"])
            .unpivot(variable_name="Ω", value_name=f"basin_volume_{col_name}", index="T")
            .with_columns(c("Ω").cast(pl.UInt32))
        )
        new_optimization_results = new_optimization_results.join(
            new_basin_volume_df, on=["T", "Ω"], how="left"
        )
    optimization_results = new_optimization_results
    
    

    for idx, col_name in enumerate(name_mapping.keys()):

        data = optimization_results.pivot(index="T", on="Ω", values=col_name)
        
        for i, senario in enumerate(data.drop("T").columns):
            fig.add_trace(
                go.Scatter(
                    x=data["T"].to_list(),
                    y=data[senario].to_list(),
                    mode="lines",
                    opacity=0.3,
                    name=f"Scenarios",
                    line=dict(color="grey"),
                    showlegend=showlegend & (i == 0) & (idx==0),
                    legendgroup="stochastic_scenario",
                ),
                row=idx + 1,
                col=col,
            )
        
        stat_data = (
            optimization_results.group_by("T")
            .agg(
                c(col_name).median().alias("Median"),
                c(col_name).quantile(0.15).alias("15th-quantile"),
                c(col_name).quantile(0.85).alias("85th-quantile"),
            )
            .sort("T")
        )

        mean_stat_data = stat_data.with_columns(
            c("Median", "15th-quantile", "85th-quantile")
                .rolling_mean(window_size=7)
                .shift(-4)
        )

        stat_data = pl.concat(
            [
                stat_data.slice(0, 1),  # first row
                mean_stat_data.slice(1, mean_stat_data.height - 2),  # middle rows
                stat_data.slice(-1, 1),  # last row
            ],
            how="diagonal_relaxed",
        ).interpolate()

        for stat_name in stat_data.drop("T").columns:
            fig.add_trace(  
                go.Scatter(
                    x=stat_data["T"].to_list(),
                    y=stat_data[stat_name].to_list(),
                    mode="lines",
                    name=stat_name,
                    line=dict(color="red" if stat_name == "Median" else "orange"),
                    showlegend=showlegend & (idx==0),
                    legendgroup="stochastic_scenario",
                ),
                row=idx + 1,
                col=col,
            )
        fig.update_traces(
            selector=dict(legendgroup="stochastic_scenario"),
            legendgrouptitle_text="<b>Stochastic variables</b>",
            legendgrouptitle=dict(font=dict(size=20)),
        )    
    

    hydro_name = optimization_results.select(cs.starts_with("hydro_power")).columns

    
    for i, hydro in enumerate(hydro_name):

        fig.add_trace(
            go.Bar(
                x=optimization_results.filter(c("Ω") == optimization_results["Ω"][0])[
                    "T"
                ].to_list(),
                y=optimization_results.filter(c("Ω") == optimization_results["Ω"][0])[
                    hydro
                ].to_list(),
                name="Turbine" if i == 0 else "Pump",
                marker=dict(color=COLORS[i]),
                showlegend=showlegend,
                legendgroup="hydro_power",
                width=1,
            ),
            row=4,
            col=col,
        )

    fig.update_traces(
            selector=dict(legendgroup="hydro_power"),
            legendgrouptitle_text="<b>Hydro power [MW]<b>",
            legendgrouptitle=dict(font=dict(size=20)),
        ) 
    
    ticks_df = optimization_results.filter(
        (c("timestamp").dt.month().is_in([2, 4, 6, 8, 10, 12]))
    ).filter(c("timestamp").dt.month().is_first_distinct())\
    .with_columns(
        pl.col("timestamp").dt.strftime("%b").alias("formatted_timestamp")
    )

    
    for ann in fig.layout.annotations: # type: ignore
        if ann.text in row_titles:
            ann.update(font=dict(size=20))  # set your desired size here
    
    for i in range(1, len(row_titles) + 1):
        fig.update_xaxes(
            tickfont=dict(size=tick_size),
            ticklabelposition="outside",
            ticklabelstandoff=10,
            tickmode = 'array',
            tickvals = ticks_df["T"].to_list(),
            ticktext = ticks_df["formatted_timestamp"].to_list(),
            row=i,
            col=col,
        )
        fig.update_yaxes(tickfont=dict(size=tick_size), row=i, col=col)    
            
    return fig


def plot_second_stage_result(
    results: pl.DataFrame,
    water_basin: pl.DataFrame,
    basin_volume_expectation: pl.DataFrame,
    display_battery: bool = False,
    tick_size: int = 20,
    fig: Optional[go.Figure] = None,
    col: int = 1,
) -> go.Figure:

    with_battery = results.select(cs.contains("battery")).shape[1] > 0
    with_ancillary = results.select(cs.contains("ancillary_reserve")).shape[1] > 0
    timestep = results["timestamp"].diff()[1]

    row_titles = [
        "<b>Price [EUR]<b>",
        "<b>Reservoir level [%]<b>",
        "<b>Hydro power [MW]<b>",
    ]

    if with_ancillary:
        row_titles.append("<b>Ancillary reserve [MW]<b>")

    if with_battery and display_battery:
        row_titles.append("<b>Battery power [MW]<b>")
        row_titles.append("<b>Battery SOC [%]<b>")

    if fig is None:
        fig = make_subplots(
            rows=len(row_titles),
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            row_titles=row_titles,
        )
        fig.update_layout(
            margin=dict(t=10, l=25, r=8, b=60),
            width=900,  # Set the width of the figure
            height=300 * len(row_titles),
            legend_tracegroupgap=215,
            barmode="overlay",
        )

    _, cols = fig._get_subplot_rows_columns()
    showlegend = col == len(cols)
    row_idx = 1
    fig = plot_second_stage_market_price(
        results=results,
        with_ancillary=with_ancillary,
        fig=fig,
        row=row_idx,
        showlegend=showlegend,
        col=col
    )
    row_idx += 1
    fig = plot_second_stage_basin_volume(
        results=results,
        basin_volume_expectation=basin_volume_expectation,
        water_basin=water_basin,
        fig=fig,
        row=row_idx,
        showlegend=showlegend,
        col=col
    )
    row_idx += 1
    fig = plot_hydro_power(results=results, fig=fig, row=row_idx, timestep=timestep, showlegend=showlegend, col=col)
    row_idx += 1
    if with_ancillary:
        fig = plot_ancillary_reserve(
            results=results,
            fig=fig,
            row=row_idx,
            timestep=timestep,
            with_battery=with_battery,
            showlegend=showlegend,
            col=col
        )
        row_idx += 1
    if with_battery and display_battery:
        fig = plot_battery_power(
            results=results, fig=fig, row=row_idx, timestep=timestep, showlegend=showlegend,col=col
        )
        row_idx += 1
        fig = plot_battery_soc(results=results, fig=fig, row=row_idx, showlegend=showlegend,col=col)
        row_idx += 1

    ticks_df = results.filter(
        (c("timestamp").dt.month().is_in([2, 5, 8, 11]) & (c("timestamp").dt.day() == 15))
    ).filter(c("timestamp").dt.month().is_first_distinct())\
    .with_columns(
        pl.col("timestamp").dt.strftime("%b %Y").alias("formatted_timestamp")
    )

    for i in range(1, len(row_titles) + 1):
        fig.update_xaxes(
            tickfont=dict(size=tick_size),
            ticklabelposition="outside",
            ticklabelstandoff=10,
            tickmode = 'array',
            tickvals = ticks_df["timestamp"].to_list(),
            ticktext = ticks_df["formatted_timestamp"].to_list(),
            row=i,
            col=col,
        )
        fig.update_yaxes(tickfont=dict(size=tick_size), row=i, col=col)

    return fig


def plot_first_stage_result(
    results: pl.DataFrame,
    water_basin: pl.DataFrame,
    tick_size: int = 20,
    fig: Optional[go.Figure] = None,
    col: int = 1,
) -> go.Figure:

    timestep = results["timestamp"].diff()[1]
    row_titles = [
        "<b>Price [Euro]<b>",
        "<b>Basin level [%]<b>",
        "<b>Hydro power [MW]<b>",
    ]

    if fig is None:
        fig = make_subplots(
            rows=len(row_titles),
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            row_titles=row_titles,
        )
        fig.update_layout(
            margin=dict(t=10, l=25, r=8, b=60),
            width=900,  # Set the width of the figure
            height=300 * len(row_titles),
            legend_tracegroupgap=215,
            barmode="overlay",
        )

    _, cols = fig._get_subplot_rows_columns()
    showlegend = col == len(cols)
    
    fig = plot_second_stage_market_price(
        results=results,
        with_ancillary=False,
        fig=fig,
        row=1,
        col=col,
        showlegend=showlegend,
    )
    fig = plot_basin_volume(
        results=results,
        fig=fig,
        water_basin=water_basin,
        row=2,
        col=col,
        showlegend=showlegend,
    )
    fig = plot_hydro_power(
        results=results,
        fig=fig,
        row=3,
        timestep=timestep,
        col=col,
        showlegend=showlegend,
    )
    
    ticks_df = results.filter(
        (c("timestamp").dt.month().is_in([2, 5, 8, 11]) & (c("timestamp").dt.day() == 15))
    ).filter(c("timestamp").dt.month().is_first_distinct())\
    .with_columns(
        pl.col("timestamp").dt.strftime("%b %Y").alias("formatted_timestamp")
    )

    for i in range(1, len(row_titles) + 1):

        fig.update_xaxes(
            tickfont=dict(size=tick_size),
            ticklabelposition="outside",
            ticklabelstandoff=10,
            tickmode = 'array',
            tickvals = ticks_df["timestamp"].to_list(),
            ticktext = ticks_df["formatted_timestamp"].to_list(),
            col=col,
        )
        fig.update_yaxes(
            tickfont=dict(size=tick_size),
            row=i, col=col)

    return fig


def plot_reservoir_level_measurement(basin_height_measurement: pl.DataFrame, basin_volume_table: pl.DataFrame):
    

    reservoir_level = basin_height_measurement.join(basin_volume_table, on="height", how="left").with_columns(
        c("timestamp").dt.year().alias("year"),
        (c("timestamp") - pl.datetime(c("timestamp").dt.year(), 1, 1,time_zone="UTC")).dt.total_days().alias("dt"),
    ).filter(c("timestamp").is_first_distinct()).pivot(
        on="year",
        values="volume",
        index="dt",
    ).sort("dt").interpolate()

    fig = go.Figure()

    for year in reservoir_level.drop(["dt", "2019"]).columns:
        fig.add_trace(
            go.Scatter(
                x=reservoir_level["dt"].to_list(),
                y=(100*reservoir_level[year]).to_list(),
                mode="lines",
                name=str(year),
            )
        )
    fig.update_layout(
        margin=dict(t=10, l=25, r=8, b=60),
        legend_title_text='Years',
        legend=dict(font=dict(size=20)),
        yaxis=dict(
            title=dict(
                text='Reservoir level [%]', # The axis title text
                font=dict(
                    size=20,
                )
            )
        )
    )


    ticks_df =  pl.DataFrame(
        {
            "timestamp": pl.date_range(start=datetime(2019, 1, 1), end=datetime(2020, 12, 31), interval=timedelta(days=1), eager=True)
        }
    ).with_row_index(name="row_index").filter(
            (c("timestamp").dt.month().is_in([2, 4, 6, 8, 10, 12]))
        ).filter(c("timestamp").dt.month().is_first_distinct())\
        .with_columns(
            pl.col("timestamp").dt.strftime("%b").alias("formatted_timestamp")
        )

    fig.update_xaxes(
        tickfont=dict(size=20),
        ticklabelposition="outside",
        ticklabelstandoff=10,
        tickmode = 'array',
        tickvals = ticks_df["row_index"].to_list(),
        ticktext = ticks_df["formatted_timestamp"].to_list(),
        
    )
    fig.update_yaxes(
        tickfont=dict(size=20))
    return fig

def plot_battery_arbitrage(results: pl.DataFrame, tick_size: int = 20) -> go.Figure:

    with_ancillary = results.select(cs.contains("ancillary_reserve")).shape[1] > 0
    timestep = results["timestamp"].diff()[1]
    row_titles = [
        "<b>Market price [EUR]<b>",
        "<b>Hydro power [MW]<b>",
        "<b>Battery power [MW]<b>",
        "<b>Battery SOC [%]<b>"
    ]
    
    if with_ancillary:
        row_titles.append("<b>Ancillary reserve [MW]<b>")
    
    fig = make_subplots(
                rows=len(row_titles),
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.02,
                row_titles=row_titles,
            )
    fig.update_layout(
        margin=dict(t=10, l=25, r=8, b=60),
        width=1200,  # Set the width of the figure
        height=300 * len(row_titles),
        legend_tracegroupgap=208,
        legend=dict(font=dict(size=20)),
        barmode="overlay",
    )

    fig = plot_second_stage_market_price(
            results=results,
            with_ancillary=True,
            fig=fig,
            row=1,
            col=1,
            showlegend=True,
        )

    fig = plot_hydro_power(results=results, fig=fig, row=2, timestep=timestep, showlegend=True, col=1)


    fig = plot_battery_power(
        results=results, fig=fig, row=3, timestep=timestep, showlegend=True,col=1
    )
    fig = plot_battery_soc(results=results, fig=fig, row=4, showlegend=True,col=1)
    
    for ann in fig.layout.annotations: # type: ignore
        if ann.text in row_titles:
            ann.update(font=dict(size=20))  # set your desired size here

    if with_ancillary:
        fig = plot_ancillary_reserve(
                results=results,
                fig=fig,
                row=5,
                timestep=timestep,
                with_battery=True,
                showlegend=True,
                col=1
            )

    for i in range(1, len(row_titles) + 1):
        fig.update_xaxes(
            tickfont=dict(size=tick_size),
            ticklabelposition="outside",
            ticklabelstandoff=10,
            tickmode = 'array',
            row=i,
            col=1,
        )
        fig.update_yaxes(tickfont=dict(size=tick_size), row=i, col=1)

    return fig
