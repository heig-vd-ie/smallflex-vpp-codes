import plotly.graph_objs as go
import plotly.express as px
import polars as pl
from polars import col as c
import pyomo.environ as pyo
from plotly.subplots import make_subplots

from utility.pyomo_preprocessing import extract_optimization_results, linear_interpolation_using_cols

from utility.general_function import build_non_existing_dirs

COLORS = px.colors.qualitative.Plotly

def plot_first_stage_summarized(
    model_instance: pyo.Model, input_table: dict[str, pl.DataFrame], with_pumping: bool, folder_name: str,
    time_divider: int
    ):
    build_non_existing_dirs(folder_name)
    file_name = f"{folder_name}/first_stage_with_pumping.png" if with_pumping else f"{folder_name}/first_stage_without_pumping.png"
    col_names = ["turbined_volume", "pumped_volume", "market_price", "max_market_price", "min_market_price", "discharge_volume"]

    result: pl.DataFrame = extract_optimization_results(model_instance, col_names[0])\
        .with_columns(
            extract_optimization_results(model_instance, col)[col] for col in col_names[1:]
        )

    fig = make_subplots(
            rows=3, cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
            row_titles= ["DA price [EUR/MWh]", "Greis lac height [masl]", "Turbined volume [Mm3]"] )
    fig.add_trace(
                go.Scatter(
                    x=(result["T"]/time_divider).to_list(), y=result["market_price"].to_list(), mode='lines',line=dict(color=COLORS[0]),showlegend=False
                ), row=1, col=1
            ) 
    fig.add_trace(
            go.Scatter(
                x=(result["T"]/time_divider).to_list(), y=result["max_market_price"].to_list(), mode='lines',line=dict(color="red"),showlegend=False
            ), row=1, col=1
        ) 
    fig.add_trace(
            go.Scatter(
                x=(result["T"]/time_divider).to_list(), y=result["min_market_price"].to_list(), mode='lines',line=dict(color="red"),showlegend=False
            ), row=1, col=1
        ) 

    basin_volume = extract_optimization_results(model_instance, "basin_volume")

    basin_volume = basin_volume.join(
        input_table["turbined_table_per_volume"][["height", "volume"]], 
        left_on="basin_volume", right_on="volume", how="full", coalesce=True
        ).sort("basin_volume")
    basin_volume = linear_interpolation_using_cols(basin_volume, "basin_volume", "height").drop_nulls("T").sort("T")

    fig.add_trace(
                go.Scatter(
                    x=(basin_volume["T"]/time_divider).to_list(), y=basin_volume["height"].to_list(),
                    mode='lines', line=dict(color=COLORS[0]), showlegend=False
                ), row=2, col=1
            )
    volume_result: pl.DataFrame = result.with_columns(c("T")//time_divider)\
        .group_by("T").agg(c("turbined_volume", "pumped_volume").sum()).sort("T")

    fig.add_trace(
                go.Bar(
                    x=volume_result["T"].to_list(), y=(volume_result["turbined_volume"]/1e6).to_list(), showlegend=False,
                    marker=dict(color=COLORS[0]), width=1
                ), row=3, col=1
            )

    fig.add_trace(
                go.Bar(
                    x=volume_result["T"].to_list(), y=(-volume_result["pumped_volume"]/1e6).to_list(), showlegend=False,
                    marker=dict(color="red"), width=1
                ), row=3, col=1
            )
    fig.update_layout(
        barmode='relative',
        margin=dict(t=60, l=65, r= 10, b=60), 
        width=1000,   # Set the width of the figure
        height=800
    )

    fig.show()
    fig.write_image(file_name)
