import plotly.graph_objs as go
import plotly.express as px
import polars as pl
from polars import col as c
import pyomo.environ as pyo
from plotly.subplots import make_subplots
from plotly.graph_objects import Figure

from utility.pyomo_preprocessing import extract_optimization_results, linear_interpolation_using_cols

from utility.general_function import build_non_existing_dirs

COLORS = px.colors.qualitative.Plotly



def plot_basin_volume(
    model_instance: pyo.Model, index: dict[str, pl.DataFrame], 
    subset_mapping: dict, fig: Figure, row: int, time_divider: int, **kwargs
    ) -> Figure:
    basin_volume: pl.DataFrame = extract_optimization_results(
        model_instance=model_instance, var_name="basin_volume", subset_mapping=subset_mapping
        ).join(index["water_basin"][["B", "name", "volume_max", "volume_min"]], on="B", how="left")\
        .with_columns(
            (c("basin_volume") - c("volume_min"))/(c("volume_max") - c("volume_min"))*100
        ).pivot(on="name", values="basin_volume", index="T")
        

    for i, name in enumerate(basin_volume.drop("T").columns):
        fig.add_trace(
        go.Scatter(
            x=(basin_volume["T"]/time_divider).to_list(), y=basin_volume[name].to_list(),
            mode='lines', line=dict(color=COLORS[i]), showlegend=True, name=name.replace("_", " "),
            legendgroup="basin_volume",
        ), row=row, col=1)
    fig.update_traces(selector=dict(legendgroup="basin_volume"), legendgrouptitle_text="Basin volume")
    return fig


def plot_market_price(model_instance: pyo.Model, 
    subset_mapping: dict, fig: Figure, row: int, time_divider: int, **kwargs
    ) -> Figure:
    col_names = ["market_price", "max_market_price", "min_market_price"]

    market_price = pl.DataFrame({"T": list(model_instance.T)}).cast(pl.UInt32) # type: ignore
    for col_name in col_names:
        
        market_price = market_price.join(extract_optimization_results(
            model_instance=model_instance, var_name=col_name, subset_mapping=subset_mapping
        ), on="T", how="left")
        
    fig.add_trace(
                go.Scatter(
                    x=(market_price["T"]/time_divider).to_list(), 
                    y=market_price["market_price"].to_list(), 
                    legendgroup="market_price", name= "Market price",
                    mode='lines', line=dict(color=COLORS[0]), showlegend=True
                ), row=row, col=1
            ) 
    fig.add_trace(
            go.Scatter(
                x=(market_price["T"]/time_divider).to_list(), 
                y=market_price["max_market_price"].to_list(), 
                legendgroup="market_price", name= "Boundaries",
                mode='lines',line=dict(color="red"), showlegend=True
            ), row=row, col=1
        ) 
    fig.add_trace(
            go.Scatter(
                x=(market_price["T"]/time_divider).to_list(), 
                y=market_price["min_market_price"].to_list(), 
                legendgroup="market_price", name= "Boundaries",
                mode='lines',line=dict(color="red"), showlegend=False
            ), row=row, col=1
        ) 
    fig.update_traces(selector=dict(legendgroup="market_price"), legendgrouptitle_text="DA market price")
    return fig

def plot_powered_volume(
    model_instance: pyo.Model, index: dict[str, pl.DataFrame], 
    subset_mapping: dict, fig: Figure, row: int, time_divider: int, **kwargs
    ) -> Figure:
    c_nb = 0
    for i, var_name in enumerate(["turbined_volume", "pumped_volume"]):
        factor = 1e-6 if i == 0 else -1e-6
        results = extract_optimization_results(
            model_instance=model_instance, var_name=var_name, subset_mapping=subset_mapping
            ).join(index["hydro_power_plant"][["H", "name"]], on="H", how="left")\
            .pivot(on="name", values=var_name, index="T")\
            .with_columns(c("T")//time_divider)\
            .group_by("T").agg(pl.all().exclude("T").sum()).sort("T")
        
        for col_name in results.drop("T").columns:
            fig.add_trace(
                go.Bar(
                    x=results["T"].to_list(), y=(factor*results[col_name]).to_list(), showlegend=True,
                    marker=dict(color=COLORS[c_nb]), width=1, name= col_name.replace("_", " ") + " " + var_name.replace("_", " "),
                    legendgroup="volume_powered"
                ), row=row, col=1
            )
            c_nb += 1
    fig.update_traces(selector=dict(legendgroup="volume_powered"), legendgrouptitle_text="Powered water volume")
    return fig

def plot_result_summarized(
    model_instance: pyo.Model, index: dict[str, pl.DataFrame],subset_mapping: dict, time_divider: int) -> Figure:

        fig: Figure = make_subplots(
                rows=3, cols = 1, shared_xaxes=True, vertical_spacing=0.02, x_title="<b>Weeks<b>", 
                row_titles= ["DA price [EUR/MWh]", "Basin water volume [%]", "Powered volume [Mm3]"])

        kwargs: dict = {
                "model_instance": model_instance, 
                "index": index, 
                "subset_mapping": subset_mapping, 
                "fig": fig,
                "time_divider": time_divider}

        kwargs["fig"] = plot_market_price(row=1, **kwargs)
        kwargs["fig"] = plot_basin_volume(row=2, **kwargs)
        fig = plot_powered_volume(row=3, **kwargs)
        fig.update_layout(
                barmode='relative',
                margin=dict(t=60, l=65, r= 10, b=60), 
                width=1000,   # Set the width of the figure
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
