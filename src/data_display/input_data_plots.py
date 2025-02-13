from itertools import product
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import polars as pl
import plotly.express as px
import plotly.graph_objs as go
from polars import col as c
import numpy as np
import tqdm

from numpy_function import relative_error_within_boundaries, error_within_boundaries
from general_function import build_non_existing_dirs


from data_federation.input_model import SmallflexInputSchema
from utility.pyomo_preprocessing import optimal_segments

COLORS = px.colors.qualitative.Plotly

def plot_market_prices(data: pl.DataFrame) -> go.Figure:
    # non_existing dirs
    year_list: list[str] = sorted(data["year"].unique().to_list())
    direction_list: list[str] = sorted(data["direction"].unique().to_list(), reverse=True)
    
    unit = data["unit"][0]
    
    fig = make_subplots(
        rows=len(direction_list), cols = 1, shared_xaxes=True, vertical_spacing=0.05, x_title="<b>Time<b>", 
        y_title="<b>" + unit + "<b>", row_titles=direction_list)
    
    for i, values in enumerate(product(year_list, direction_list)):
        year = values[0]
        direction = values[1]
        fig_index = i % len(direction_list) + 1
        color_index= i//len(direction_list)
        plot_data: pl.DataFrame = data.filter(c("year") == year).filter(c("direction") == direction).sort("index")
        
        hovertemplate: str = 'Price: %{y:.1f} '+ unit + '<br>Time: %{customdata}</b><extra></extra>'

        fig.add_trace(
            go.Scatter(
                x=plot_data["index"].to_list(), y=plot_data["avg"].to_list(), 
                customdata=plot_data["displayed_date"].to_list(),
                mode='lines', name=str(year), legendgroup=str(year), line=dict(color=COLORS[color_index]),
                hovertemplate=hovertemplate, showlegend=fig_index==1
            ), row=fig_index, col=1
        )
        

    return update_figure_layout(fig=fig, data=data)


def plot_discharge_flow(data: pl.DataFrame) -> go.Figure:
    year_list: list[str] = sorted(data["year"].unique().to_list())
    fig = go.Figure()
    for i, year in enumerate(year_list):

        plot_data: pl.DataFrame = data.filter(c("year") == year).sort("index")
        hovertemplate: str = 'Flow: %{y:.1f} m3/s<br>Time: %{customdata}</b><extra></extra>'

        fig.add_trace(
            go.Scatter(
                x=plot_data["index"].to_list(), y=plot_data["value"].to_list(), 
                customdata=plot_data["displayed_date"].to_list(),
                mode='lines', name=str(year), legendgroup=str(year), line=dict(color=COLORS[i%len(COLORS)]),
                hovertemplate=hovertemplate, showlegend=True
            )
        )
        
    fig.update_xaxes(title_text="<b>Time<b>")
    fig.update_yaxes(title_text="<b>m3/s<b>")
    
    return update_figure_layout(fig=fig, data=data)

def plot_discrete_performance_table(data: pl.DataFrame) -> go.Figure:
    
    data = data.sort("state_number").with_columns(c("state_number").cast(pl.Utf8))  
    flow_data: pl.DataFrame = data.pivot(index="head", on="state_number", values="flow").sort("head")
    electrical_power_data: pl.DataFrame = data.pivot(index="head", on="state_number", values="electrical_power").sort("head")
    fig = make_subplots(
            rows=2, cols = 1, shared_xaxes=True, vertical_spacing=0.05, 
            x_title="<b>head [m]<b>",  row_titles=["Flow [m3/s]", "Power [MW]"])

    for i, state in enumerate(data["state_number"].unique().to_list()):
        hovertemplate: str = 'Flow: %{y:.1f} [m3/s]<br>Head: %{x:.1f} m</b><extra></extra>'
        fig.add_trace(
                go.Scatter(
                    x=flow_data["head"].to_list(), y=flow_data[state].to_list(), 
                    mode='lines', name=str(state), legendgroup=str(state), line=dict(color=COLORS[i]),
                    hovertemplate=hovertemplate, showlegend=True
                ), row=1, col=1
            )
        
        hovertemplate: str = 'Electrical Power: %{y:.1f} [MW]<br>Head: %{x:.1f} m</b><extra></extra>'
        fig.add_trace(
                go.Scatter(
                    x=electrical_power_data["head"].to_list(), y=electrical_power_data[state].to_list(), 
                    mode='lines', name=str(state), legendgroup=str(state), line=dict(color=COLORS[i]),
                    hovertemplate=hovertemplate, showlegend=False
                ), row=2, col=1
            )
    return fig  

def plot_continuous_performance_table(data: pl.DataFrame) -> go.Figure:
    hovertemplate: str = 'Flow: %{x:.2f} m3/s<br>Power: %{y:.1f} MW </b><extra></extra>'
    fig = go.Figure()
    for i, head in enumerate(data["head"].unique().to_list()):
        plot_data: pl.DataFrame = data.filter(c("head") == head).sort("flow")
        hovertemplate: str = 'Flow: %{x:.2f} m3/s<br>Power: %{y:.1f} MW </b><extra></extra>'
        fig.add_trace(
            go.Scatter(
                x=plot_data["flow"].to_list(), y=plot_data["electrical_power"].to_list(), 
                mode='lines', name=str(head), legendgroup=str(head), line=dict(color=COLORS[i%len(COLORS)]),
                hovertemplate=hovertemplate, showlegend=True
            )
        )
        fig.update_xaxes(title_text="<b>Flow [m3/s]<b>")
        fig.update_yaxes(title_text="<b>Power [MW]<b>")
    return fig  

def plot_power(data: pl.DataFrame) -> go.Figure:
    year_list: list[str] = sorted(data["year"].unique().to_list())
    fig = go.Figure()
    for i, year in enumerate(year_list):

        plot_data: pl.DataFrame = data.filter(c("year") == year).sort("index")
        hovertemplate: str = 'Power: %{y:.1f} MW<br>Time: %{customdata}</b><extra></extra>'

        fig.add_trace(
            go.Scatter(
                x=plot_data["index"].to_list(), y=plot_data["avg_active_power"].to_list(), 
                customdata=plot_data["displayed_date"].to_list(),
                mode='lines', name=str(year), legendgroup=str(year), line=dict(color=COLORS[i%len(COLORS)]),
                hovertemplate=hovertemplate, showlegend=True
            )
        )
        
    fig.update_xaxes(title_text="<b>Time<b>")
    fig.update_yaxes(title_text="<b>MW<b>")
    
    return update_figure_layout(fig=fig, data=data)

def plot_basin_height(data: pl.DataFrame) -> go.Figure:

    fig = go.Figure()

    plot_data: pl.DataFrame = data.sort("timestamp").with_row_index(name="index")
    hovertemplate: str = 'Power: %{y:.1f} MW<br>Date: %{customdata}</b><extra></extra>'

    fig.add_trace(
        go.Scatter(
            x=plot_data["index"].to_list(), y=plot_data["height"].to_list(), 
            customdata=plot_data["date_str"].to_list(),
            mode='lines', line=dict(color=COLORS[0]),
            hovertemplate=hovertemplate, showlegend=True
        )
    )
        
    fig.update_xaxes(title_text="<b>Date<b>")
    fig.update_yaxes(title_text="<b>masl<b>")
    fig.update_layout(autosize=True, margin=dict(t=20, l=65))
    
    ticks_nb: int= data.unique("year").height  
    ticks_df = plot_data.gather_every(n=plot_data.height // ticks_nb, offset=plot_data.height // (ticks_nb*2))
    
    for ax in fig['layout']:
        if ax[:5] == 'xaxis':
            fig['layout'][ax]['tickmode'] = "array" # type: ignore
            fig['layout'][ax]['ticktext'] = ticks_df["year"] # type: ignore
            fig['layout'][ax]['tickvals'] = ticks_df["index"] # type: ignore

    return fig

    

def plot_basin_height_volume_table(small_flex_input_schema: SmallflexInputSchema) -> dict:
    plot_dict: dict = dict()
    basin_height_volume_table = small_flex_input_schema.basin_height_volume_table

    water_basin_fk = basin_height_volume_table["water_basin_fk"].unique().to_list()


    for data in small_flex_input_schema.water_basin.filter(c("uuid").is_in(water_basin_fk)).to_dicts():
        plot_data = basin_height_volume_table\
            .filter(c("water_basin_fk") >=  data["uuid"])
        if data["height_min"]:
            plot_data =plot_data.filter(c("height") >=  data["height_min"])
        if data["height_max"]:
            plot_data =plot_data.filter(c("height") <=  data["height_max"])
        
        plot_data = plot_data.sort("height")
            
        fig = go.Figure()
        
        x = np.array(plot_data["height"])
        y = np.array(plot_data["volume"])
        segments = optimal_segments(x, y, 5)

        hovertemplate: str = 'Height: %{x:.1f} masl<br>: Volume: %{y:.1f} Mm^3</b><extra></extra>'

        fig.add_trace(
                    go.Scatter(
                        x=x[segments], y=y[segments], 
                        mode='lines+markers', line=dict(color="red"), showlegend=True,
                        hovertemplate=hovertemplate, name="Linearized"
                    )
                )

        fig.add_trace(
                    go.Scatter(
                        x=plot_data["height"].to_list(), y=(plot_data["volume"]).to_list(), 
                        mode='lines', line=dict(color=COLORS[0]), showlegend=True,
                        hovertemplate=hovertemplate, name="Measured"
                    )
                )

        fig.update_xaxes(title_text="<b>Height [masl]<b>")
        fig.update_yaxes(title_text="<b>Volume [m3]<b>")
        plot_dict[data["name"]] = fig
    return plot_dict

def update_figure_layout(fig: go.Figure, data: pl.DataFrame) -> go.Figure:
    fig.update_layout(autosize=True, margin=dict(t=20, l=65))
    
    ticks_df: pl.DataFrame = data.unique("index").sort("index")   
    ticks_df = ticks_df.gather_every(n=ticks_df.height // 12, offset=ticks_df.height // 24).with_columns(
        c("timestamp").dt.strftime('%b').alias("month"),
    )
    
    for ax in fig['layout']:
        if ax[:5] == 'xaxis':
            fig['layout'][ax]['tickmode'] = "array" # type: ignore
            fig['layout'][ax]['ticktext'] = ticks_df["month"] # type: ignore
            fig['layout'][ax]['tickvals'] = ticks_df["index"] # type: ignore

    return fig


def plot_forecast(
    measurement_df: pl.DataFrame, forecast_df: pl.DataFrame, plot_name: str, plot_folder: str, title: str,
    z_score: int = 1, nb_days: int = 5, min_boundaries: float = 0.15):
    
    build_non_existing_dirs(plot_folder)
    
    n_rows = forecast_df.height - nb_days -1

    mean_error_np = np.zeros([forecast_df.height - nb_days, nb_days])
    sum_error_np = np.zeros([forecast_df.height - nb_days, nb_days])
    nb_error_np = np.zeros([forecast_df.height - nb_days, nb_days])

    fig = make_subplots(
            rows=n_rows, cols = 1, row_titles=[title]*n_rows,
        )
    
    fig_2 = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05, subplot_titles=("Mean error [%]", "Sum error", "Number of error"))
    data = forecast_df.slice(0, n_rows - 1).to_dicts()
    for i in tqdm.tqdm(range(n_rows-1), desc=f"Plotting {title} forecast"):
        forecast_np = np.array(data[i]["forecast"])
        actual_date = data[i]["timestamp"]

        mean_forecast = np.mean(forecast_np, axis=0)
        std_forecast = np.std(forecast_np, axis=0)
        boundary =  np.maximum(mean_forecast * min_boundaries, z_score * std_forecast)
        z_score_max = mean_forecast + boundary
        z_score_min = mean_forecast - boundary

        measurement_np = measurement_df\
            .filter(c("timestamp") > actual_date).slice(0, len(mean_forecast))["value"].to_numpy()
            
        relative_error_np = relative_error_within_boundaries(measurement_np, z_score_min, z_score_max)[:24*nb_days].reshape(nb_days, 24)
        error_np = error_within_boundaries(measurement_np, z_score_min, z_score_max)[:24*nb_days].reshape(nb_days, 24)
        
        mean_error_np[i, :] = np.mean(relative_error_np, axis=1)
        nb_error_np[i, :] = np.sum(relative_error_np > 0, axis=1)
        sum_error_np[i, :] = np.sum(error_np, axis=1)
        
        datetime_list = pl.datetime_range(
                actual_date + timedelta(hours=1), actual_date + timedelta(days=5) - timedelta(hours=1), timedelta(hours=1), 
                eager=True
            ).to_list()
        
        for j, scenario in enumerate(data[i]["forecast"]):
            fig = fig.add_trace(
                go.Scatter(
                    x=datetime_list, y=scenario, 
                    mode='lines', name="Forecast", legendgroup=f"day{i}",  showlegend=j==0,
                    line=dict(color="grey"),  opacity=.3,
                ), row=i+ 1, col=1)

        fig = fig.add_trace(
                go.Scatter(
                    x=datetime_list, y=mean_forecast, 
                    mode='lines', name="Forecast average", legendgroup=f"day{i}",
                    line=dict(color="red"))
                , row=i+ 1, col=1)

        fig = fig.add_trace(
                go.Scatter(
                    x=datetime_list, y=z_score_max, 
                    mode='lines', name=f"Forecast boundaries", legendgroup=f"day{i}",  showlegend=True,
                    line=dict(color="red"),  opacity=.5)
                , row=i+ 1, col=1)

        fig = fig.add_trace(
                go.Scatter(
                    x=datetime_list, y=z_score_min, 
                    mode='lines', name=f"Forecast boundaries", legendgroup=f"day{i}",  showlegend=False,
                    line=dict(color="red"),  opacity=.5)
                , row=i + 1, col=1)

        fig = fig.add_trace(
                go.Scatter(
                    x=datetime_list, y=measurement_np, 
                    mode='lines', name="Measurement", legendgroup=f"day{i}",
                    line=dict(color="blue"))
                , row=i+ 1, col=1)
        fig.update_traces(selector=dict(legendgroup=f"day{i}"), legendgrouptitle_text=actual_date.strftime("%Y-%m-%d"))

    for i in range(nb_days):
        fig_2.add_trace(go.Box(y=mean_error_np[:, i]*100, name= f"day {i+1}", showlegend=False, line=dict(color=COLORS[i])), row=1, col=1)
        fig_2.add_trace(go.Box(y=sum_error_np[:, i], name= f"day {i+1}", showlegend=False, line=dict(color=COLORS[i])), row=2, col=1)
        fig_2.add_trace(go.Box(y=nb_error_np[:, i], name= f"day {i+1}", showlegend=False, line=dict(color=COLORS[i])), row=3, col=1)

    fig.update_layout(
            margin=dict(t=60, l=65, r= 10, b=60), 
            width=1000,   # Set the width of the figure
            height=n_rows*300,
            legend_tracegroupgap=204
        )

    fig_2.update_layout(
            margin=dict(t=60, l=65, r= 10, b=60), 
            width=1000,   # Set the width of the figure
            height=3*200,
            title= dict(text =f"{title} forecast error statistics")
        )

    fig.write_html(f"{plot_folder}/{plot_name}.html")
    fig_2.write_html(f"{plot_folder}/{plot_name}_statistics.html")
    
def plot_historical_scenarios(
    data: pl.DataFrame, x_axis: str, plot_name: str, plot_folder: str, title: str, z_score: float = 1, values: str="value"):
    
    build_non_existing_dirs(plot_folder)
    
    loc_list = data["location"].unique().to_list()
    
    fig = make_subplots(
                rows=len(loc_list), cols = 1, shared_xaxes=True, vertical_spacing=0.02, subplot_titles=loc_list,
                x_title=x_axis
            )
    loc_list = data["location"].unique().to_list()
    for i, location in enumerate(loc_list):
        data_loc = data.filter(c("location") == location).pivot(index=x_axis, on="year", values=values)
        for j, col in enumerate(data_loc.columns[1:]):
            fig.add_trace(
                go.Scatter(
                    x=data_loc[x_axis].to_list(), y=data_loc[col].to_list(), mode='lines',
                    name="Measurement", legendgroup="Measurement",  showlegend=i+j == 0,
                    line=dict(color="gray"), opacity=0.3)
            , row=i+ 1, col=1)
            
        data_loc = data_loc.with_columns(
            pl.mean_horizontal(pl.all().exclude(x_axis)).alias("mean"),
            pl.concat_list(pl.all().exclude(x_axis)).list.eval(pl.element().std()).list.get(0).alias("std")
        ).with_columns(
            (c("mean") + z_score*c("std")).alias("upper_bound"),
            (c("mean") - z_score*c("std")).clip(lower_bound=0.0).alias("lower_bound"),
        )
        fig.add_trace(
            go.Scatter(
                x=data_loc[x_axis].to_list(), y=data_loc["mean"].to_list(), mode='lines',
                name="Mean", legendgroup="Mean",  showlegend=i == 0,
                line=dict(color="red"))
        , row=i+ 1, col=1)

        fig.add_trace(
            go.Scatter(
                x=data_loc[x_axis].to_list(), y=data_loc["upper_bound"].to_list(), mode='lines',
                name="z_score", legendgroup="z_score",  showlegend=i == 0,
                line=dict(color="red"), opacity=0.5)
        , row=i+ 1, col=1)
        fig.add_trace(
            go.Scatter(
                x=data_loc[x_axis].to_list(), y=data_loc["lower_bound"].to_list(), mode='lines',
                name="z_score", legendgroup="z_score",  showlegend=False,
                line=dict(color="red"), opacity=0.5)
        , row=i+ 1, col=1)
    fig.update_layout(
        margin=dict(t=60, l=65, r= 10, b=60), 
        width=1000,   # Set the width of the figure
        height=len(loc_list)*300,
        title= dict(text=title),
    )
    fig.write_html(f"{plot_folder}/{plot_name}.html")
