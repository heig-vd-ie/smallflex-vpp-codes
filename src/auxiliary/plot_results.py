import polars as pl
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from auxiliary.auxiliary import build_non_existing_dirs
import plotly.offline

def plot_forecast_results(
    data_scenarios_df: pl.DataFrame, data_forecast_df: pl.DataFrame, file_name, 
    folder= r"./.cache/plot/prediction"
    ):

    build_non_existing_dirs(folder)
    idx_name = ["week", "time_step"]
    scenario_names = data_scenarios_df["scenario"].unique().to_list()

    data_scenarios_df = data_scenarios_df.pivot(values="value",index=idx_name, columns="scenario")\
        .sort(idx_name)
    data_forecast_df = data_forecast_df.pivot(values="value",index=idx_name, columns="scenario")\
        .sort(idx_name)
    x_axis = list(range(data_scenarios_df.height))
    fig = make_subplots(
        rows=len(scenario_names), shared_xaxes=True, vertical_spacing=0.05, x_title="<b>Date<b>",
        row_titles=scenario_names
    )
   
    for idx, scenario_name in enumerate(scenario_names):
        fig.add_trace(go.Scatter(
            x=x_axis, y=data_scenarios_df[scenario_name].to_list(),
            showlegend=idx==0, mode="lines", name="scenario",marker=dict(color="royalblue")
        ), row=idx + 1, col=1)
        fig.add_trace(go.Scatter(
            x=x_axis, y=data_forecast_df[scenario_name].to_list(),
            showlegend=idx==0, mode="lines", name="forecast",marker=dict(color="firebrick")
        ), row=idx + 1, col=1)

    save_fig(
        fig=fig, filename=folder + "/" + file_name + ".html",
        title="<b>{}<b>".format(file_name.replace("_", " "))
    )


def save_fig(fig, filename: str, title: str, label: list = None, ticks: list = None,
             width: int = 1600, height: int = 1000, title_y: float = 0.97, legend_size: int = 16,
             tick_size: int = 16, axis_title_size: int= 16, title_size: int = 16):
    """

    Args:
        fig:
        filename:
        title:
        auto_open:
        label:
        ticks:
        width:
        height:
        title_y:
        legend_size:
    """
    # fig.update_traces(marker_line_width=0, selector=dict(type="bar"))
    # fig.update_xaxes(showline=True, linecolor='black', mirror=True,tickfont_family="Arial Black")
    # fig.update_yaxes(showline=True, linecolor='black', mirror=True, tickfont_family="Arial Black")
    fig.update_layout(
        font={"size": title_size}, xaxis_title_font={"size":axis_title_size}, yaxis_title_font={"size":axis_title_size},
        margin=dict(t=55), title={'text':title, 'y': title_y, 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top'},
        legend={'font': {'size': legend_size}}
    )
    # for ax in fig['layout']:
    #     if ax[:5] == 'xaxis':
    #         fig['layout'][ax]['showgrid'] = False
    #         fig['layout'][ax]['zeroline'] = False
    #         fig['layout'][ax]['tickmode'] = "array"
    #         fig['layout'][ax]['ticktext'] =  label
    #         fig['layout'][ax]['tickvals'] = ticks
    #         fig['layout'][ax]['tickfont']['size'] = tick_size
    #         fig['layout'][ax]['tickfont']['color'] = "black"
    #     if ax[:5] == 'yaxis':
    #         fig['layout'][ax]['showgrid'] = False
    #         fig['layout'][ax]['zeroline'] = False
    #         fig['layout'][ax]['tickfont']['size'] = tick_size
    #         fig['layout'][ax]['tickfont']['color'] = "black"

    plotly.offline.plot(fig, filename=filename, auto_open=False)
