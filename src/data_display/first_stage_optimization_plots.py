import plotly.graph_objs as go
import plotly.express as px
import polars as pl
from plotly.subplots import make_subplots
COLORS = px.colors.qualitative.Plotly

def plot_first_stage_summarized(model_instance, min_flow_factor, max_flow_factor, nb_days, year):
    result = pl.DataFrame({
        col : getattr(model_instance, col).extract_values().values() for col in ["powered_volume", "basin_volume", "market_price", "discharge_volume"]
    }).with_row_index(name="index")

    print(result[["powered_volume", "discharge_volume"]].sum()/1e6)

    fig = make_subplots(
            rows=3, cols = 1, shared_xaxes=True, vertical_spacing=0.05, x_title="<b>Index<b>", 
            row_titles= ["Price [EUR/MWh]", "Height [masl]", "Volume [Mm3]"] )
    fig.add_trace(
                go.Scatter(
                    x=result["index"].to_list(), y=result["market_price"].to_list(), mode='lines',line=dict(color=COLORS[0]),showlegend=False
                ), row=1, col=1
            ) 
    fig.add_trace(
                go.Scatter(
                    x=result["index"].to_list(), y=result["basin_volume"].to_list(), mode='lines', line=dict(color=COLORS[0]),showlegend=False
                ), row=2, col=1
            ) 
    fig.add_trace(
                go.Bar(
                    x=result["index"].to_list(), y=(result["powered_volume"]/1e6).to_list(), showlegend=False, marker=dict(color=COLORS[0]), 
                ), row=3, col=1
            )   
    fig.update_layout(
        margin=dict(t=60, l=65, r= 10, b=60), 
        width=1200,   # Set the width of the figure
        height=800,   #
        title= f"First stage optimization results ([{min_flow_factor}: {max_flow_factor}] flow factor and {nb_days} days per index and {year} year)")
    fig.show()