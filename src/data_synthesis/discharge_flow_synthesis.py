
import polars as pl
from polars import col as c
import polars.selectors as cs
import numpy as np
import random
from datetime import date, timedelta
from itertools import chain
import tqdm 

from datetime import datetime, UTC

from plotly.subplots import make_subplots
import plotly.graph_objs as go

import quantecon as qe
from scipy.interpolate import UnivariateSpline
from statsmodels.tsa.seasonal import seasonal_decompose

from general_function import build_non_existing_dirs
from data_synthesis.data_synthesis_utility import state_to_value, transition_matrix, find_bin_edges


def discharge_flow_analyse(
    discharge_flow: pl.DataFrame, daily_bin: np.ndarray, smoothing_period: int = 3,
    horizon: int = 365, timestamp_nb: int = 24
    ) -> tuple[pl.DataFrame, list[pl.DataFrame]]:
    
    threshold = {0: 0.1, 1:0.16, 2:0.4, 3:1.0, 4:3.0, 5:10.0}
    
    
    timestamps = pl.datetime_range(
        start=date(2018, 1, 1), 
        end=date(2019, 1, 1), 
        interval=timedelta(hours=1), 
        eager=True, closed="left").to_list()

    
    trend_list: list = []
    scenario_list: list[pl.DataFrame] = []
    
    year_list = discharge_flow.with_columns(c("timestamp").dt.year())["timestamp"].unique().sort().to_list()
    
    fig = make_subplots(rows=len(year_list), cols=1, vertical_spacing=0.02)
    
    for year in year_list:

        data = discharge_flow\
            .filter(
                c("timestamp").is_between(datetime(year-1, 12, 31, tzinfo=UTC), datetime(year+1, 1, 2, tzinfo=UTC))
            ).sort("timestamp")["timestamp", "value"]
            
        dec_seas = seasonal_decompose(data['value'], period=timestamp_nb) 

        data = data\
            .with_columns(
                pl.Series(dec_seas.trend).alias("trend"),
                pl.Series(dec_seas.seasonal + dec_seas.resid).alias("resid")
            ).with_columns(
                c("resid").diff().alias("diff_resid")
            ).fill_nan(None).fill_null(strategy='forward').fill_null(strategy='backward')

        data = data.filter(c("timestamp").dt.year() == year).with_columns(
            c("timestamp").dt.ordinal_day().alias("day"),
            c("timestamp").dt.hour().alias("hour"),
            c("timestamp").dt.month().alias("month")
        )
        
        trend_list.append(data["trend"].slice(0, horizon*timestamp_nb).to_list())
        
        
        scenario = data\
            .group_by("day", maintain_order=True)\
            .agg(
                c("trend").mean().alias("days_trend"),
                c("resid")
            ).with_columns(
                c("days_trend").map_elements(lambda x: np.digitize(x, daily_bin) - 1, return_dtype=pl.Int64)
            ).with_columns(
                c("resid").list.eval(pl.element().abs()).list.max().alias("max_resid"),
                c("days_trend").replace_strict(threshold, default=None).alias("threshold")
            ).filter(
                c("max_resid") <= c("threshold")
            ).drop("max_resid", "threshold")
            
        scenario_list.append(scenario)
        
    x = np.arange(horizon*timestamp_nb)

    spline = UnivariateSpline(x, np.mean(np.array(trend_list), axis=0), s=smoothing_period*timestamp_nb)
    y_mean = spline(x)
    spline = UnivariateSpline(x, np.min(np.array(trend_list), axis=0), s=smoothing_period*timestamp_nb)
    y_min = spline(x)

    trend_data = pl.DataFrame(trend_list, schema=list(map(str, year_list))) \
        .with_columns(
            pl.Series(y_mean).alias("mean"),
            pl.Series(y_min).alias("min"),
        ).with_columns(
            (pl.all().exclude(["min", "mean"]) - c("min"))/c("mean")
        )
        
    for year in year_list:
        fig.add_trace(
            go.Scatter(
                x=timestamps, y=y_mean, mode='lines', name=f'{year}',
                marker=dict(color="red"), showlegend=False), 
            row=year_list.index(year)+1, col=1)
        fig.add_trace(
            go.Scatter(
                x=timestamps, y=y_min, mode='lines', name=f'{year}',
                marker=dict(color="red"), showlegend=False), 
            row=year_list.index(year)+1, col=1)
    
    return trend_data, scenario_list

    
def generate_discharge_flow_markov_chain(
    trend_data: pl.DataFrame, lower_bound = 3000,  upper_bound = 7500, nb_states = 15, nb_data_group: int = 24
    ) -> tuple[qe.MarkovChain, np.array, float]: # type: ignore
    
    trend_data = trend_data.slice(lower_bound, upper_bound- lower_bound)
    
    year_data_nb = trend_data.unpivot(on=cs.all())["value"].to_numpy()

    bin_edges = find_bin_edges(year_data_nb, nb_states=nb_states)[:-1]
    bin_edges[0] = max(0, bin_edges[0])

    last_state_scale = 1/np.mean(year_data_nb[year_data_nb >= bin_edges[-1]] - bin_edges[-1])

    dgz_trend_data = trend_data\
        .with_columns(
            pl.all().map_elements(lambda x: np.digitize(x, bin_edges) - 1, return_dtype=pl.Int64).clip(0)
        )
    dgz_trend_data = dgz_trend_data\
        .with_columns(
            (pl.arange(0, dgz_trend_data.height)//nb_data_group).alias("group")
        ).group_by("group")\
        .agg(pl.all().exclude("group").mean().round(0).cast(pl.Int32))\
        .sort("group").drop("group")

    tran_matrix = transition_matrix(profiles=dgz_trend_data.to_numpy().T, nb_states=nb_states)
    marcov_chains = (
            qe.MarkovChain(tran_matrix)
        )
    
    return marcov_chains, bin_edges, last_state_scale

def synthesize_discharge_flow_trend(
        trend_data, markov_chain: qe.MarkovChain, bin_edges: np.array, nb_profile: int, last_state_scale: float, # type: ignore
        nb_days = 365, mean_window: int = 36, lower_bound: list[int] = [1800, 2500], 
        upper_bound: list[int] = [7000, 8000]
    )-> pl.DataFrame: 

    init_vector = np.random.randint(0, len(bin_edges), nb_profile)

    syn_state = markov_chain.simulate(
            init=init_vector, ts_length=nb_days
        ).astype(int)

    start_data = trend_data.slice(0, lower_bound[0]).drop("mean", "min").mean().transpose()
    start_boundaries = (
        np.where(bin_edges <= start_data.mean()[0, 0] - start_data.std()[0, 0])[0][-1],
        np.where(bin_edges >= start_data.mean()[0, 0] + start_data.std()[0, 0])[0][0]      
    )
    
    end_data = trend_data.slice(upper_bound[1]).drop("mean", "min").mean().transpose()
    end_boundaries = (
        np.where(bin_edges <= end_data.mean()[0, 0] - end_data.std()[0, 0])[0][-1],
        np.where(bin_edges >= end_data.mean()[0, 0] + end_data.std()[0, 0])[0][0]      
    )
    
    syn_trend = pl.DataFrame(syn_state, schema=list(map(lambda x: str(x), range(len(syn_state)))))

    nb_timestamp = pl.DataFrame({
        str(col): pl.arange(0, nb_days*24, step= 24, eager=True)
        for col in range(nb_profile)
    }).with_columns(
        pl.all().map_elements(lambda x: x + np.random.randint(-12, 12), return_dtype=pl.Int64).diff()
    ).fill_null(24*365-pl.all().sum())

    syn_trend = syn_trend\
        .with_columns(
            pl.concat_list(c(col), nb_timestamp[col]).map_elements(lambda x: [x[0]]*x[1], return_dtype=pl.List(pl.Int64))
            for col in syn_trend.columns
        ).select(
            pl.all().implode().map_elements(lambda x: chain(*x), return_dtype=pl.List(pl.Int64))
        ).explode(pl.all())

    syn_trend = syn_trend.with_row_index(name="timestamp")\
            .with_columns(
                pl.when(c("timestamp") <= np.random.uniform(*lower_bound)) # type: ignore
                .then(pl.lit(random.randint(*start_boundaries))) # type: ignore
                .when(c("timestamp") >= np.random.uniform(*upper_bound)) # type: ignore
                .then(pl.lit(random.randint(*end_boundaries))) # type: ignore
                .otherwise(c(col)).alias(col) 
                for col in syn_trend.columns
            ).drop("timestamp")

    syn_trend = syn_trend.with_columns(
        pl.all().map_elements(
            lambda x: state_to_value(state=x, bin_edges=bin_edges,last_state_scale=last_state_scale), 
            return_dtype=pl.Float64)
    )

    syn_trend = syn_trend.with_columns(
            pl.all() * trend_data["mean"]  + trend_data["min"]  #.slice(3000, 4500)
        ).with_columns(
            pl.all().rolling_mean(window_size=mean_window).fill_null(strategy="backward")
        )

    return syn_trend

def pick_day_scenario(col: pl.Expr, scenario_list: list[pl.DataFrame]):
    scenario = random.choice(scenario_list)
    min_scenario: int = scenario["days_trend"].min()  # type: ignore
    max_scenario: int= scenario["days_trend"].max() # type: ignore
    return col.clip(min_scenario, max_scenario)\
        .map_elements(
            lambda x: scenario.filter(c("days_trend") == x).sample(with_replacement=True)["resid"][0].to_list(), 
            return_dtype=pl.List(pl.Float64)
        )

def add_daily_changes(
    syn_trend: pl.DataFrame, scenario_list: list[pl.DataFrame], daily_bin: np.array) -> pl.DataFrame: # type: ignore

    syn_daily_changes = syn_trend.with_row_index(name="day").with_columns(
        c("day")//24
    ).group_by("day", maintain_order=True).agg(pl.all().exclude("day").mean()).drop("day")\
    .with_columns(
        pl.all().map_elements(lambda x: np.digitize(x, daily_bin) - 1, return_dtype=pl.Int64)
    )


    syn_daily_changes = syn_daily_changes\
        .with_columns(
            c(col).pipe(pick_day_scenario, scenario_list=scenario_list)
            for col in syn_daily_changes.columns
        ).explode(pl.all())\
        .with_columns(
            pl.all().rolling_mean(window_size=3).shift(-1).fill_null(strategy="forward").fill_null(strategy="backward")
        )

    syn_profile = syn_trend + syn_daily_changes
    
    return syn_profile


def discharge_flow_synthesis_pipeline(
    discharge_flow: pl.DataFrame, nb_profile: int, daily_bin: np.ndarray=np.array([0, 0.22, 0.4, 1, 2.5, 3.5])
    ):
    with tqdm.tqdm(total=4) as pbar:
        pbar.set_description("Discharge flow data analysis")
        trend_data, scenario_list = discharge_flow_analyse(discharge_flow=discharge_flow, daily_bin=daily_bin)
        pbar.update(1)
        pbar.set_description("Markov chain generation")
        markov_chain, bin_edges, last_state_scale = generate_discharge_flow_markov_chain(trend_data=trend_data)
        pbar.update(1)
        pbar.set_description("Synthesize discharge flow trend")
        syn_trend = synthesize_discharge_flow_trend(
            trend_data=trend_data, markov_chain=markov_chain, nb_profile=nb_profile, bin_edges=bin_edges,
            last_state_scale=last_state_scale)
        pbar.update(1)
        pbar.set_description("Add daily changes")
        syn_profile = add_daily_changes(syn_trend=syn_trend, scenario_list=scenario_list, daily_bin=daily_bin)
        pbar.update(1)
    return syn_profile, trend_data