
import polars as pl
from polars import col as c
import numpy as np
import random
from datetime import timedelta
import tqdm 
import pvlib
import pandas as pd
import math

import quantecon as qe

from data_synthesis.data_synthesis_utility import state_to_value, transition_matrix, find_bin_edges, digitize_col_with_custom_bin



def generate_irradiation_markov_chain(
    irradiation_ratio: pl.DataFrame,  nb_hourly_states: int = 10, daily_bin = np.array([0, 0.6, 0.85])
    )-> tuple[dict[str, dict[int, np.array]], dict[str, dict[int, float]], dict[str,  dict[int, qe.MarkovChain]], dict[str, qe.MarkovChain]]: # type: ignore

    nb_daily_states = len(daily_bin)
    
    daily_irradiation = irradiation_ratio.group_by(c("day")).agg(
            pl.all().exclude("day", "timestamp", "hour").mean()
    ).sort("day")

    dgz_daily_irradiation = daily_irradiation.with_columns(
        pl.all().exclude("day").pipe(digitize_col_with_custom_bin, bin_edges=daily_bin)
    )
    bin_edges_dict: dict[str, dict[int, np.array]] = {} # type: ignore
    last_state_scale_dict: dict[str, dict[int, float] ] = {}
    hourly_marcov_chains_dict: dict[str,  dict[int, qe.MarkovChain]] = {}
    daily_markov_chains_dict: dict[str, qe.MarkovChain] = {}
    for col in dgz_daily_irradiation.columns[1:]:
        tran_matrix = transition_matrix(profiles=dgz_daily_irradiation[col].to_numpy().T, nb_states=nb_daily_states)
        markov_chains = (
                qe.MarkovChain(tran_matrix)
            )
        daily_markov_chains_dict[col] = markov_chains
        bin_edges_dict[col], last_state_scale_dict[col], hourly_marcov_chains_dict[col] = (
            generate_hourly_markov_chain(
                irradiation_ratio=irradiation_ratio, dgz_daily_irradiation=dgz_daily_irradiation, col=col, 
                nb_daily_states=nb_daily_states, nb_hourly_states=nb_hourly_states
            )
        )
    return bin_edges_dict, last_state_scale_dict, hourly_marcov_chains_dict, daily_markov_chains_dict

def generate_hourly_markov_chain(
    irradiation_ratio: pl.DataFrame, dgz_daily_irradiation: pl.DataFrame, col: str, nb_daily_states: int, nb_hourly_states: int
    ) -> tuple[dict[int, np.array], dict[int, float], dict[int, qe.MarkovChain]]: # type: ignore
    
    last_state_scale_dict: dict[int, float] = {}
    bin_edges_dict: dict[int, np.array] = {} # type: ignore
    hourly_marcov_chains_dict: dict[int, qe.MarkovChain] = {}
    for i in range(nb_daily_states):
        day_index = dgz_daily_irradiation.filter(
                c(col) == i
            )["day"].to_list()

        day_by_scenario = irradiation_ratio.filter(c("day").is_in(day_index))

        scenario_all_data = day_by_scenario[col].drop_nulls().to_numpy().T

        bin_edges = find_bin_edges(scenario_all_data, nb_states=nb_hourly_states)[:-1]
        bin_edges_dict[i] = bin_edges   
        last_state_scale_dict[i] = np.mean(scenario_all_data[scenario_all_data >= bin_edges[-1]] - bin_edges[-1])
        
        dgz_day_by_scenario = day_by_scenario.with_columns(
            c(col).pipe(digitize_col_with_custom_bin, bin_edges=bin_edges_dict[i])
        ).pivot(
            on="day", values=col, index="hour"
        ).drop("hour").to_numpy().T
        tran_matrix = transition_matrix(profiles=dgz_day_by_scenario, nb_states=nb_hourly_states)
        markov_chains = (
                qe.MarkovChain(tran_matrix)
            )
        hourly_marcov_chains_dict[i] = markov_chains
    return bin_edges_dict, last_state_scale_dict, hourly_marcov_chains_dict

def irradiation_analysis(irradiation: pl.DataFrame, latitude: float, longitude: float) -> pl.DataFrame:

    tz = "UTC"
    location = pvlib.location.Location(latitude, longitude, tz=tz)
    max_year = irradiation["year"].max() + 1 # type: ignore
    min_year = irradiation["year"].min() 

    times = pd.date_range(start=f"{min_year}-01-01", end=f"{max_year}-01-01", freq=timedelta(hours=1), tz=tz)
    clearsky: pl.DataFrame = pl.from_pandas(location.get_clearsky(times, model='ineichen').reset_index()).slice(0, -1) # type: ignore


    clearsky = clearsky.select(
        c("index").dt.year().alias("year"),
        ((c("index").dt.ordinal_day() - 1) * 24 + c("index").dt.hour()).alias("timestamp"),
        pl.when(c("ghi") <= 50).then(pl.lit(0)).otherwise(c("ghi")).alias("value")
    ).pivot(on="year", values="value", index="timestamp").sort("timestamp").slice(0, 8760)

    irradiation = irradiation\
        .pivot(on="year", values="value", index="timestamp").sort("timestamp").slice(0, 8760)\
        .with_columns(
            pl.all().fill_null(strategy="forward").fill_null(strategy="backward")
        )

    irradiation_ratio = irradiation.drop("timestamp") / clearsky.drop("timestamp")

    irradiation_ratio = irradiation_ratio\
        .with_columns(pl.all().replace({np.inf: None, np.nan: None}).clip(0, 1.7))\
        .with_row_index(name="timestamp")\
        .with_columns(
            (c("timestamp")//24).alias("day"),
            (c("timestamp")%24).alias("hour")
        )
    return irradiation_ratio

def pick_irradiation_day(cluster: pl.Expr, syn_hourly: dict[int, list[list[float]]]) -> pl.Expr:
    return cluster.map_elements(
        lambda x: random.choice(syn_hourly[x]), 
        return_dtype=pl.List(pl.Float64)
    )


def irradiation_synthesis(
    daily_markov_chains_dict: dict[int, qe.MarkovChain], hourly_marcov_chains_dict: dict[str, dict[int, qe.MarkovChain]],
    bin_edges_dict, last_state_scale_dict, nb_profiles, nb_daily_states: int, nb_hourly_states: int, year: int,
    latitude: float, longitude: float, time_zone: str = "UTC",
    timestep: timedelta= timedelta(hours=1)
    ) -> pl.DataFrame:

    
    times = pd.date_range(
        start=f"{year}-01-01", end=f"{year + 1}-01-01", 
        freq=timestep, tz=time_zone, inclusive= "left")
    
    location = pvlib.location.Location(latitude, longitude, tz=time_zone)

    nb_timestep_per_day: int = timedelta(days=1)//timestep
    nb_profiles_per_year: int = math.ceil(nb_profiles/len(daily_markov_chains_dict))
    nb_days: int= times.shape[0]//nb_timestep_per_day
    
    clearsky: pl.DataFrame = pl.from_pandas(
        location.get_clearsky(times, model='ineichen').reset_index() # type: ignore
        )

    syn_irradiation_profile = pl.DataFrame()

    for i, year in enumerate(tqdm.tqdm(daily_markov_chains_dict.keys(), desc="Synthesizing irradiation data")):
        syn_hourly: dict[int, list[list[float]]] = {}
        for cluster in hourly_marcov_chains_dict[year].keys(): # type: ignore
            
            hourly_markov_chain: qe.MarkovChain = hourly_marcov_chains_dict[year][cluster] # type: ignore
            bin_edges: np.array = bin_edges_dict[year][cluster] # type: ignore
            last_state_scale: float = last_state_scale_dict[year][cluster]
            hourly_init_vector: np.array  = np.random.randint(0, nb_hourly_states, nb_profiles_per_year*nb_days) # type: ignore
            
            syn_state: np.array = hourly_markov_chain.simulate(init=hourly_init_vector, ts_length=nb_timestep_per_day).astype(int).T # type: ignore
            syn_hourly[cluster] = pl.DataFrame(syn_state).select(
                    pl.all().map_elements(
                    lambda x: state_to_value(state=x, bin_edges=bin_edges,last_state_scale=last_state_scale), 
                    return_dtype=pl.Float64).implode()
            ).transpose(column_names=["value"])["value"].to_list()
            
        daily_init_vector = np.random.randint(0, nb_daily_states, nb_profiles_per_year)
        syn_day = daily_markov_chains_dict[year].simulate(init=daily_init_vector, ts_length=nb_days).astype(int)
        
        new_profile: pl.DataFrame = pl.DataFrame(
                syn_day, schema=list(map(str, np.arange(i*nb_profiles_per_year, (i+ 1)*nb_profiles_per_year, 1)))
            ).with_columns(
                pl.all().pipe(pick_irradiation_day, syn_hourly=syn_hourly)
            ).explode(pl.all())
            
        syn_irradiation_profile = pl.concat([syn_irradiation_profile, new_profile], how="horizontal")
        
    syn_irradiation_profile = syn_irradiation_profile.select(
        pl.struct(
            (c(col) * clearsky["ghi"]).alias("ghi"),
            (c(col) * clearsky["dni"]).alias("dni"),
            (c(col) * clearsky["dhi"]).alias("dhi")
        ).alias(col)
        for col in syn_irradiation_profile.columns
    )
    
    return syn_irradiation_profile
        