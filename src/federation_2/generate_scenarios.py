"""
Generate scenarios
"""
import polars as pl
import tqdm
from datetime import timedelta
from sqlalchemy import create_engine
from schema.schema import Irradiation, WindSpeed, Temperature, DischargeFlow, MarketPrice
from federation_2.generate_forecast import day_ahead_forecast_arima_with_lag
from itertools import product
import logging
import dotenv
import coloredlogs
from auxiliary.auxiliary import load_configs
from auxiliary.plot_results import plot_forecast_results

log, config = load_configs()

def fill_null_remove_outliers(raw_data_df: pl.dataframe, d_time: timedelta, z_score: int) -> pl.DataFrame: 
    # Remove outlier using z_score test
    time_step_per_day: int = int(timedelta(days=1)/d_time)
    cleaned_data_df = raw_data_df.select([
        pl.col("timestamp").str.strptime(dtype=pl.Datetime, format="%Y-%m-%d %H:%M:%S%.6f").dt.truncate(d_time),
        pl.col("value")   
    ]).sort("timestamp")\
    .group_by("timestamp", maintain_order=True).agg(pl.col("value").mean())

    # Remove outlier using z_score test
    cleaned_data_df = cleaned_data_df\
    .with_columns(
        (((pl.col("value") - pl.col("value").mean()) / pl.col("value").std()).abs()).alias("z_score")
    ).filter(pl.col("z_score") < z_score)

    # fill missing values with data of previous, next day and week
    cleaned_data_df = cleaned_data_df.sort("timestamp")\
    .upsample(time_column="timestamp", every=d_time, maintain_order=True)\
    .with_columns(
        pl.col("value").fill_null(pl.col("value").shift(n = time_step_per_day))
        .fill_null(pl.col("value").shift(n = -time_step_per_day))
        .fill_null(pl.col("value").shift(n = 7*time_step_per_day))
        .fill_null(pl.col("value").shift(n = -7*time_step_per_day))
        .fill_null(strategy="forward"),
        pl.col("timestamp").dt.year().cast(pl.Int64).alias("year"),
        pl.col("timestamp").dt.week().cast(pl.Int64).alias("week"),  
        pl.col("timestamp").dt.weekday().cast(pl.Int64).alias("weekday"),    
        pl.col("timestamp").dt.month().cast(pl.Int64).alias("month"),  
    )
    # Remove 53 week if exist and change year of first and last week in order to have consistent year 
    if cleaned_data_df["year"].unique().shape[0] != 1:
        cleaned_data_df = cleaned_data_df\
        .filter(pl.col("week") <= 52)\
        .with_columns(
            pl.when((pl.col("week") == 52) & (pl.col("month") == 1))
            .then(pl.col("year") - 1)
            .when((pl.col("week") == 1) & (pl.col("month") == 12))
            .then(pl.col("year") + 1)
            .otherwise(pl.col("year")).alias("year")
        )
    else:
        cleaned_data_df = cleaned_data_df.sort(["year","week",  "weekday","timestamp"])
    # define time step
    cleaned_data_df = cleaned_data_df.with_columns([
    cleaned_data_df.group_by(['year', 'week', 'weekday'], maintain_order=True)\
    .agg(pl.int_range(0, pl.len()).alias("time_step"))\
    .explode("time_step")["time_step"]
    ]).with_columns(
        (pl.col("time_step") + (pl.col("weekday") - 1)*time_step_per_day + 1).alias("time_step")
    ).sort(["year", "week", "time_step"])\
    .filter(pl.struct(["year","week", "time_step"]).is_first_distinct())

    cleaned_data_df = cleaned_data_df\
    .select([
        pl.col("timestamp"), 
        pl.col(["year", "week", "time_step"]).cast(pl.Int64),
        pl.col("value"), 
        pl.lit(d_time).alias("delta_t")
    ]).sort(["year", "week", "time_step"])
    
    return cleaned_data_df 


def generate_scenarios(raw_data_df: pl.DataFrame, d_time: timedelta):


    scenario_name = ["min", "max", "median"]
    fill_null = False
    time_step_per_day: int = int(timedelta(days=1)/d_time)
    data_schema = pl.DataFrame(list(
        product(range(1, 53), range(1, 7*time_step_per_day+1), scenario_name)), 
        schema=(("week", pl.Int64), ("time_step", pl.Int64), ("scenario",pl.Utf8))
    )
    # Yearly stat : find min max and median year
    raw_data_df = raw_data_df.sort(["year", "week", "time_step"])
    yearly_mean_val = raw_data_df.group_by("year").agg(pl.col("value").mean()).sort("value")
    year_median = yearly_mean_val.with_columns((pl.col("value")-pl.mean("value"))**2).sort("value")["year"][0]
    year_mapping_df = year_mapping_df = pl.DataFrame({
        "year": [yearly_mean_val["year"][0], yearly_mean_val["year"][-1], year_median],
        "scenario" : scenario_name
    }).with_columns(pl.col("year").cast(pl.Int64))

    stat_data_by_year = raw_data_df.join(year_mapping_df, on="year", how="inner")\
        
    # Time_step : find min max and median for each time step
    stat_data_by_time_step =raw_data_df.group_by(["week", "time_step"], maintain_order=True).agg([
            pl.col("value").mean().alias("median"),
            pl.col("value").max().alias("max"),
            pl.col("value").min().alias("min"),
            pl.col("delta_t").first()
        ]).melt(
            id_vars =["week", "time_step", "delta_t"], value_name="time_step_value",
            value_vars=scenario_name, variable_name="scenario"
        )
    # Generate scenario using yearly stat data with null filled with main set stat
    scenario_data_df = stat_data_by_time_step.join(stat_data_by_year, on=["week", "time_step", "scenario"], how="outer")\
        .select([
            pl.col(["week", "time_step", "delta_t", "scenario"]),
            pl.col("value").fill_null(pl.col("time_step_value")).alias("value")
        ])
    # Interpolate oif there is remaining missing values
    if scenario_data_df.height <= data_schema.height:
        fill_null = True
        scenario_data_df = data_schema.join(scenario_data_df, on=["week", "time_step", "scenario"], how="outer")\
            .sort("scenario", "week", "time_step").interpolate()\
            .fill_null(strategy="forward").fill_null(strategy="backward")\
            .select([pl.col(["week", "time_step"]).cast(pl.Int64), "delta_t", "scenario","value"])
       
    return scenario_data_df, fill_null

def initialize_time_series(
    db_cache_file: str, table: DischargeFlow | Irradiation | WindSpeed |Temperature | MarketPrice,
    plot:bool=True
) -> pl.DataFrame:
    engine = create_engine(f"sqlite+pysqlite:///{db_cache_file}", echo=False)
    con = engine.connect()
    table_name = table.__tablename__

    d_time=timedelta(hours=1)
    missing_data = []

    total_data_forecast_df: pl.DataFrame = pl.DataFrame()
    # Fine type columns 
    if table_name == "MarketPrice":
        type_col = "market"
    elif table_name == "DischargeFlow":
        type_col = "river"
    else:
        type_col = "alt"
    # Define if forecasted values cannot be negative
    non_negative = table_name in ["Irradiation", "WindSpeed", "river"] 
    # Define z_score outlier detection depending on the table
    z_score = 4 if table_name in ["Irradiation", "Temperature"] else 10
    # query raw_data
    with tqdm.tqdm(total=1, ncols=100, desc="Read {} table in sqlite database".format(table_name)) as pbar: 
        raw_data_df = pl.read_database(query="""SELECT {f1}.* FROM {f1}""".format(f1=table_name), connection=con)  
        pbar.update()  

    for type_value in tqdm.tqdm(raw_data_df[type_col].unique(), desc="Clean data and generate scenarios of " + table_name):

        cleaned_data_df: pl.DataFrame = fill_null_remove_outliers(
            raw_data_df.filter(pl.col(type_col) == type_value), d_time=d_time, z_score=z_score
        )
        data_scenarios_df, fill_null = generate_scenarios(cleaned_data_df, d_time=d_time)

        data_forecast_df: pl.DataFrame = day_ahead_forecast_arima_with_lag(
            data_scenarios_df, d_time=d_time, non_negative=non_negative)

        total_data_forecast_df = pl.concat([
            total_data_forecast_df,
            data_forecast_df.with_columns(pl.lit(type_value).alias(type_col))
        ])
        if plot:
            plot_forecast_results(
                data_scenarios_df=data_scenarios_df, data_forecast_df=data_forecast_df,
                file_name=table_name + "_" + str(type_value))

        if fill_null:
            missing_data.append(str(type_value))

    total_data_forecast_df.write_database(
        table_name=table_name + "Norm", connection=f"sqlite+pysqlite:///{db_cache_file}", 
        if_exists="replace", engine="sqlalchemy"
    )
    if missing_data:
        log.warning(", ".join(missing_data) + " data are not complete from table " + table_name)
    return total_data_forecast_df