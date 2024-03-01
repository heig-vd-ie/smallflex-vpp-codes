"""
Generate scnearios
"""
import random
import polars as pl
import tqdm
from sqlalchemy import create_engine
from schema.schema import Irradiation, WindSpeed, Temperature, DischargeFlow, MarketPrice
from federation.generate_forecast import generate_dataframe_forecast


def query_time_series_data(db_cache_file, alt=None, river=None, market=None, tables=None):
    """
    Query time series
    """
    if tables is None:
        tables = [Irradiation, WindSpeed, Temperature, DischargeFlow, MarketPrice]
    engine = create_engine(f"sqlite+pysqlite:///{db_cache_file}", echo=False)
    con = engine.connect()
    data = {}
    for table_schema in tables:
        table_schema_name = table_schema.__tablename__
        if table_schema_name in ["DischargeFlow", "DischargeFlowNorm"]:
            if river is not None:
                data[table_schema_name] = pl.read_database(query="""SELECT {f1}.* FROM {f1} where {f1}.river = \"{f2}\"""".format(f1=table_schema.__tablename__, f2=river), connection=con)
        elif table_schema_name in ["MarketPrice", "MarketPriceNorm"]:
            if market is not None:
                data[table_schema_name] = pl.read_database(query="""SELECT {f1}.* FROM {f1} where {f1}.market = \"{f2}\"""".format(f1=table_schema.__tablename__, f2=market), connection=con)
        else:
            if alt is not None:
                alts = pl.read_database(query="""SELECT DISTINCT {f1}.alt FROM {f1}""".format(f1=table_schema.__tablename__), connection=con)
                target_alt = min(alts["alt"].to_list(), key=lambda x: abs(alt - x))
                data[table_schema_name] = pl.read_database(query="""SELECT {f1}.* FROM {f1} where {f1}.alt = {f2}""".format(f1=table_schema.__tablename__, f2=target_alt), connection=con)
    return data


def query_market_data(db_cache_file, market_combinations=None):
    """
    Query market time series
    """
    if market_combinations is None:
        market_combinations = {"DA-sym": {"market": "DA", "direction": "sym", "country": "CH", "source": "alpiq"},}
    engine = create_engine(f"sqlite+pysqlite:///{db_cache_file}", echo=False)
    con = engine.connect()
    data = {}
    for market_new_name in list(market_combinations.keys()):
        market = market_combinations[market_new_name]["market"]
        direction = market_combinations[market_new_name]["direction"]
        country = market_combinations[market_new_name]["country"]
        source = market_combinations[market_new_name]["source"]
        data[market_new_name] = pl.read_database(query="""SELECT {f1}.* FROM {f1} where {f1}.market = \"{f2}\" and {f1}.direction = \"{f3}\" and {f1}.country = \"{f4}\" and {f1}.source = \"{f5}\""""
                                                 .format(f1="MarketPrice", f2=market, f3=direction, f4=country, f5=source), connection=con)
        data[market_new_name] = data[market_new_name].with_columns(pl.lit(market_new_name).alias("market")).drop("direction", "country", "source")
    return data

def fill_null_remove_outliers(data, d_time, z_score):
    """
    Fill null remove outliers
    """
    data = data.select(["timestamp", "value"])
    avg = data.mean().get_column("value")[0]
    std = data.std().get_column("value")[0]
    data = data.with_columns(((pl.col("value") - avg) / std).alias("z_score"))
    data = data.filter((pl.col("z_score") < z_score) & (pl.col("z_score") > -z_score))
    # fill missing values with data of previous day
    data = data.with_columns(pl.col("timestamp").str.strptime(dtype=pl.Datetime, format="%Y-%m-%d %H:%M:%S%.6f")).set_sorted("timestamp", descending=False).upsample(time_column="timestamp", every=d_time)
    data = data.with_columns(pl.col("timestamp").dt.hour().alias("hour")).sort("hour").with_columns(pl.all().forward_fill()).select(["timestamp", "value"]).sort("timestamp")
    # define_wyt_time_series
    d_time_int = int(d_time.split("h")[0]) if "h" in d_time else int(d_time.split("m")[0]) / 60 if "m" in d_time else RuntimeError
    # define week and year
    arbitrary_year = pl.col("timestamp").dt.strftime("2030-%m-%d %H:%M:%S").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
    data = data.with_columns([arbitrary_year.dt.week().alias("week"), pl.col("timestamp").dt.year().alias("year")])
    # define time step
    data = data.group_by(["week", "year"], maintain_order=True).agg([pl.col("timestamp"), pl.col("value")]).with_columns(pl.col("value").map_elements(lambda x: range(len(x))).alias("time_step"))
    data = data.explode(["timestamp", "time_step", "value"]).select(["timestamp", "year", "week", "time_step", "value"])
    # remove first and end weeks to have consistent years
    data = data.filter((pl.col("week") < 53) & (pl.col("time_step") < int(168 / d_time_int))).with_columns(pl.lit(d_time_int).alias("delta_t").cast(pl.Float64))
    return data


def generate_scenarios(data, scen_number=5):
    """
    Generate scenarios
    """
    # in total
    arbitrary_year = pl.col("timestamp").dt.strftime("2030-%m-%d %H:%M:%S").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
    data_combine = data.with_columns([arbitrary_year, pl.lit(2030).alias("year")]).sort("timestamp").group_by("week", "time_step")
    data_avg = data.group_by("year").mean()
    data_year0 = {}
    data_year1 = {}
    data_year = {}
    for scen in range(scen_number):
        year = 2023 - scen
        q = random.uniform(0, 1)
        # in total
        year_random = data_avg.filter(pl.col("value") == pl.quantile("value", quantile=q, interpolation="lower")).get_column("year")[0]
        data_year0[scen] = data.filter(pl.col("year").is_in([year_random])).with_columns([pl.col("week").cast(pl.UInt32), pl.col("time_step").cast(pl.Int64), pl.lit(year).alias("scenario")])
        data_year_fill_null = data_combine.quantile(q, "nearest").select(["timestamp", "week", "time_step", "value", "delta_t"]).with_columns([pl.col("week").cast(pl.UInt32), pl.col("time_step").cast(pl.Int64), pl.lit(year).alias("scenario")])
        data_year0[scen] = data_year0[scen].join(data_year_fill_null, on=["week", "time_step", "delta_t"], how="outer").with_columns([pl.col(i).fill_null(pl.col(i+"_right")) for i in ["timestamp", "value", "scenario", "week", "time_step", "delta_t"]]).drop(["timestamp_right", "value_right", "scenario_right", "week_right", "time_step_right", "delta_t_right"])
        # yearly
        data_year1[scen] = data.filter(pl.col("year").is_in([year])).with_columns([pl.col("week").cast(pl.UInt32), pl.col("time_step").cast(pl.Int64), pl.lit(year).alias("scenario")])
        # join
        data_year[scen] = data_year1[scen].join(data_year0[scen], on=["week", "time_step", "delta_t"], how="outer").with_columns([pl.col(i).fill_null(pl.col(i+"_right")) for i in ["timestamp", "value", "scenario", "year", "week", "time_step", "delta_t"]]).drop(["timestamp_right", "value_right", "scenario_right", "year_right",  "week_right", "time_step_right", "delta_t_right"])
    data_scenarios = pl.concat(list(data_year.values())).with_columns(pl.col("scenario").cast(pl.Utf8))
    return data_scenarios


def initialize_time_series(db_cache_file, table_schema, scen_number=5, market_combinations=None):
    """
    Initialize price signals
    """
    engine = create_engine(f"sqlite+pysqlite:///{db_cache_file}", echo=False)
    con = engine.connect()
    table_schema_name = table_schema.__tablename__
    if table_schema_name == "MarketPrice":
        additional_column = "market"
        data = query_market_data(db_cache_file, market_combinations=market_combinations)
        non_negative = False
    elif table_schema_name == "DischargeFlow":
        additional_column = "river"
        rivers = pl.read_database(query="""SELECT DISTINCT {f1}.river FROM {f1}""".format(f1="DischargeFlow"), connection=con)["river"].to_list()
        data = {r: query_time_series_data(db_cache_file, river=r, tables=[DischargeFlow])["DischargeFlow"] for r in rivers}
        non_negative = True
    else:
        additional_column = "alt"
        alts = pl.read_database(query="""SELECT DISTINCT {f1}.alt FROM {f1}""".format(f1=table_schema.__tablename__), connection=con)["alt"].to_list()
        data = {alt: query_time_series_data(db_cache_file, alt=alt, tables=[table_schema])[table_schema_name] for alt in alts}
        non_negative = True if table_schema_name in ["Irradiation", "WindSpeed"] else False
    data_scenarios = {}
    data_forecast = {}
    result_data = pl.DataFrame(schema=  {"week": pl.UInt32, "time_step": pl.Int64, "scenario": pl.Utf8, "delta_t": pl.Float64, "value": pl.Float64, additional_column: pl.Utf8 if additional_column != "alt" else pl.Float64, "horizon": pl.Utf8})
    for d in tqdm.tqdm(data, desc="Load and generate scenarios of " + table_schema_name):
        z_score = 4 if d in ["Irradiation", "Temperature"] else 10
        data[d] = fill_null_remove_outliers(data[d], d_time="1h", z_score=z_score)
        data_scenarios[d] = generate_scenarios(data[d], scen_number=scen_number)
        data_forecast[d] = generate_dataframe_forecast(data_scenarios[d], d_time="1h", non_negative=non_negative)
        data_scenarios[d] = data_scenarios[d].select(["week", "time_step", "scenario", "delta_t", "value"]).with_columns([pl.lit(d).alias(additional_column), pl.lit("RT").alias("horizon")])
        data_forecast[d] = data_forecast[d].select(["week", "time_step", "scenario", "delta_t", "value"]).with_columns([pl.lit(d).alias(additional_column), pl.lit("DA").alias("horizon"), pl.col("week").cast(pl.UInt32)])
        result_data = pl.concat([result_data, data_scenarios[d], data_forecast[d]]) 
    result_data.write_database(table_name=table_schema_name + "Norm", connection=f"sqlite+pysqlite:///{db_cache_file}", if_exists="replace", engine="sqlalchemy")
    return result_data
        