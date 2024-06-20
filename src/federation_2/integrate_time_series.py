from auxiliary.download_data import download_from_switch
from auxiliary.read_gries_data import read_gries_txt_data
from auxiliary.read_gletsch_data import read_gletsch_csv_data
from auxiliary.read_swissgrid_data import (
    read_spot_price_swissgrid, read_balancing_price_swissgrid, read_fcr_price_swissgrid, read_frr_price_swissgrid
    )
import polars.selectors as cs
from auxiliary.auxiliary import read_pyarrow_data
from sqlalchemy import create_engine
from schema.schema import Base
from uuid import uuid4
import polars as pl
import os
from datetime import datetime
import tqdm


def generate_sql_tables_gries(
    restart_interim_data = False, read_parquet = ".cache/interim/Gries/gries.parquet", 
    write_sql = f'sqlite:///.cache/interim/time_series_schema.db'
):
    engine = create_engine(write_sql, echo=False)
    Base.metadata.create_all(engine) 
    table_names ={"ghi_": "Irradiation", "temperature_": "Temperature", "wind_speed_": "WindSpeed"}
    download_from_switch(switch_path="/Gries", local_file_path=".cache/data/Gries")
    results = []
    if (not os.path.exists(read_parquet)) | restart_interim_data: 
        raw_data_df = read_gries_txt_data(gries_path=".cache/data/Gries", where=read_parquet).rename({"datetime": "timestamp"})
    else:
        raw_data_df = read_pyarrow_data(where=read_parquet).rename({"datetime": "timestamp"})
    
    for old_name, new_name in table_names.items():
        altitude_mapping = dict(map(
            lambda x: (x, str((int(x.split("_")[-1]) + int(x.split("_")[-2])) / 2)),
            raw_data_df.select(cs.contains(old_name)).columns
        ))
        cleaned_data_df = raw_data_df.rename(altitude_mapping)\
            .melt(id_vars ="timestamp", value_vars=list(altitude_mapping.values()), variable_name="alt")\
            .with_columns(pl.col("alt").cast(pl.Float64))
        with tqdm.tqdm(total=1, ncols=100, desc="Write {} table in sqlite database".format(new_name)) as pbar:
            cleaned_data_df.write_database(
                table_name=new_name, connection=write_sql, if_table_exists="replace", engine="sqlalchemy")
            pbar.update()
        results.append(cleaned_data_df)

    return results


def generate_baseline_discharge_sql(
    read_parquet=".cache/interim/hydrometeo/gletsch_ar.parquet", 
    restart_interim_data = False, write_sql = f'sqlite:///.cache/interim/time_series_schema.db', 
    switch_path = "hydrometeo/Gletsch", local_file_path=r".cache/data/hydrometeo/Gletsch", years = ["2019", "2020"]
):
    engine = create_engine(write_sql, echo=False)
    Base.metadata.create_all(engine) 
    rivers  = {"Gletsch": 25, "Altstafel": 28, "Merzenbach": 0.5, "Blinne": 0.55}
    norm_gletsch = rivers["Gletsch"]  # mean discharge in m3/s
    dt = datetime.fromisoformat
    for year in years:
        download_from_switch(switch_path=switch_path + year, local_file_path=local_file_path + year, env_file=".env")

    if (not os.path.exists(read_parquet)) | restart_interim_data: 
        raw_data_df = read_gletsch_csv_data(
            hydro_mateo_path=local_file_path, years=years, ar_selection=True, where=read_parquet
        ).rename({"datetime": "timestamp"})
    else:
        raw_data_df = read_pyarrow_data(where=read_parquet).rename({"datetime": "timestamp"})
        
    cleaned_data_df = raw_data_df.set_sorted("timestamp", descending=False)\
        .upsample(time_column="timestamp", every="1h")\
        .select([
            pl.col("timestamp"),
            (pl.col("obs").interpolate(method="linear")/ norm_gletsch).alias("percentage")
        ]).drop_nulls()\
        .with_columns(
            (pl.col("percentage") * norm).alias(river) 
            for river, norm in rivers.items()
        ).melt(id_vars="timestamp", value_vars=list(rivers.keys()), variable_name="river")\
        .with_columns(
            pl.col("timestamp").map_elements(lambda t: uuid4().bytes).alias("uuid")
        )
        

    with tqdm.tqdm(total=1, ncols=100, desc="Write DischargeFlow table in sqlite database") as pbar:   
        cleaned_data_df.write_database(
            table_name="DischargeFlow", connection=write_sql, if_table_exists="replace", engine="sqlalchemy"
        )
        pbar.update()
    return cleaned_data_df


def generate_baseline_price_sql(
    read_parquet=".cache/interim/swissgrid", restart_interim_data = False,
    write_sql = f'sqlite:///.cache/interim/time_series_schema.db', switch_path = r"swissgrid/", 
    local_file_path=r"swissgrid/", market_categories  = ["spot", "balancing", "frr", "fcr"]
):
    engine = create_engine(write_sql, echo=False)
    Base.metadata.create_all(engine) 
    factor = {"EUR/MWh": 1, "ct/kWh": 10, "EURO/MWh": 1, "EUR/MW": 1}
    cleaned_data_df = pl.DataFrame()

    for market_category in market_categories:
        download_from_switch(
            switch_path=os.path.join(switch_path, market_category), 
            local_file_path=os.path.join(local_file_path, market_category), env_file=".env"
            )
        read_func = globals()["read_" + market_category + "_price_swissgrid"]
        if (not os.path.exists(os.path.join(read_parquet, market_category) + "_price.parquet")) | restart_interim_data:
            raw_data_df = read_func(
            local_file_path=os.path.join(local_file_path, market_category), 
            where=os.path.join(read_parquet, market_category) + "_price.parquet"
            ).rename({"datetime": "timestamp"})
        else:
            raw_data_df = read_pyarrow_data(
                where=os.path.join(read_parquet, market_category) + "_price.parquet"
            ).rename({"datetime": "timestamp"})
         
        old_columns = raw_data_df.select(pl.all().exclude("timestamp")).columns
        units = list(map(lambda x: x.replace("]", "").split(" [")[1], old_columns))
        markets = list(map(lambda x: x.split(" [")[0], old_columns))
        
        cleaned_data_temp_df = raw_data_df.with_columns(
            (pl.col(old_column) * factor[unit]).alias(market)
            for old_column, unit, market in zip(old_columns, units, markets)
        ).melt(id_vars ="timestamp", value_vars=markets, variable_name="market")
        
        cleaned_data_df = pl.concat([cleaned_data_df, cleaned_data_temp_df])
        
    cleaned_data_df = cleaned_data_df.drop_nulls(subset=["value"])
    with tqdm.tqdm(total=1, ncols=100, desc="Write MarketPrice table in sqlite database") as pbar:   
        cleaned_data_df.write_database(
            table_name="MarketPrice", connection=write_sql, if_table_exists="replace", engine="sqlalchemy"
        )
        pbar.update()

    return cleaned_data_df


if __name__ == "__main__":
    
    generate_baseline_price_sql(restart_interim_data=False)
    generate_baseline_discharge_sql(restart_interim_data=False)
    generate_sql_tables_gries(restart_interim_data=False)

