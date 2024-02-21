from auxiliary.download_data import download_from_switch
from auxiliary.read_gries_data import read_gries_txt_data
from auxiliary.read_gletsch_data import read_gletsch_csv_data
from auxiliary.read_swissgrid_data import read_spot_price_swissgrid, read_balancing_price_swissgrid, read_fcr_price_swissgrid, read_frr_price_swissgrid
from auxiliary.read_alpiq_data import read_apg_capacity, read_apg_energy, read_da, read_ida, read_reg_afrr_cap, read_reg_afrr_ene, read_reg_mfrr_cap, read_reg_mfrr_ene
from auxiliary.auxiliary import read_pyarrow_data
from sqlalchemy import create_engine
from schema.schema import Base
from uuid import uuid4
import polars as pl
import os
from datetime import datetime
import tqdm


def generate_sql_tables_gries(restart_interim_data = False, read_parquet = ".cache/interim/Gries/gries.parquet", write_sql = f'sqlite:///.cache/interim/time_series_schema.db'):
    download_from_switch(switch_path="/Gries", local_file_path=".cache/data/Gries")
    all_data = read_gries_txt_data(gries_path=".cache/data/Gries", where=read_parquet) if restart_interim_data else read_pyarrow_data(where=read_parquet)
    rename_columns = {"column_0": "uuid", "column_1": "timestamp", "column_2": "alt", "column_3": "value"}
    g_types  = ["ghi_", "temperature_", "wind_speed_"]
    table_names =dict(zip(g_types, ["Irradiation", "Temperature", "WindSpeed"]))
    df = [pl.DataFrame() for _ in g_types]
    engine = create_engine(write_sql, echo=False)
    Base.metadata.create_all(engine)
    for g_ind, g in enumerate(g_types):
        desired_columns = list(filter(lambda c: c.startswith(g), all_data.columns))
        altitudes = [(int(j[-2]) + int(j[-1])) / 2 for j in [i.split("_") for i in desired_columns]]
        for index in tqdm.tqdm(range(len(desired_columns)), ncols=100, desc="Load data of " + g.split("_")[0]):
            col = desired_columns[index]
            df_temp = (all_data.select([pl.col("datetime"), pl.col(col)]).map_rows(lambda t: (uuid4().bytes, t[0], altitudes[index], t[1])).rename(rename_columns))
            df[g_ind] = pl.concat([df[g_ind], df_temp])
        df[g_ind].write_database(table_name=table_names[g], connection=write_sql, if_exists="replace", engine="sqlalchemy")
    return df


def generate_baseline_discharge_sql(read_parquet=".cache/interim/hydrometeo/gletsch_ar.parquet", restart_interim_data = False, write_sql = f'sqlite:///.cache/interim/time_series_schema.db'):
    rivers  = {"Gletsch": 25, "Altstafel": 28, "Merzenbach": 0.5, "Blinne": 0.55}
    norm_gletsch = rivers["Gletsch"]  # mean discharge in m3/s
    dt = datetime.fromisoformat
    download_from_switch(switch_path="hydrometeo/Gletsch2020", local_file_path=".cache/data/hydrometeo/Gletsch2020", env_file=".env")
    download_from_switch(switch_path="hydrometeo/Gletsch2019", local_file_path=".cache/data/hydrometeo/Gletsch2019", env_file=".env")
    all_data_ar = read_gletsch_csv_data(hydro_mateo_path=r".cache/data/hydrometeo/Gletsch", years=["2019", "2020"], ar_selection=True, where=read_parquet) if ((not os.path.exists(read_parquet)) | restart_interim_data) \
        else read_pyarrow_data(where=read_parquet)
    data = all_data_ar.set_sorted("datetime", descending=False).upsample(time_column="datetime", every="1h").filter(pl.col("datetime") > dt("2020-09-15"))
    data = data.select(["datetime", "obs"]).with_columns(pl.col("obs").interpolate(method="linear")).drop_nulls().with_columns(pl.col("obs") / norm_gletsch)
    data = data.rename({"obs": "percentage"})
    rename_columns = {"column_0": "uuid", "column_1": "timestamp", "column_2": "river", "column_3": "value"}
    df = pl.DataFrame()
    engine = create_engine(write_sql, echo=False)
    Base.metadata.create_all(engine)
    for river in rivers.keys():
        norm = rivers[river]
        df_temp = data.with_columns((pl.col("percentage") * norm).alias("value")).select([pl.col("datetime"), pl.col("value")]).map_rows(lambda t: (uuid4().bytes, t[0], river, t[1])).rename(rename_columns)
        df = pl.concat([df, df_temp])
    df.write_database(table_name="DischargeFlow", connection=write_sql, if_exists="replace", engine="sqlalchemy")
    return df


def generate_baseline_price_sql(read_parquet=".cache/interim/swissgrid", restart_interim_data = False, write_sql = f'sqlite:///.cache/interim/time_series_schema.db', if_exists="replace",
                                country="CH", source="swissgrid-open"):
    market_categories  = ["spot", "balancing", "frr", "fcr"]
    market_dict = {
        "DA price": {"market": "DA", "direction": "sym"},
        "Short price": {"market": "BAL", "direction": "short"},
        "Long price": {"market": "BAL", "direction": "long"},
        "RR-neg": {"market": "RR-act", "direction": "neg"},
        "FRR-pos": {"market": "aFRR-cap", "direction": "pos"},
        "FRR-neg": {"market": "aFRR-cap", "direction": "neg"},
        "RR-pos": {"market": "RR-act", "direction": "pos"},
        "FCR": {"market": "FCR-cap", "direction": "sym"}
    }
    all_data = {}
    df = pl.DataFrame()
    for market_category in market_categories:
        download_from_switch(switch_path=os.path.join(r"swissgrid/", market_category), local_file_path=os.path.join(r".cache/data/swissgrid", market_category), env_file=".env")
        read_func = globals()["read_" + market_category + "_price_swissgrid"]
        all_data[market_category] = read_func(local_file_path=os.path.join(r".cache/data/swissgrid", market_category), where=os.path.join(read_parquet, market_category) + "_price.parquet") if (not os.path.exists(os.path.join(read_parquet, market_category) + "_price.parquet")) | restart_interim_data \
            else read_pyarrow_data(where=os.path.join(read_parquet, market_category) + "_price.parquet")
        rename_columns = {"column_0": "uuid", "column_1": "timestamp", "column_2": "market", "column_3": "direction",  "column_4": "country",  "column_5": "source", "column_6": "value"}
        engine = create_engine(write_sql, echo=False)
        Base.metadata.create_all(engine)
        columns = list(set(all_data[market_category].columns).difference(["datetime"]))
        for c in columns:
            c_name = c.split(" [")[0]
            market = market_dict[c_name]["market"]
            direction = market_dict[c_name]["direction"]
            unit = c.split(" [")[1].split("]")[0]
            factor = {"EUR/MWh": 1, "ct/kWh": 10, "EURO/MWh": 1, "EUR/MW": 1}
            df_temp = all_data[market_category].with_columns(pl.col(c).alias("value")).select([pl.col("datetime"), pl.col("value") * factor[unit]]).map_rows(lambda t: (uuid4().bytes, t[0], market, direction, country, source, t[1])).rename(rename_columns)
            df = pl.concat([df, df_temp])
    df = df.drop_nulls(subset=["value"])
    df.write_database(table_name="MarketPrice", connection=write_sql, if_exists=if_exists, engine="sqlalchemy")
    return df


def generate_baseline_alpiq_price_sql(read_parquet=".cache/interim/alpiq", restart_interim_data = False, write_sql = f'sqlite:///.cache/interim/time_series_schema.db', if_exists="replace"):
    market_categories  = {
        "apg_capacity": "apg/capacity", 
        "apg_energy": "apg/energy", 
        "da": "da-ida", 
        "ida": "da-ida",
        "reg_afrr_cap": "regelleistung/aFRR-capacity",
        "reg_afrr_ene": "regelleistung/aFRR-energy",
        "reg_mfrr_cap": "regelleistung/mFRR-capacity",
        "reg_mfrr_ene": "regelleistung/mFRR-energy",
        "reg_fcr": "regelleistung/FCR-capacity",
        "rte_cap": "rte/capacity",
        "rte_ene": "rte/energy"
        }
    market_dict = {
        "apg_capacity_SRR_NEG": {"market": "aFRR-cap", "direction": "neg", "country": "AT", "source": "apg"},
        "apg_capacity_SRR_POS": {"market": "aFRR-cap", "direction": "pos", "country": "AT", "source": "apg"},
        "apg_capacity_PRR_POSNEG": {"market": "FCR-cap", "direction": "sym", "country": "AT", "source": "apg"},
        "apg_capacity_TRR_POS": {"market": "mFRR-cap", "direction": "neg", "country": "AT", "source": "apg"},
        "apg_capacity_TRR_NEG": {"market": "mFRR-cap", "direction": "pos", "country": "AT", "source": "apg"},
        "apg_energy_SRR_POS": {"market": "aFRR-act", "direction": "neg", "country": "AT", "source": "apg"},
        "apg_energy_SRR_NEG": {"market": "aFRR-act", "direction": "pos", "country": "AT", "source": "apg"},
        "apg_energy_TRR_POS": {"market": "mFRR-act", "direction": "pos", "country": "AT", "source": "apg"},
        "apg_energy_TRR_NEG": {"market": "mFRR-act", "direction": "neg", "country": "AT", "source": "apg"},
        "da_CH": {"market": "DA", "direction": "sym", "country": "CH", "source": "alpiq"},
        "da_DE": {"market": "DA", "direction": "sym", "country": "DE", "source": "alpiq"},
        "da_AT": {"market": "DA", "direction": "sym", "country": "AT", "source": "alpiq"},
        "da_FR": {"market": "DA", "direction": "sym", "country": "FR", "source": "alpiq"},
        "da_IT-NORD": {"market": "DA", "direction": "sym", "country": "IT-NORD", "source": "alpiq"},
        "ida_DE": {"market": "IDA", "direction": "sym", "country": "DE", "source": "alpiq"},
        "ida_FR": {"market": "IDA", "direction": "sym", "country": "FR", "source": "alpiq"},
        "ida_AT": {"market": "IDA", "direction": "sym", "country": "AT", "source": "alpiq"},
        "ida_CH-IDA1": {"market": "IDA", "direction": "sym", "country": "CH-IDA1", "source": "alpiq"},
        "ida_CH-IDA2": {"market": "IDA", "direction": "sym", "country": "CH-IDA2", "source": "alpiq"},
        "ida_IT-NORD-MI-A1": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI-A1", "source": "alpiq"},
        "ida_IT-NORD-MI-A2": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI-A2", "source": "alpiq"},
        "ida_IT-NORD-MI-A3": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI-A3", "source": "alpiq"},
        "ida_IT-NORD-MI1": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI1", "source": "alpiq"},
        "ida_IT-NORD-MI2": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI2", "source": "alpiq"},
        "ida_IT-NORD-MI3": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI3", "source": "alpiq"},
        "ida_IT-NORD-MI4": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI4", "source": "alpiq"},
        "ida_IT-NORD-MI5": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI5", "source": "alpiq"},
        "ida_IT-NORD-MI6": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI6", "source": "alpiq"},
        "ida_IT-NORD-MI7": {"market": "IDA", "direction": "sym", "country": "IT-NORD-MI7", "source": "alpiq"},
        "reg_afrr_cap_pos": {"market": "aFRR-cap", "direction": "pos", "country": "DE", "source": "regelleistung"},
        "reg_afrr_cap_neg": {"market": "aFRR-cap", "direction": "neg", "country": "DE", "source": "regelleistung"},
        "reg_afrr_ene_pos": {"market": "aFRR-act", "direction": "pos", "country": "DE", "source": "regelleistung"},
        "reg_afrr_ene_neg": {"market": "aFRR-act", "direction": "neg", "country": "DE", "source": "regelleistung"},
        "reg_mfrr_cap_pos": {"market": "mFRR-cap", "direction": "pos", "country": "DE", "source": "regelleistung"},
        "reg_mfrr_cap_neg": {"market": "mFRR-cap", "direction": "neg", "country": "DE", "source": "regelleistung"},
        "reg_mfrr_ene_pos": {"market": "mFRR-act", "direction": "pos", "country": "DE", "source": "regelleistung"},
        "reg_mfrr_ene_neg": {"market": "mFRR-act", "direction": "neg", "country": "DE", "source": "regelleistung"},
        "reg_fcr_AT": {"market": "FCR-cap", "direction": "sym", "country": "AT", "source": "regelleistung"},
        "reg_fcr_CH": {"market": "FCR-cap", "direction": "sym", "country": "CH", "source": "regelleistung"},
        "reg_fcr_BE": {"market": "FCR-cap", "direction": "sym", "country": "BE", "source": "regelleistung"},
        "reg_fcr_NL": {"market": "FCR-cap", "direction": "sym", "country": "NL", "source": "regelleistung"},
        "reg_fcr_DE": {"market": "FCR-cap", "direction": "sym", "country": "DE", "source": "regelleistung"},
        "reg_fcr_FR": {"market": "FCR-cap", "direction": "sym", "country": "FR", "source": "regelleistung"},
        "reg_fcr_DK": {"market": "FCR-cap", "direction": "sym", "country": "DK", "source": "regelleistung"},
        "reg_fcr_SL": {"market": "FCR-cap", "direction": "sym", "country": "SL", "source": "regelleistung"},
        "rte_cap_aFRR_sym": {"market": "aFRR-cap", "direction": "sym", "country": "FR", "source": "rte"},
        "rte_cap_FCR_sym": {"market": "FCR-cap", "direction": "sym", "country": "FR", "source": "rte"},
        "rte_cap_mFRR_pos": {"market": "mFRR-cap", "direction": "pos", "country": "FR", "source": "rte"},
        "rte_cap_RR_pos": {"market": "RR-pos", "direction": "pos", "country": "FR", "source": "rte"},
        "rte_cap_aFRR_neg": {"market": "aFRR-cap", "direction": "neg", "country": "FR", "source": "rte"},
        "rte_cap_aFRR_pos": {"market": "aFRR-cap", "direction": "pos", "country": "FR", "source": "rte"},
        "rte_ene_mFRR-pos": {"market": "mFRR-act", "direction": "pos", "country": "FR", "source": "rte"},
        "rte_ene_RR-pos": {"market": "RR-act", "direction": "pos", "country": "FR", "source": "rte"},
        "rte_ene_RR-neg": {"market": "RR-act", "direction": "neg", "country": "FR", "source": "rte"},
        "rte_ene_mFRR-neg": {"market": "mFRR-act", "direction": "neg", "country": "FR", "source": "rte"},
    }
    all_data = {}
    df = pl.DataFrame()
    for market_category, market_folder in market_categories.items():
        download_from_switch(switch_path=os.path.join(r"alpiq/", market_folder), local_file_path=os.path.join(r".cache/data/alpiq", market_folder), env_file=".env")
        if (not os.path.exists(os.path.join(read_parquet, market_category) + ".parquet")) | restart_interim_data:
            read_func = globals()["read_" + market_category]
            all_data[market_category] = read_func(local_file_path=os.path.join(r".cache/data/alpiq", market_folder), where=os.path.join(read_parquet, market_category) + ".parquet")
        else :
            all_data[market_category] = read_pyarrow_data(where=os.path.join(read_parquet, market_category) + ".parquet")
        rename_columns = {"column_0": "uuid", "column_1": "timestamp", "column_2": "market", "column_3": "direction",  "column_4": "country",  "column_5": "source", "column_6": "value"}
        engine = create_engine(write_sql, echo=False)
        Base.metadata.create_all(engine)
        columns = list(set(all_data[market_category].columns).difference(["datetime", "market"]))
        for c in columns:
            unit = c.split("[")[1].split("]")[0]
            factor = {"EUR/MWh": 1, "ct/kWh": 10, "EURO/MWh": 1, "EUR/MW": 1}
            df_temp = all_data[market_category].with_columns(pl.col(c).alias("value")).select([pl.col("datetime"), market_category + "_" + pl.col("market"), pl.col("value") * factor[unit]]).map_rows(
                lambda t: (uuid4().bytes, t[0], market_dict[t[1]]["market"], market_dict[t[1]]["direction"], market_dict[t[1]]["country"], market_dict[t[1]]["source"], t[2])).rename(rename_columns)
            df = pl.concat([df, df_temp])
    df = df.drop_nulls(subset=["value"])
    df.write_database(table_name="MarketPrice", connection=write_sql, if_exists=if_exists, engine="sqlalchemy")
    return df


if __name__ == "__main__":
    generate_sql_tables_gries(restart_interim_data=False)
    generate_baseline_discharge_sql(restart_interim_data=False)
    generate_baseline_price_sql(restart_interim_data=False)
    generate_baseline_alpiq_price_sql(restart_interim_data=False, if_exists="append")
