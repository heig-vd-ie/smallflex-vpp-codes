"""
Read alpiq data
"""
from auxiliary.auxiliary import save_pyarrow_data
import polars as pl
import os
import tqdm

def read_apg_capacity(**kwargs):
    return read_apg(energy=False, **kwargs)

def read_apg_energy(**kwargs):
    return read_apg(energy=True, **kwargs)


def read_reg_mfrr_cap(**kwargs):
    return read_reg_afrr_cap(**kwargs)

def read_reg_mfrr_ene(**kwargs):
    return read_reg_afrr_ene(**kwargs)


def read_da(local_file_path, **kwargs):
    return read_da_ida(local_file_path=os.path.join(local_file_path, "DA_2015-2023") + ".csv", **kwargs)


def read_ida(local_file_path, **kwargs):
    return read_da_ida(local_file_path=os.path.join(local_file_path, "IDA_2015-2023") + ".csv", **kwargs)


def read_apg(local_file_path, where=None, energy=False):
    col_name = "Energy Price [€/MWh]" if energy else "Capacity Price [€/MWh]"
    final_col = "[EUR/MWh]" if energy else "[EUR/MW]"
    all_data = pl.DataFrame()
    file_names = os.listdir(local_file_path)
    for file_name in tqdm.tqdm(file_names, desc="Read files of apg price"):
        file_path = os.path.join(local_file_path, file_name)
        df_temp = pl.read_csv(file_path, separator=",", has_header=True, null_values=["NA"])
        df_temp = df_temp.with_columns([
            pl.col("Time from [CET/CEST]").alias("datetime"), 
            pl.concat_str(["Type", "Direction"], separator="_").alias("market"), 
            (pl.col("Quantity [MW]") * pl.col(col_name)).alias("volume_price")
            ]).group_by(by=["datetime", "market"]).sum().with_columns(
                (pl.col("volume_price") / pl.col("Quantity [MW]")).alias(final_col)
                ).select(["datetime", "market", final_col]).with_columns(
                    pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S")
                    )
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    all_data = all_data.unique().sort("datetime")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_da_ida(local_file_path, where=None):
    for file_name in tqdm.tqdm([local_file_path], desc="Read files of apg price"):
        all_data = pl.read_csv(file_name, separator=";").slice(offset=4)
        columns = list(set(all_data.columns) - {"ID"})
        all_data = all_data.rename({"ID": "datetime"}).with_columns(
            pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S")
            ).with_columns([
                pl.col(c).cast(pl.Float64) for c in columns
                ]).melt(id_vars="datetime").rename({"variable": "market", "value": "[EUR/MWh]"}).drop_nulls(subset=["[EUR/MWh]"]).sort("datetime")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_reg_afrr_cap(local_file_path, where=None):
    col_name = "GERMANY_AVERAGE_CAPACITY_PRICE"
    all_data = pl.DataFrame()
    file_names = list(filter(lambda x: x.startswith("RESULT_OVERVIEW"), os.listdir(local_file_path)))
    for file_name in tqdm.tqdm(file_names, desc="Read files of regelleistung price"):
        file_path = os.path.join(local_file_path, file_name)
        df_temp = pl.read_excel(file_path)
        act_col_name = list(filter(lambda x: x.startswith(col_name), df_temp.columns))
        df_temp = df_temp.select([
            pl.col("DATE_FROM"),
            pl.col("PRODUCT"),
            pl.col(act_col_name).alias("[EUR/MW]").cast(pl.Float64, strict=False),
        ]).with_columns([
            pl.col("PRODUCT").str.split_exact("_", 2).struct.rename_fields(["first_part", "second_part", "third_part"]).alias("fields")
        ]).unnest("fields").select([
            (pl.concat_str(["DATE_FROM", "second_part"], separator=" ") + ":00") .str.to_datetime("%m-%d-%y %H:%M").alias("datetime"),
            pl.col("first_part").str.to_lowercase().alias("market"),
            pl.col("[EUR/MW]")
        ])
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    all_data = all_data.unique().sort("datetime")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_reg_afrr_ene(local_file_path, where=None):
    col_name = "GERMANY_AVERAGE_ENERGY_PRICE"
    all_data = pl.DataFrame()
    file_names = list(filter(lambda x: x.startswith("RESULT_OVERVIEW"), os.listdir(local_file_path)))
    for file_name in tqdm.tqdm(file_names, desc="Read files of regelleistung price"):
        file_path = os.path.join(local_file_path, file_name)
        df_temp = pl.read_excel(file_path, read_csv_options={"null_values": ["n.a."]})
        act_col_name = list(filter(lambda x: x.startswith(col_name), df_temp.columns))
        df_temp = df_temp.select([
            pl.col("DELIVERY_DATE"),
            pl.col("PRODUCT"),
            pl.col(act_col_name).alias("[EUR/MW]").cast(pl.Float64, strict=False),
        ]).with_columns([
            pl.col("PRODUCT").str.split_exact("_", 2).struct.rename_fields(["first_part", "second_part", "third_part"]).alias("fields")
        ]).unnest("fields").with_columns(pl.col("second_part").cast(pl.Int64)).select([
            (pl.col("DELIVERY_DATE") + "00:00").str.to_datetime("%m-%d-%y %H:%M").alias("datetime"),
            pl.col("second_part"),
            pl.col("first_part").str.to_lowercase().alias("market"),
            pl.col("[EUR/MW]")
            ])
        n_step = df_temp["second_part"].max()
        df_temp = df_temp.with_columns(((pl.col("second_part") - 1)*24*60*60*1e3 / n_step).cast(pl.Duration(time_unit="ms")))
        df_temp = df_temp.select([pl.col("datetime") + pl.col("second_part"), pl.col("market"), pl.col("[EUR/MW]")])
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    all_data = all_data.unique().sort("datetime")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_reg_fcr(local_file_path, where=None):
    col_dict = {
        "AT_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "AT",
        "AUSTRIA_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "AT",
        "BE_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "BE",
        "BELGIUM_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "BE",
        "DK_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "DK",
        "DENMARK_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "DK",
        "FRANCE_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "FR",
        "FR_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "FR",
        "CH_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "CH",
        "SWITZERLAND_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "CH",
        "DE_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "DE",
        "GERMANY_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "DE", 
        "NL_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "NL",
        "NETHERLANDS_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "NL",
        "SLOVENIA_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "SL",
        "SI_SETTLEMENTCAPACITY_PRICE_[EUR/MW]": "SL"
    }

    col_name = tuple(col_dict.keys())
    all_data = pl.DataFrame()
    file_names = list(filter(lambda x: x.startswith("RESULT_OVERVIEW"), os.listdir(local_file_path)))
    for file_name in tqdm.tqdm(file_names, desc="Read files of fcr regelleistung price"):
        file_path = os.path.join(local_file_path, file_name)
        df_temp = pl.read_excel(file_path, read_csv_options={"null_values": ["n.a.", "-"]})
        act_col_name = list(filter(lambda x: x.startswith(col_name), df_temp.columns))
        fin_col_name = [col_dict[c] for c in act_col_name]
        df_temp = df_temp.with_columns([
        pl.col(c).alias(col_dict[c]).cast(pl.Float64, strict=False) for c in act_col_name
            ]).select([pl.col(c) for c in ["DATE_FROM", "PRODUCTNAME"] + fin_col_name]).with_columns([
                pl.col("PRODUCTNAME").str.split_exact("_", 2).struct.rename_fields(["first_part", "second_part", "third_part"]).alias("fields")
            ]).unnest("fields").with_columns([
                (pl.concat_str(["DATE_FROM", "second_part"], separator=" ") + ":00") .str.to_datetime("%m-%d-%y %H:%M").alias("datetime"),
            ]).select([pl.col(c) for c in ["datetime"] + fin_col_name]).melt(id_vars="datetime").rename({"variable": "market", "value": "[EUR/MW]"}).sort("datetime")
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    all_data = all_data.unique().sort("datetime")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_rte_cap(local_file_path, where=None):
    market_dict = {
        "Réserve rapide": "mFRR",
        "Réserve secondaire": "aFRR",
        "Réserve complémentaire": "RR",
        "Réserve primaire": "FCR"
    }
    direction_dict = {
        "A la hausse": "pos",
        "A la baisse": "neg",
        "A la hausse et à la baisse": "sym"
    }
        
    all_data = pl.DataFrame()
    file_names = list(filter(lambda x: x.endswith(".xls"), os.listdir(local_file_path)))
    for file_name in tqdm.tqdm(file_names, desc="Read files of rte price"):
        file_path = os.path.join(local_file_path, file_name)
        df_temp = pl.read_csv(file_path, truncate_ragged_lines=True, encoding='iso-8859-1', has_header=True, skip_rows=1, separator="\t", null_values=["*"])
        df_temp = df_temp.slice(1, -2).select([
            pl.col("Date"),
            pl.col("Heures").str.split_exact(" - ", 1).alias("time").struct.rename_fields(["first_part", "second_part"]),
            pl.col("Type de réserve").map_dict(market_dict).alias("market"),
            pl.col("Sens de l'ajustement").map_dict(direction_dict).alias("direction"),
            pl.col("Prix de la réserve (en euros/MW/30min)").alias("[EUR/MW]")
        ]).unnest("time").select([
            (pl.col("Date") + " " + pl.col("first_part")).alias("datetime").str.to_datetime("%d/%m/%Y %H:%M"),
            pl.col("market") + "_" + pl.col("direction"),
            pl.col("[EUR/MW]").str.replace(",", ".").cast(pl.Float64)
        ])
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    all_data = all_data.unique().drop_nulls(subset=["[EUR/MW]"]).sort("datetime")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_rte_ene(local_file_path, where=None):
    all_data = pl.DataFrame()
    file_names = list(filter(lambda x: x.endswith(".xls"), os.listdir(local_file_path)))
    for file_name in tqdm.tqdm(file_names, desc="Read files of rte price"):
        file_path = os.path.join(local_file_path, file_name)
        df_temp = pl.read_csv(file_path, truncate_ragged_lines=True, encoding='iso-8859-1', has_header=True, skip_rows=1, separator="\t", null_values=["*", "N/A"])
        df_temp = df_temp.slice(1, -2).select([
            pl.col("Heure de début").alias("datetime"),
            pl.col("Prix Moyen Pondéré, des Offres d'Ajustement Activées à la hausse, pour cause P=C, à partir d'offres dont le DMO est inférieur ou égal à 13 minutes (en euros/MWh)").cast(pl.Float64).alias("mFRR-pos"),
            pl.col("Prix Moyen Pondéré des Offres d'Ajustement Activées à la Baisse, pour cause P=C, à partir d'offres dont le DMO est inférieur ou égal à 13 minutes (en euros/MWh)").alias("mFRR-neg"),
            pl.col("Prix Moyen Pondéré, des Offres d'Ajustement Activées à la hausse, pour cause P=C, à partir d'offres dont le DMO est strictement supérieur à 13 minutes (en euros/MWh)").alias("RR-pos"),
            pl.col("Prix Moyen Pondéré des Offres d'Ajustement Activées à la Baisse, pour cause P=C, à partir d'offres dont le DMO est strictement supérieur à 13 minutes (en euros/MWh)").alias("RR-neg"),
        ]).slice(0,-1).with_columns(pl.col("datetime").str.to_datetime("%d/%m/%Y %H:%M")).melt(id_vars="datetime").rename({"variable": "market", "value": "[EUR/MWh]"}).drop_nulls(subset=["[EUR/MWh]"]).sort("datetime")
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    all_data = all_data.unique().sort("datetime")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data