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


def read_da(local_file_path, **kwargs):
    return read_da_ida(local_file_path=os.path.join(local_file_path, "DA_2015-2023") + ".csv", **kwargs)


def read_ida(local_file_path, **kwargs):
    return read_da_ida(local_file_path=os.path.join(local_file_path, "IDA_2015-2023") + ".csv", **kwargs)


def read_apg(local_file_path, where= None, energy=False):
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


def read_da_ida(local_file_path, where= None):
    for file_name in tqdm.tqdm([local_file_path], desc="Read files of apg price"):
        all_data = pl.read_csv(file_name, separator=";").slice(offset=4)
        columns = list(set(all_data.columns) - {"ID"})
        all_data = all_data.rename({"ID": "datetime"}).with_columns(
            pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S")
            ).with_columns([
                pl.col(c).cast(pl.Float64) for c in columns
                ]).melt(id_vars="datetime").rename({"variable": "market", "value": "[EUR/MWh]"}).sort("datetime")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data
