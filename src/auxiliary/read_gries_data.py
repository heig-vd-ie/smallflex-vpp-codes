"""
Read Gries data
"""
import os
import polars as pl
import pandas as pd
import pyarrow.parquet as pq
import tqdm
from auxiliary.auxiliary import build_non_existing_dirs


def read_gries_txt_data(gries_path: str, where: str = None):
    """
    A function to read the txt files of gries dataset
    :param gries_path: path of Gries data
    :param where: where to save the data
    :return: all_data
    """
    all_data = [pl.DataFrame() for i in range(3)]
    folder = gries_path
    file_names_all = os.listdir(folder)
    translate_tables = {"glob_Gri": "ghi", "tempGri": "temperature", "windGri": "wind_speed"}
    for l_index, l in enumerate(translate_tables.keys()):
        file_names = list(filter(lambda file: l in file, file_names_all))
        fil_col = {"datetime": "datetime"}
        for i in range(325, 334):
            fil_col[str(i)] = translate_tables[l] + "_" + str((i - 300) * 100 - 50) + "_" + str((i - 300) * 100 + 50)
        for file_name in tqdm.tqdm(file_names, desc="Read file"):
            file_path = os.path.join(folder, file_name)
            df_temp = pl.from_pandas(pd.read_csv(file_path, sep=r'\s+', skiprows=1)).slice(1).with_columns(pl.concat_str(["YYYY", "MM", "DD", "HH", pl.lit("0")], separator=" ").alias("datetime").str.to_datetime("%Y %m %d %H %M")).select(fil_col.keys()).rename(fil_col)
            all_data[l_index] = pl.concat([all_data[l_index], df_temp], how="diagonal")
    all_data1 = all_data[0].join(all_data[1], on="datetime")
    all_data2 = all_data1.join(all_data[2], on="datetime")
    if where is not None:
        build_non_existing_dirs(os.path.dirname(where))
        pq.write_table(all_data2.to_arrow(), where, compression=None)
    return all_data2
