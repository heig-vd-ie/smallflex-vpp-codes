import os
import tqdm
import polars as pl
import pyarrow.parquet as pq
from auxiliary.auxiliary import build_non_existing_dirs


def read_ulrichen_txt_data(ulrichen_path: str, where: str = None):
    """
    A function to read the txt files of ulrichen dataset
    :param ulrichen_path: path of Ulrichen data
    :param where: where to save the data
    :return: all_data
    """
    all_data = pl.DataFrame()
    folder = ulrichen_path
    file_names = os.listdir(folder)
    file_names = list(filter(lambda file: "_data.txt" in file, file_names))
    columns_names = {"time": "datetime", "gre000z0": "global_radiation_w/m2", "tresurs0": "temperature_surface_c", "fve010z0": "wind_speed_m/s"}
    for file_name in tqdm.tqdm(file_names, desc="Read file"):
        file_path = os.path.join(folder, file_name)
        df_temp = pl.read_csv(file_path, separator=";", has_header=True, skip_rows=0, null_values=["-"])
        all_data = pl.concat([all_data, df_temp])
    all_data = all_data.select(list(columns_names.keys())).rename(columns_names).with_columns([pl.col("datetime").cast(pl.Utf8).str.to_datetime("%Y%m%d%H%M"), pl.col("global_radiation_w/m2").cast(pl.Float64), pl.col("temperature_surface_c").cast(pl.Float64), pl.col("wind_speed_m/s").cast(pl.Float64)])
    if where is not None:
        build_non_existing_dirs(os.path.dirname(where))
        pq.write_table(all_data.to_arrow(), where, compression=None)
    return all_data
