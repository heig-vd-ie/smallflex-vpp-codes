import os
import tqdm
import polars as pl
import datetime
import re
from auxiliary.auxiliary import save_pyarrow_data


def read_gletsch_csv_data(hydro_mateo_path: str, years: list[str], ar_selection: bool, where: str = None):
    """
    A function to read the csv files of gletsch dataset
    :param hydro_mateo_path: path of Gletsch data
    :param years: list of years
    :param ar_selection: doe we want to select the ar files or not
    :param where: where to save the data
    :return: all data
    """
    all_data = pl.DataFrame()
    for year in years:
        folder = hydro_mateo_path + year
        file_names = os.listdir(folder)
        file_names = list(filter(lambda file: "_AR.csv" in file if ar_selection else "_AR.csv" not in file, file_names))
        for file_name in tqdm.tqdm(file_names, desc="Read files of the year " + year):
            file_path = os.path.join(folder, file_name)
            prediction_date = datetime.datetime.strptime(re.search('_\d+', file_name).group().split("_")[-1], "%Y%m%d%H")
            if pl.scan_csv(file_path, separator=" ", has_header=False, null_values=["NA"]).fetch(1).rows()[0][0] == "Index":
                skip_rows = 1
            else:
                skip_rows = 0
            if pl.scan_csv(file_path, separator=" ", has_header=False, null_values=["NA"]).fetch(1).rows()[0] != ("Index", "sim", "obs"):
                df_temp = pl.read_csv(file_path, separator=" ", has_header=False, null_values=["NA"], skip_rows=skip_rows)
                df_temp = df_temp.with_columns(pl.lit(None).alias(c).cast(pl.Float64) for c in set(['column_' + str(i) for i in range(1, 7)]).difference(df_temp.columns))
                df_temp = df_temp.with_columns(pl.lit(prediction_date).alias("prediction_date"))
                all_data = pl.concat([all_data, df_temp])
    all_data = all_data.rename({"column_1": "date", "column_2": "time", "column_3": "X0.05", "column_4": "X0.5", "column_5": "X0.95", "column_6": "obs"}).with_columns(pl.concat_str(["date", "time"], separator=" ").alias("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S")).select(
        pl.col(["prediction_date", "datetime", "X0.05", "X0.5", "X0.95", "obs"]))
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data
