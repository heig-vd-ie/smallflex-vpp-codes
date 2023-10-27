from auxiliary.auxiliary import save_pyarrow_data
import os
import pandas as pd
import polars as pl
import tqdm

def read_spot_price_swissgrid(local_file_path=r".cache/data/swissgrid/spot", where=r".cache/interim/swissgrid/spot_price.parquet"):
    all_data = pl.DataFrame()
    file_names = os.listdir(local_file_path)
    for file_name in tqdm.tqdm(file_names, desc="Read files of spot price"):
        file_path = os.path.join(local_file_path, file_name)
        df_temp = pl.read_csv(file_path, separator=",", has_header=True, null_values=["NA"])
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    all_data = all_data.drop_nulls(subset=["Date (GMT+1)"]).with_columns(pl.when(pl.col("Day Ahead Auction (CH)").is_null()).then(pl.col("Day Ahead Auction")).otherwise(pl.col("Day Ahead Auction (CH)")).cast(pl.Float64).alias("Price [EUR/MWh]")).select(["Date (GMT+1)", "Price [EUR/MWh]"]).with_columns(pl.col("Date (GMT+1)").str.to_datetime(format="%Y-%m-%dT%H:%M", exact=False)).rename({"Date (GMT+1)": "datetime"})
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_balancing_price_swissgrid(local_file_path=r".cache/data/swissgrid/balancing", where=r".cache/interim/swissgrid/balancing_price.parquet"):
    all_data = pl.DataFrame()
    file_names = os.listdir(local_file_path)
    file_names = list(filter(lambda file: ".xls" in file, file_names))
    for file_name in tqdm.tqdm(file_names, desc="Read files of balancing price"):
        file_path = os.path.join(local_file_path, file_name)
        df_pd_temp = pd.read_excel(file_path)
        df_temp = pl.from_pandas(df_pd_temp.iloc[11:, 0:3].set_axis(["datetime", "Long price [ct/kWh]", "Short price [ct/kWh]"], axis="columns"))
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    all_data = all_data.with_columns(pl.col("datetime").map_elements(lambda x: None if not x[0].isdigit() else x)).drop_nulls(subset="datetime").with_columns(pl.col("datetime").str.to_datetime(format="%d.%m.%Y %H:%M:%S"))
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_frr_price_swissgrid(local_file_path=r".cache/data/swissgrid/frr", where=r".cache/interim/swissgrid/frr_price.parquet"):
    all_data = pl.DataFrame()
    file_names = os.listdir(local_file_path)
    for file_name in tqdm.tqdm(file_names, desc="Read files of frr price"):
        file_path = os.path.join(local_file_path, file_name)
        xlsx_file = ".xlsx" in file_name
        df_pd_temp = pd.read_excel(file_path, sheet_name="Zeitreihen0h15", engine="xlrd" if not (".xlsx" in file_name) else None)
        df_temp = pl.from_pandas(df_pd_temp[["Unnamed: 0", "Durchschnittliche positive Sekundär-Regelenergie Preise\nAverage positive secondary control energy prices", "Durchschnittliche negative Sekundär-Regelenergie Preise\nAverage negative secondary control energy prices",
                                             "Durchschnittliche positive Tertiär-Regelenergie Preise\nAverage positive tertiary control energy prices", "Durchschnittliche negative Tertiär-Regelenergie Preise\nAverage negative tertiary control energy prices"]].set_axis(
            ["datetime", "FRR-pos [EURO/MWh]", "FRR-neg [EURO/MWh]", "RR-pos [EURO/MWh]", "RR-neg [EURO/MWh]"], axis="columns").iloc[1:, :])
        df_temp = df_temp.with_columns(pl.col("datetime").str.to_datetime("%d.%m.%Y %H:%M")) if df_temp["datetime"].dtype == pl.Utf8 else df_temp
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data


def read_fcr_price_swissgrid(local_file_path=r".cache/data/swissgrid/fcr", where=r".cache/interim/swissgrid/fcr_price.parquet"):
    all_data = pl.DataFrame()
    file_names = os.listdir(local_file_path)
    for file_name in tqdm.tqdm(file_names, desc="Read files of fcr price"):
        file_path = os.path.join(local_file_path, file_name)
        df_temp = pl.read_csv(file_path, separator=";", has_header=True, null_values=["NA"])
        df_temp = df_temp.with_columns(pl.concat_str(["date", "time"], separator=" ").alias("datetime"), (pl.col("volume") * pl.col("price")).alias("volume_price")).group_by(by="datetime").sum().with_columns((pl.col("volume_price") / pl.col("volume")).alias("FCR [EUR/MW]")).with_columns(
            pl.col("datetime").str.to_datetime("%y_%m_%d %H:%M")).sort("datetime").select(["datetime", "FCR [EUR/MW]"])
        all_data = pl.concat([all_data, df_temp], how="diagonal")
    if where is not None:
        save_pyarrow_data(all_data, where)
    return all_data
