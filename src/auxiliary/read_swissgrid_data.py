from auxiliary.auxiliary import save_pyarrow_data
import os
import polars as pl
import tqdm

def read_spot_price_swissgrid(local_file_path=".cache/data/swissgrid/spot", where=".cache/interim/swissgrid/spot_price.parquet"):
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
