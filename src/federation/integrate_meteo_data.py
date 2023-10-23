from auxiliary.download_data import download_from_switch
from auxiliary.read_gries_data import read_gries_txt_data
from auxiliary.auxiliary import read_pyarrow_data
from sqlalchemy import create_engine
from schema.schema import Base
from uuid import uuid4
import polars as pl
import tqdm


def generate_sql_tables_gries(restart_interim_data = False, read_parquet = ".cache/interim/Gries/gries.parquet", write_sql = f'sqlite:///.cache/interim/Gries/gries_schema.db'):
    download_from_switch(switch_path="/Gries", local_file_path=".cache/data/Gries")
    all_data = read_gries_txt_data(gries_path=".cache/data/Gries", where=read_parquet) if restart_interim_data else read_pyarrow_data(where=read_parquet)
    #
    rename_columns = {"column_0": "uuid", "column_1": "timestamp", "column_2": "alt", "column_3": "value"}
    g_types  = ["ghi_", "temperature_", "wind_speed_"]
    table_names =dict(zip(g_types, ["Irradiation", "Temperature", "WindSpeed"]))
    df = [pl.DataFrame() for _ in g_types]
    #
    engine = create_engine(write_sql, echo=False)
    Base.metadata.create_all(engine)
    #
    for g_ind, g in enumerate(g_types):
        desired_columns = list(filter(lambda c: c.startswith(g), all_data.columns))
        altitudes = [(int(j[-2]) + int(j[-1])) / 2 for j in [i.split("_") for i in desired_columns]]
        for index in tqdm.tqdm(range(len(desired_columns)), ncols=100, desc="Load data of " + g.split("_")[0]):
            col = desired_columns[index]
            df_temp = all_data.select([pl.col("datetime"), pl.col(col)]).apply(lambda t: (uuid4().bytes, t[0], altitudes[index], t[1])).rename(rename_columns)
            df[g_ind] = pl.concat([df[g_ind], df_temp])
        df[g_ind].write_database(table_name=table_names[g], connection=write_sql, if_exists="replace", engine="sqlalchemy")
    return df


if __name__ == "__main__":
    generate_sql_tables_gries(restart_interim_data=False)
