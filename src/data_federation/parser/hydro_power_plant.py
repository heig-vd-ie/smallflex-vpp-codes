import polars as pl

import re

from utility.general_function import  modify_string

def get_hydro_power_plant_data(file_path: str) -> tuple[pl.DataFrame, float]:
    
    data: list[str] = open(file_path, "r").readlines()

    data = list(map(lambda x: x.replace("\n", ""), data))

    idx_1: int = data.index("<List>")
    idx_2: int = data.index("</List>")
    up_height: float = float(
        list(filter(lambda x: "Upstream reservoir level" in x, data))[0].split(" : ")[-1].split(" ")[0]
    )
    down_height: float = float(
        list(filter(lambda x: "Downstream reservoir level" in x, data))[0].split(" : ")[-1].split(" ")[0]
    )

    data_pl = pl.DataFrame(
        [x.split("\t")[:-1] for x in data[idx_1+1:idx_2] if not re.match(r"^-+$",  x)],
        schema=modify_string(data[idx_1-2], {r"\s+": "\t"}).split("\t")[:-1], orient="row"
    ).with_columns(
        pl.all().cast(pl.Float64),
        pl.lit(up_height - down_height).alias("head"),
    )
    return data_pl, up_height-down_height