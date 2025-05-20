import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo


from utility.pyomo_preprocessing import (
    extract_optimization_results, pivot_result_table, remove_suffix)

from general_function import pl_to_dict, generate_log

log = generate_log(name=__name__)

def process_first_stage_results(model_instance: pyo.Model, water_basin_index: pl.DataFrame, flow_to_vol_factor: float) -> pl.DataFrame:

    
    volume_max_mapping: dict[str, float] = pl_to_dict(water_basin_index[["B", "volume_max"]])
    market_price = extract_optimization_results(
            model_instance=model_instance, var_name="market_price"
        )
    
    basin_volume = extract_optimization_results(
            model_instance=model_instance, var_name="basin_volume"
        ).with_columns(
            (c("basin_volume") / c("B").replace_strict(volume_max_mapping, default=None)).alias("basin_volume")
        )

    basin_volume = pivot_result_table(
        df = basin_volume, on="B", index=["T"], 
        values="basin_volume")

    nb_hours_mapping = pl_to_dict(extract_optimization_results(
            model_instance=model_instance, var_name="nb_hours"
    )[["T", "nb_hours"]])

    turbined_volume = extract_optimization_results(
            model_instance=model_instance, var_name="turbined_flow"
        ).with_columns(
            (
                c("turbined_flow") * flow_to_vol_factor *c("T").replace_strict(nb_hours_mapping, default=None)
            ).alias("turbined_volume")
        )
        
    turbined_volume = pivot_result_table(
        df = turbined_volume, on="H", index=["T"], 
        values="turbined_volume")
        
    pumped_volume = extract_optimization_results(
            model_instance=model_instance, var_name="pumped_flow"
        ).with_columns(
            (
                c("pumped_flow") * flow_to_vol_factor * c("T").replace_strict(nb_hours_mapping, default=None)
            ).alias("pumped_volume")
        )
        
    pumped_volume = pivot_result_table(
        df = pumped_volume, on="H", index=["T"],
        values="pumped_volume")

    pumped_power = extract_optimization_results(
            model_instance=model_instance, var_name="pumped_power"
        )

    pumped_power = pivot_result_table(
        df = pumped_power, on="H", index=["T"], 
        values="pumped_power")

    turbined_power = extract_optimization_results(
            model_instance=model_instance, var_name="turbined_power"
        )

    turbined_power = pivot_result_table(
        df = turbined_power, on="H", index=["T"],
        values="turbined_power")

    simulation_results: pl.DataFrame = market_price\
        .join(basin_volume, on = "T", how="inner")\
        .join(turbined_volume, on = "T", how="inner")\
        .join(pumped_volume, on = "T", how="inner")\
        .join(pumped_power, on = "T", how="inner")\
        .join(turbined_power, on = "T", how="inner")\
        .with_columns(
            ((
                pl.sum_horizontal(cs.starts_with("turbined_power")) -
                pl.sum_horizontal(cs.starts_with("pumped_power"))
            ) * c("T").replace_strict(nb_hours_mapping, default=None) * c("market_price")).alias("income")
        )
        
    hydro_name = list(map(str, list(model_instance.H))) # type: ignore

    simulation_results = simulation_results.with_columns(
        pl.struct(cs.ends_with(hydro) & ~cs.starts_with("basin_volume"))
        .pipe(remove_suffix).alias("hydro_" + hydro) 
        for hydro in hydro_name
    ).select(    
        ~(cs.starts_with("turbined") | cs.starts_with("pumped")) # type: ignore
    )
    
    return simulation_results