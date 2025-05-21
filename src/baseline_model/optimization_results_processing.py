import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo


from utility.pyomo_preprocessing import (
    join_pyomo_variables, extract_optimization_results, pivot_result_table, remove_suffix)

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

def process_second_stage_results(model_instance: pyo.Model, optimization_results: dict[str, pl.DataFrame], sim_nb: int) -> dict[str, pl.DataFrame]:
    
        for var_name in ["flow", "power", "basin_volume", "spilled_volume"]:
            data = extract_optimization_results(
                    model_instance=model_instance, var_name=var_name
                ).with_columns(
                    pl.lit(sim_nb).alias("sim_nb")
                )
            optimization_results[var_name] = pl.concat([optimization_results[var_name], data], how="diagonal_relaxed")
                    
                
        start_basin_volume = extract_optimization_results(
                model_instance=model_instance, var_name="end_basin_volume"
            ).with_columns(
                pl.lit(sim_nb + 1).alias("sim_nb")
            ).rename({"end_basin_volume": "start_basin_volume"})

        remaining_volume = join_pyomo_variables(
                model_instance=model_instance, 
                var_list=["diff_volume_pos", "diff_volume_neg"], 
                index_list=["H"]
            ).select(
                c("H"),
                pl.lit(sim_nb + 1).alias("sim_nb"),
                (c("diff_volume_pos") - c("diff_volume_neg")).alias("remaining_volume"),
            )
        optimization_results["start_basin_volume"] = pl.concat([optimization_results["start_basin_volume"], start_basin_volume], how="diagonal_relaxed")
        optimization_results["remaining_volume"] = pl.concat([optimization_results["remaining_volume"], remaining_volume], how="diagonal_relaxed")

        return optimization_results

def combine_second_stage_results(
    optimization_results: dict[str, pl.DataFrame],  powered_volume: pl.DataFrame, market_price: pl.DataFrame, 
    index: dict[str, pl.DataFrame], flow_to_vol_factor: float
    ):

    remaining_volume = pivot_result_table(
        df = optimization_results["remaining_volume"], on="H", index="sim_nb", 
        values="remaining_volume")

    powered_volume = pivot_result_table(
        df = powered_volume, on="H", index="sim_nb", 
        values="powered_volume"
        ).with_columns(
            c("sim_nb").cast(pl.Int32).alias("sim_nb")
        )

    real_powered_volume = pivot_result_table(
        df = optimization_results["flow"]
            .group_by("sim_nb", "H")
            .agg((c("flow").sum() * flow_to_vol_factor ).alias("real_powered_volume")),
        on="H", index="sim_nb", 
        values="real_powered_volume")
    
            
    start_basin_volume = pivot_result_table(
        df = optimization_results["start_basin_volume"],
        on="B", index="sim_nb", 
        values="start_basin_volume")
    
    optimization_summary = remaining_volume\
    .join(powered_volume, on = "sim_nb", how="inner")\
    .join(real_powered_volume, on = "sim_nb", how="inner")\
    .join(start_basin_volume, on = "sim_nb", how="inner")

    volume = optimization_results["flow"].with_columns((c("flow") * flow_to_vol_factor).alias("volume"))
    volume = pivot_result_table(
        df = volume, on="H", index=["T", "sim_nb"], 
        values="volume", reindex=True)

    power = pivot_result_table(
        df = optimization_results["power"], on="H", index=["T", "sim_nb"], 
        values="power", reindex=True)
    
    volume_max_mapping: dict[str, float] = pl_to_dict(index["water_basin"][["B", "volume_max"]])
    basin_volume = optimization_results["basin_volume"].with_columns(
        (c("basin_volume") / c("B").replace_strict(volume_max_mapping, default=None)).alias("basin_volume")
    )

    basin_volume = pivot_result_table(
        df = basin_volume, on="B", index=["T", "sim_nb"], 
        values="basin_volume", reindex=True)
    
    spilled_volume = pivot_result_table(
        df = optimization_results["spilled_volume"], on="B", index=["T", "sim_nb"], 
        values="spilled_volume", reindex=True)

    market_price = market_price.with_row_index(name="real_index")\
        .select(c("real_index"), c("avg").alias("market_price"))

    combined_results = index["datetime"]\
        .with_row_index(name="real_index")[["real_index", "timestamp"]]\
        .join(basin_volume, on = "real_index", how="inner")\
        .join(volume, on = "real_index", how="inner")\
        .join(power, on = "real_index", how="inner")\
        .join(spilled_volume, on = "real_index", how="inner")\
        .join(market_price, on = "real_index", how="inner")\
        .with_columns(
            (pl.sum_horizontal(cs.starts_with("power")) * c("market_price")).alias("income"),
        )

    return optimization_summary, combined_results