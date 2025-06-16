import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo


from utility.pyomo_preprocessing import (
    join_pyomo_variables, extract_optimization_results, pivot_result_table, remove_suffix)

from general_function import pl_to_dict, generate_log

log = generate_log(name=__name__)

def process_first_stage_results(model_instance: pyo.Model, market_price: pl.DataFrame, index: dict[str, pl.DataFrame], flow_to_vol_factor: float) -> pl.DataFrame:

    model_instance=model_instance
    market_price= market_price
    water_basin_index= index["water_basin"]
    pump_index = index["hydro_power_plant"].filter(c("type")== "pump")["H"].to_list()


    volume_max_mapping: dict[str, float] = pl_to_dict(water_basin_index["B", "volume_max"])
    market_price = market_price.select(
            c("T"),
            c("timestamp"),
            cs.ends_with("avg").name.map(lambda x: x.replace("avg", "") + "market_price")
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

    powered_volume = extract_optimization_results(
            model_instance=model_instance, var_name="flow"
        ).with_columns(
            (
                pl.when(c("H").is_in(pump_index)).then(-c("flow")).otherwise(c("flow")) * flow_to_vol_factor *
                c("T").replace_strict(nb_hours_mapping, default=None)
            ).alias("powered_volume")
        )
        
    powered_volume = pivot_result_table(
        df = powered_volume, on="H", index=["T"], 
        values="powered_volume")
        

    hydro_power = extract_optimization_results(
            model_instance=model_instance, var_name="hydro_power"
        )

    hydro_power = pivot_result_table(
        df = hydro_power, on="H", index=["T"], 
        values="hydro_power")

    simulation_results: pl.DataFrame = market_price\
        .join(basin_volume, on = "T", how="inner")\
        .join(powered_volume, on = "T", how="inner")\
        .join(hydro_power, on = "T", how="inner")\
        .with_columns(
            (
            pl.sum_horizontal(cs.starts_with("powered_volume")) *
            c("T").replace_strict(nb_hours_mapping, default=None) * c("market_price")
            ).alias("income")
        )
    
    
    return simulation_results

def process_second_stage_results(model_instance: pyo.Model, optimization_results: dict[str, pl.DataFrame], sim_nb: int) -> dict[str, pl.DataFrame]:
    
        for var_name in ["flow", "hydro_power", "basin_volume", "spilled_volume"]:
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