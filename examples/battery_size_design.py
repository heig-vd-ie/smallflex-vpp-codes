#%%
import os
import json
import tqdm
from itertools import product
from datetime import timedelta
import polars as pl
from polars import col as c


from general_function import dict_to_duckdb, build_non_existing_dirs, pl_to_dict

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DeterministicConfig
from pipelines.result_manager import PipelineResultManager
from pipelines.model_manager.deterministic_first_stage import DeterministicFirstStage
from pipelines.model_manager.deterministic_second_stage import DeterministicSecondStage
from timeseries_preparation.deterministic_data import process_timeseries_data
from data_display.baseline_plots import plot_result

from config import settings

os.chdir(os.getcwd().replace("/src", ""))
os.environ["GRB_LICENSE_FILE"] = os.environ["HOME"] + "/gurobi_license/gurobi.lic"

# %%
YEAR_LIST = [
    2023
]

PV_POWER_MASK = (c("sub_basin") == "Greisse_4") & (c("start_height") == 2050)
WIND_POWER_MASK = (c("sub_basin") == "Greisse_3") & (c("start_height") == 3050)

HYDROPOWER_MASK = {
    "discrete_turbine": c("name").is_in(["Aegina discrete turbine"]),
    "discrete_turbine_pump": c("name").is_in(["Aegina discrete turbine", "Aegina pump"]),
    "continuous_turbine_pump": c("name").is_in(["Aegina continuous turbine", "Aegina pump"]),
}
BATTERY_SIZE = {
    "no_battery": {"rated_power": 0, "capacity": 0},
    "battery_1_MW_2MWh": {"rated_power": 1, "capacity": 2},
    "battery_2_MW_4MWh": {"rated_power": 2, "capacity": 4},
    "battery_5_MW_10MWh": {"rated_power": 5, "capacity": 10},
    "battery_1_MW_4MWh": {"rated_power": 1, "capacity": 4},
    "battery_2_MW_8MWh": {"rated_power": 2, "capacity": 8},
    "battery_5_MW_20MWh": {"rated_power": 5, "capacity": 20},
}
# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)
output_folder = file_names["battery_size_design"]
build_non_existing_dirs(output_folder)

data_config: DeterministicConfig = DeterministicConfig(
        first_stage_timestep=timedelta(days=1),
        second_stage_sim_horizon=timedelta(days=5),
        nb_state_dict={0: 3},
        second_stage_quantile=0.15,
        battery_efficiency=0.95, 
        battery_rated_power=0, 
        battery_capacity=0,
        verbose=False
    )

result_manager: PipelineResultManager = PipelineResultManager()

# %%
for year in YEAR_LIST:
    data_config.year = year
    plot_folder = f"{file_names["results_plot"]}/battery_size_design/{year}"
    build_non_existing_dirs(plot_folder)
    
    previous_hydro_power_mask = None
    powered_volume_quota: pl.DataFrame = pl.DataFrame()
    results_data = {}

    pbar = tqdm.tqdm(list(product(*[HYDROPOWER_MASK.keys(), BATTERY_SIZE.keys()])), ncols=150, position=0)
    for hydro_power_mask, battery_size in pbar:
        pbar.set_description(f"Optimization with {hydro_power_mask} with {battery_size} for year {year}")
        scenario_name = "_".join([hydro_power_mask, battery_size])
        
        data_config.battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
        data_config.battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
        
        if previous_hydro_power_mask != hydro_power_mask:
            previous_hydro_power_mask = hydro_power_mask
    
            deterministic_first_stage: DeterministicFirstStage = DeterministicFirstStage(
                data_config=data_config,
                smallflex_input_schema=smallflex_input_schema,
                hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
                
            )
            timeseries = process_timeseries_data(
                smallflex_input_schema=smallflex_input_schema,
                basin_index_mapping=pl_to_dict(deterministic_first_stage.water_basin["uuid", "B"]),
                data_config=data_config,
                pv_power_mask=PV_POWER_MASK,
                wind_power_mask=WIND_POWER_MASK
            )
            
            deterministic_first_stage.set_timeseries(timeseries=timeseries)
            deterministic_first_stage.solve_model()
            powered_volume_quota: pl.DataFrame = result_manager.extract_powered_volume_quota(
                model_instance=deterministic_first_stage.model_instance,
                first_stage_nb_timestamp=data_config.first_stage_nb_timestamp,
            )
        

        deterministic_second_stage: DeterministicSecondStage = DeterministicSecondStage(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
            powered_volume_quota=powered_volume_quota
        )
        timeseries = process_timeseries_data(
            smallflex_input_schema=smallflex_input_schema,
            basin_index_mapping=pl_to_dict(deterministic_second_stage.water_basin["uuid", "B"]),
            data_config=data_config,
            pv_power_mask=PV_POWER_MASK,
            wind_power_mask=WIND_POWER_MASK
        )
        deterministic_second_stage.set_timeseries(timeseries=timeseries)
        deterministic_second_stage.solve_every_models()

        second_stage_optimization_results, powered_volume_overage, powered_volume_shortage = (
            result_manager.extract_second_stage_optimization_results(
                model_instances=deterministic_second_stage.model_instances,
                timestep_index=deterministic_second_stage.timeseries,
                nb_timestamp_per_ancillary=data_config.nb_timestamp_per_ancillary,
                with_battery=data_config.battery_capacity > 0
            )
        )

        results_data[scenario_name] = second_stage_optimization_results
        max_volume_mapping = pl_to_dict(deterministic_first_stage.water_basin["B", "volume_max"])
        start_volume_mapping = pl_to_dict(deterministic_first_stage.water_basin["B", "start_volume"])
        # Plot results
        fig = plot_result(
            results=second_stage_optimization_results,
            max_volume_mapping=max_volume_mapping,
            start_volume_mapping=start_volume_mapping,
            with_battery=data_config.battery_capacity > 0,
        )
        fig.write_html(f"{plot_folder}/{scenario_name}_results.html")

    dict_to_duckdb(results_data, f"{output_folder}/{year}_results.duckdb")