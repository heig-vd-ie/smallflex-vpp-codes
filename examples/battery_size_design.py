#%%
import os
os.chdir(os.getcwd().replace("/src", ""))

from examples import *


# %%
YEAR_LIST = [
    2021, 2022, 2023
]

# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES)) # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(file_path=file_names["duckdb_input"])

data_config: DataConfig = DataConfig(
    bound_penalty_factor=0.25,
    nb_scenarios=30,
    first_stage_max_powered_flow_ratio=0.75,
    market_price_window_size=56
)

output_folder = file_names["ancillary_market"]
build_non_existing_dirs(output_folder)

# %%


previous_hydro_power_mask = None
powered_volume_quota: pl.DataFrame = pl.DataFrame()
results_data = {}
basin_volume_expectation: pl.DataFrame = pl.DataFrame()
income_list: list = []
scenario_list = list(product(*[HYDROPOWER_MASK.keys(), BATTERY_SIZE.keys()]))



data_config.battery_rated_power = BATTERY_SIZE["battery_1_MW_2MWh"]["rated_power"]
data_config.battery_capacity = BATTERY_SIZE["battery_1_MW_2MWh"]["capacity"]

        

first_stage_optimization_results, basin_volume_expectation, fig_1 = first_stage_stochastic_pipeline(
    data_config=data_config,
    smallflex_input_schema=smallflex_input_schema,
    hydro_power_mask=HYDROPOWER_MASK["continuous_turbine_pump"],
)

stochastic_second_stage : StochasticSecondStage = StochasticSecondStage(
    data_config=data_config,
    smallflex_input_schema=smallflex_input_schema,
    basin_volume_expectation=basin_volume_expectation,
    hydro_power_mask=HYDROPOWER_MASK["continuous_turbine_pump"],
)

timeseries_forecast, timeseries_measurement = process_second_stage_timeseries_stochastic_data(
    smallflex_input_schema=smallflex_input_schema,
    data_config=data_config)


