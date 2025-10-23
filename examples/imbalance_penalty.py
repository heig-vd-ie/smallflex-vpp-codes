import os
os.chdir(os.getcwd().replace("/src", ""))

from examples import *

file_names: dict[str, str] = json.load(open(settings.FILE_NAMES)) # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(file_path=file_names["duckdb_input"])
basin_volume_expectation = pl.read_csv(".cache/basin_volume_expectation.csv")

# smallflex_input_schema_2 = SmallflexInputSchema().duckdb_to_schema(file_path=".cache/input/small_flex_input_data_2.db")
# smallflex_input_schema_dict = smallflex_input_schema.__dict__
# smallflex_input_schema_dict["market_price_measurement"] = smallflex_input_schema_2.market_price_measurement
# dict_to_duckdb(smallflex_input_schema_dict, file_names["duckdb_input"])


data_config: DataConfig = DataConfig(
    bound_penalty_factor=0.25,
    nb_scenarios=30,
    first_stage_max_powered_flow_ratio=0.75,
    market_price_window_size=56,
    verbose=False,
)

output_folder = file_names["ancillary_market"]
build_non_existing_dirs(output_folder)

previous_hydro_power_mask = None
powered_volume_quota: pl.DataFrame = pl.DataFrame()
results_data = {}




data_config.battery_rated_power = BATTERY_SIZE["battery_1_MW_2MWh"]["rated_power"]
data_config.battery_capacity = BATTERY_SIZE["battery_1_MW_2MWh"]["capacity"]


data_config.battery_rated_power = 0
data_config.battery_capacity = 0
    

stochastic_second_stage : StochasticSecondStage = StochasticSecondStage(
    data_config=data_config,
    smallflex_input_schema=smallflex_input_schema,
    basin_volume_expectation=basin_volume_expectation,
    hydro_power_mask=HYDROPOWER_MASK["continuous_turbine_pump"],
)

timeseries_forecast, timeseries_measurement = process_second_stage_timeseries_stochastic_data(
    smallflex_input_schema=smallflex_input_schema,
    data_config=data_config)

stochastic_second_stage.set_timeseries(timeseries_forecast=timeseries_forecast, timeseries_measurement=timeseries_measurement)

stochastic_second_stage.solve_every_models()

results, adjusted_income, imbalance_penalty = extract_third_stage_optimization_results(
    model_instances=stochastic_second_stage.third_stage_model_instances,
    timeseries=stochastic_second_stage.timeseries_measurement
    )