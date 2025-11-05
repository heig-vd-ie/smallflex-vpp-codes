#%%
import os
os.chdir(os.getcwd().replace("/src", ""))

from examples import *


# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES)) # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(file_path=file_names["duckdb_input"])

data_config: DataConfig = DataConfig(
    nb_scenarios=20,
    first_stage_max_powered_flow_ratio=0.75,
    market_price_window_size=56,
    with_ancillary=True
)
hydro_power_mask = HYDROPOWER_MASK["continuous_turbine_pump"]
output_folder = f"{file_names["output"]}/imbalance"
plot_folder = f"{file_names["results_plot"]}/imbalance"
build_non_existing_dirs(output_folder)
build_non_existing_dirs(plot_folder)

# %%
first_stage_optimization_results, basin_volume_expectation, fig_1 = first_stage_stochastic_pipeline(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=hydro_power_mask,
        )
if fig_1 is not None:
    fig_1.write_html(f"{plot_folder}/first_stage_results.html")

# %%
results_data = {}
income_list: list = []
scenario_list = list(product(*[list(IMBALANCE_PARTICIPATION.keys()), list(BATTERY_SIZE.keys())]))
pbar = tqdm(scenario_list, desc=f"Optimization", position=0)
for imbalance_participation, battery_size in pbar:
    pbar.set_description(f"Optimization {imbalance_participation} imbalance participation and with {battery_size}")
    scenario_name = "_".join([imbalance_participation, battery_size])

    data_config.battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
    data_config.battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
    data_config.hydro_participation_to_imbalance = IMBALANCE_PARTICIPATION[imbalance_participation]

    second_stage_optimization_results, adjusted_income, fig_2 = second_stage_stochastic_pipeline(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            basin_volume_expectation=basin_volume_expectation,
            hydro_power_mask=hydro_power_mask)

    income_list.append((imbalance_participation, battery_size, adjusted_income/1e3))
    
    if fig_2 is not None:
        fig_2.write_html(f"{plot_folder}/{scenario_name}_second_stage_results.html")

    results_data[scenario_name] = second_stage_optimization_results
results_data["adjusted_income"] = pl.DataFrame(
    income_list, schema=["imbalance_participation", "battery_size", "adjusted_income"], orient="row"
).pivot(on="imbalance_participation", index="battery_size", values="adjusted_income")

print_pl(results_data["adjusted_income"], float_precision=0)
    
dict_to_duckdb(results_data, f"{output_folder}/results.duckdb")
