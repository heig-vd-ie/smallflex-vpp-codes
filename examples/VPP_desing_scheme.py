#%%
from examples import *


# %%
HYDROPOWER_MASK_LIST =["DT", "DTP", "CTP", "CTP", "CTP"]
MARKET_LIST = ['DA', 'DA', 'DA', 'FCR', 'Imbalance']

smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(file_path=settings.input_files.duckdb_input)

data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    market_price_window_size=56,
    total_scenarios_synthesized=smallflex_input_schema.discharge_volume_synthesized["scenario"].max(), # type: ignore
)

output_folder = f"{settings.output_files.output}/imbalance"
plot_folder = f"{settings.output_files.results_plot}/imbalance"
build_non_existing_dirs(output_folder)
build_non_existing_dirs(plot_folder)

# %%

# %%
results_data = {}
income_list: list = []
scenario_list = list(map(
    lambda x: list(x[0]) + [x[1]], 
    list(product(*[list(zip(HYDROPOWER_MASK_LIST, MARKET_LIST))[3:4], list(BATTERY_SIZE.keys())[4:5]]))
    ))

pbar = tqdm(scenario_list, desc=f"Optimization", position=0)
for hydro, market, battery_size in pbar:
    pbar.set_description(f"Optimization {hydro} hydro and {market} market with {battery_size}")
    scenario_name = "_".join([hydro, market, battery_size])
    
    if market == "FCR":
        data_config.with_ancillary = True
        data_config.hydro_participation_to_imbalance = False
        data_config.battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
        data_config.battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
        data_config.imbalance_battery_rated_power = 0
        data_config.imbalance_battery_capacity = 0
    elif market == "Imbalance":
        data_config.with_ancillary = False
        data_config.hydro_participation_to_imbalance = True
        data_config.battery_rated_power = 0
        data_config.battery_capacity = 0
        data_config.imbalance_battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
        data_config.imbalance_battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
    elif market == "DA":
        data_config.with_ancillary = False
        data_config.hydro_participation_to_imbalance = False
        data_config.battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
        data_config.battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
        data_config.imbalance_battery_rated_power = 0
        data_config.imbalance_battery_capacity = 0
    else: 
        raise ValueError(f"Market {market} not recognized.")
        
    first_stage_optimization_results, basin_volume_expectation, fig_1 = first_stage_stochastic_pipeline(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=HYDROPOWER_MASK[hydro],
        )

    second_stage_optimization_results, adjusted_income, fig_2 = second_stage_stochastic_pipeline(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            basin_volume_expectation=basin_volume_expectation,
            hydro_power_mask=HYDROPOWER_MASK[hydro])

    income_list.append((hydro + " " + market, battery_size, adjusted_income/1e3))
    
    if fig_2 is not None:
        fig_2.write_html(f"{plot_folder}/{scenario_name}_second_stage_results.html")

    results_data[scenario_name] = second_stage_optimization_results
results_data["adjusted_income"] = pl.DataFrame(
    income_list, schema=["market", "battery_size", "adjusted_income"], orient="row"
).pivot(on="market", index="battery_size", values="adjusted_income")

print_pl(results_data["adjusted_income"], float_precision=0)
    
dict_to_duckdb(results_data, f"{output_folder}/results.duckdb")
