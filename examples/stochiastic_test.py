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
file_name = ".cache/input/market_prices.csv"
custom_market_prices: pl.DataFrame = read_and_validate_custom_file(file_name=file_name)


first_stage_optimization_results, basin_volume_expectation, fig_1 = first_stage_stochastic_pipeline(
        data_config=data_config,
        smallflex_input_schema=smallflex_input_schema,
        hydro_power_mask=HYDROPOWER_MASK["DT"],
        plot_result=True,
        custom_market_prices=custom_market_prices
    )

fig_1.write_html(f"{plot_folder}/first_stage_results.html")