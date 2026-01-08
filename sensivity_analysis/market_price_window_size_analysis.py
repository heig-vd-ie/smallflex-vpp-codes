# %%
import os

os.chdir(os.getcwd().replace("/src", ""))

from examples import *


# %%
YEAR_LIST = [
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
]
WINDOW_SIZE = [
    7, 
    14, 
    28,
    56,
    112, 
    182]


# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)

data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    market_price_window_size=7,
    total_scenarios_synthesized=smallflex_input_schema.discharge_volume_synthesized["scenario"].max(), # type: ignore
    with_ancillary=False,
    battery_rated_power=0.0,
    battery_capacity=0.0
)


# %%

output_folder = f"{file_names["output"]}/market_price_window_size_analysis"
build_non_existing_dirs(output_folder)
mean_result = pl.DataFrame()
for year in YEAR_LIST:
    data_config.year = year
    plot_folder = f"{file_names["results_plot"]}/market_price_window_size_analysis/{year}"
    build_non_existing_dirs(plot_folder)

    previous_hydro_power_mask = None
    results_data = {}
    basin_volume_expectation: pl.DataFrame = pl.DataFrame()
    income_list: list = []
    scenario_list = list(product(*[list(HYDROPOWER_MASK.keys())[:2], WINDOW_SIZE]))
    pbar = tqdm(scenario_list, desc=f"Year {year} scenarios", position=0)
    for hydro_power_mask, window_size in pbar:
        pbar.set_description(
            f"Optimization with {hydro_power_mask} with {window_size} for year {year}"
        )
        scenario_name = "_".join([hydro_power_mask, str(window_size)])
        data_config.market_price_window_size = window_size

        if previous_hydro_power_mask != hydro_power_mask:
            previous_hydro_power_mask = hydro_power_mask

            first_stage_optimization_results, basin_volume_expectation, fig_1 = (
                first_stage_stochastic_pipeline(
                    data_config=data_config,
                    smallflex_input_schema=smallflex_input_schema,
                    hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
                )
            )
            
            results_data[f"first_stage_{scenario_name}"] = first_stage_optimization_results
            results_data[f"basin_volume_expectation_{scenario_name}"] = basin_volume_expectation
            
            if fig_1 is not None:
                fig_1.write_html(
                    f"{plot_folder}/{hydro_power_mask}_first_stage_results.html"
                )

        second_stage_optimization_results, adjusted_income, fig_2 = (
            second_stage_deterministic_pipeline(
                data_config=data_config,
                smallflex_input_schema=smallflex_input_schema,
                basin_volume_expectation=basin_volume_expectation,
                hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
            )
        )

        income_list.append((hydro_power_mask, window_size, adjusted_income / 1e3))

        if fig_2 is not None:
            fig_2.write_html(
                f"{plot_folder}/{scenario_name}_second_stage_results.html"
            )

        results_data[scenario_name] = second_stage_optimization_results
    results_data["adjusted_income"] = pl.DataFrame(
        income_list,
        schema=["hydro_power_mask", "window_size", "adjusted_income"],
        orient="row",
    ).pivot(on="hydro_power_mask", index="window_size", values="adjusted_income")

    print_pl(results_data["adjusted_income"], float_precision=0)
    
    
    col_name = "window_size"
    


    max_val = results_data["adjusted_income"].drop(col_name).to_numpy().max()
    result_percent = results_data["adjusted_income"].with_columns(
        pl.all().exclude(col_name)/max_val*100
    )

    mean_result = mean_result.vstack(result_percent)


    dict_to_duckdb(results_data, f"{output_folder}/{year}_results.duckdb")


mean_result = mean_result.group_by(col_name).agg(pl.all().mean()).sort(col_name)
print_pl(mean_result, float_precision=1)
mean_result.write_csv(f"{output_folder}/mean_result.csv")
