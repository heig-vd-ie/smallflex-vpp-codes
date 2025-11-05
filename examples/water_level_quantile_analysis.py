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
BASIN_VOLUME_QUANTILE = {
    "1": [0.25, 0.15, 0.05],
    "2": [0.25, 0.15, 0.05],
    "3": [0.4, 0.3, 0.15],
    "4": [0.4, 0.3, 0.15],
    "5": [0.45, 0.35, 0.2],
    "6": [0.45, 0.35, 0.2],
}

BASIN_VOLUME_QUANTILE_MIN = {
    "1": [0.05, 0.025, 0.01],
    "2": [0.05, 0.025, 0.01],
    "3": [0.07, 0.035, 0.015],
    "4": [0.07, 0.035, 0.015],
    "5": [0.1, 0.06, 0.03],
    "6": [0.1, 0.06, 0.03],
}
BOUND_PENALTY_FACTOR = {
    "1": [0.3, 0.15, 0.05],
    "2": [0.5, 0.3, 0.1],
    "3": [0.3, 0.15, 0.05],
    "4": [0.5, 0.3, 0.1],
    "5": [0.3, 0.15, 0.05],
    "6": [0.5, 0.3, 0.1],
}

# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)

data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    total_scenarios_synthesized=smallflex_input_schema.discharge_volume_synthesized["scenario"].max(), # type: ignore
    with_ancillary=False,
    battery_rated_power=0.0,
    battery_capacity=0.0
)

# %%

output_folder = f"{file_names["output"]}/water_level_quantile_analysis"
build_non_existing_dirs(output_folder)
mean_result = pl.DataFrame()
for year in YEAR_LIST:
    data_config.year = year
    plot_folder = f"{file_names["results_plot"]}/water_level_quantile_analysis/{year}"
    build_non_existing_dirs(plot_folder)

    previous_hydro_power_mask = None
    results_data = {}
    basin_volume_expectation: pl.DataFrame = pl.DataFrame()
    income_list: list = []
    scenario_list = list(product(*[list(HYDROPOWER_MASK.keys())[:2], BASIN_VOLUME_QUANTILE.keys()]))
    pbar = tqdm(scenario_list, desc=f"Year {year} scenarios", position=0)
    for hydro_power_mask, quantile_config in pbar:
        pbar.set_description(
            f"Optimization with {hydro_power_mask} with {quantile_config} for year {year}"
        )
        scenario_name = "_".join([hydro_power_mask, str(quantile_config)])
        data_config.basin_volume_quantile = BASIN_VOLUME_QUANTILE[quantile_config]
        data_config.basin_volume_quantile_min = BASIN_VOLUME_QUANTILE_MIN[quantile_config]
        data_config.bound_penalty_factor = BOUND_PENALTY_FACTOR[quantile_config]

        if previous_hydro_power_mask != hydro_power_mask:
            previous_hydro_power_mask = hydro_power_mask
            
            stochastic_first_stage: StochasticFirstStage = StochasticFirstStage(
                data_config=data_config,
                smallflex_input_schema=smallflex_input_schema,
                hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
            )

            timeseries = process_first_stage_timeseries_data(
                smallflex_input_schema=smallflex_input_schema,
                data_config=data_config,
                water_basin_mapping=pl_to_dict(stochastic_first_stage.water_basin["uuid", "B"]),
            )
            stochastic_first_stage.set_timeseries(timeseries=timeseries)

            stochastic_first_stage.solve_model()
                    
            first_stage_optimization_results = extract_first_stage_optimization_results(
                model_instance=stochastic_first_stage.model_instance,
                timeseries=stochastic_first_stage.timeseries
            )
            results_data[f"first_stage_{scenario_name}"] = first_stage_optimization_results
            
    
        basin_volume_expectation = extract_basin_volume_expectation(
            model_instance=stochastic_first_stage.model_instance,
            optimization_results=first_stage_optimization_results,
            water_basin=stochastic_first_stage.upstream_water_basin,
            data_config=data_config
        )
        results_data[f"basin_volume_expectation_{scenario_name}"] = basin_volume_expectation
        second_stage_optimization_results, adjusted_income, fig_2 = (
            second_stage_deterministic_pipeline(
                data_config=data_config,
                smallflex_input_schema=smallflex_input_schema,
                basin_volume_expectation=basin_volume_expectation,
                hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
            )
        )

        income_list.append((hydro_power_mask, quantile_config, adjusted_income / 1e3))

        if fig_2 is not None:
            fig_2.write_html(
                f"{plot_folder}/{scenario_name}_second_stage_results.html"
            )

        results_data[scenario_name] = second_stage_optimization_results
    results_data["adjusted_income"] = pl.DataFrame(
        income_list,
        schema=["hydro_power_mask", "quantile_config", "adjusted_income"],
        orient="row",
    ).pivot(on="hydro_power_mask", index="quantile_config", values="adjusted_income")

    print_pl(results_data["adjusted_income"], float_precision=0)
    
    col_name = "quantile_config"
    max_val = results_data["adjusted_income"].drop(col_name).to_numpy().max()
    result_percent = results_data["adjusted_income"].with_columns(
        pl.all().exclude(col_name)/max_val*100
    )

    mean_result = mean_result.vstack(result_percent)

    dict_to_duckdb(results_data, f"{output_folder}/{year}_results.duckdb")

mean_result = mean_result.group_by(col_name).agg(pl.all().mean()).sort(col_name)
print_pl(mean_result, float_precision=1)
mean_result.write_csv(f"{output_folder}/mean_result.csv")