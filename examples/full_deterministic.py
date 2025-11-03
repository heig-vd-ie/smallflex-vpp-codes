# %%
import os

os.chdir(os.getcwd().replace("/src", ""))

from examples import *


# %%
YEAR_LIST = [
    2021,
    2022,
    2023
]

BATTERY_LIST = ["no_battery", "battery_2_MW_4MWh"]


# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)

data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    total_scenarios_synthesized=smallflex_input_schema.discharge_volume_synthesized["scenario"].max(), # type: ignore
    market_price_window_size=56
)


# %%
for market in MARKET[:1]:
    output_folder = f"{file_names["output"]}/full_deterministic_{market}"
    build_non_existing_dirs(output_folder)
    data_config.with_ancillary = market == "primary_ancillary"
    for year in YEAR_LIST:
        data_config.year = year
        plot_folder = f"{file_names["results_plot"]}/full_deterministic_{market}/{year}"
        build_non_existing_dirs(plot_folder)

        previous_hydro_power_mask = None
        results_data = {}
        basin_volume_expectation: pl.DataFrame = pl.DataFrame()
        first_stage_income_list: list = []
        second_stage_income_list: list = []
        scenario_list = list(product(*[HYDROPOWER_MASK.keys(), BATTERY_LIST]))
        pbar = tqdm(scenario_list, desc=f"Year {year} scenarios", position=0)
        for hydro_power_mask, battery_size in pbar:
            pbar.set_description(
                f"Optimization with {hydro_power_mask} with {battery_size} for year {year}"
            )
            scenario_name = "_".join([hydro_power_mask, battery_size])

            data_config.battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
            data_config.battery_capacity = BATTERY_SIZE[battery_size]["capacity"]

            if previous_hydro_power_mask != hydro_power_mask:
                previous_hydro_power_mask = hydro_power_mask

                first_stage_optimization_results, basin_volume_expectation, fig_1 = (
                    first_stage_deterministic_pipeline(
                        data_config=data_config,
                        smallflex_input_schema=smallflex_input_schema,
                        hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
                    )
                )
                first_stage_income_list.append(
                    (hydro_power_mask, first_stage_optimization_results["da_income"].sum() / 1e3)
                )
                
                results_data[f"first_stage_{hydro_power_mask}"] = first_stage_optimization_results
                results_data[f"basin_volume_expectation_{hydro_power_mask}"] = basin_volume_expectation
                
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
                    display_battery=False
                )
            )

            second_stage_income_list.append((hydro_power_mask, battery_size, adjusted_income / 1e3))

            if fig_2 is not None:
                fig_2.write_html(
                    f"{plot_folder}/{scenario_name}_second_stage_results.html"
                )

            results_data[scenario_name] = second_stage_optimization_results
        results_data["first_stage_income"] = pl.DataFrame(
            first_stage_income_list,
            schema=["hydro_power_mask", "income [kEUR]"],
            orient="row",
        )
        
        results_data["second_stage_income"] = pl.DataFrame(
            second_stage_income_list,
            schema=["hydro_power_mask", "battery_size", "adjusted_income"],
            orient="row",
        ).pivot(on="hydro_power_mask", index="battery_size", values="adjusted_income")
        
        print_pl(results_data["first_stage_income"], float_precision=0)
        print_pl(results_data["second_stage_income"], float_precision=0)

        dict_to_duckdb(results_data, f"{output_folder}/{year}_results.duckdb")
