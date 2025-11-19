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
FCR_TYPE = ["DA", "Weighted mean FRC", "Max FRC"]
# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)


data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    total_scenarios_synthesized=smallflex_input_schema.discharge_volume_synthesized["scenario"].max() # type: ignore
)


# %%
output_folder = f"{file_names["output"]}/frc_sensivity_factor"
hydro_power_mask = "CTP"
battery_size = "2MW_4MWh"
build_non_existing_dirs(output_folder)


data_config.battery_rated_power = BATTERY_SIZE[battery_size]["rated_power"]
data_config.battery_capacity = BATTERY_SIZE[battery_size]["capacity"]
results_data = {}
income_list: list = []

for year in YEAR_LIST:
    data_config.year = year

    
    first_stage_optimization_results, basin_volume_expectation, fig_1 = (
        first_stage_stochastic_pipeline(
            data_config=data_config,
            smallflex_input_schema=smallflex_input_schema,
            hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
        )
    )

    pbar = tqdm(FCR_TYPE, desc=f"Year {year} scenarios", position=0)
    for fcr_factor in pbar:
        pbar.set_description(
            f"Optimization with {fcr_factor}  for year {year}"
        )
        data_config.with_ancillary = fcr_factor != "DA"
        data_config.fcr_value = "max" if fcr_factor == "Max FRC" else "avg"

        second_stage_optimization_results, adjusted_income, fig_2 = (
            second_stage_deterministic_pipeline(
                data_config=data_config,
                smallflex_input_schema=smallflex_input_schema,
                basin_volume_expectation=basin_volume_expectation,
                hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask],
            )
        )

        income_list.append((year, fcr_factor, adjusted_income / 1e3))
        results_data[fcr_factor.replace(" ", "_").lower()] = second_stage_optimization_results
    
results_data["adjusted_income"] = pl.DataFrame(
    income_list,
    schema=["year", "FCR Factor", "adjusted_income"],
    orient="row",
).pivot(values="adjusted_income", on="year", index="FCR Factor").sort("FCR Factor")

print_pl(results_data["adjusted_income"], float_precision=0)
dict_to_duckdb(results_data, f"{output_folder}/results.duckdb")

results_data["adjusted_income"].write_csv(f"{output_folder}/adjusted_income.csv")