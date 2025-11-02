# %%
import os

os.chdir(os.getcwd().replace("/src", ""))

from results_display import *

# %%
plot_folder = ".cache/plots/static_plots/"
build_non_existing_dirs(plot_folder)


width = 900
height = 400

# %%
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)

data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    total_scenarios_synthesized=1000 # type: ignore
)

deterministic_first_stage: DeterministicFirstStage = DeterministicFirstStage(
        data_config=data_config,
        smallflex_input_schema=smallflex_input_schema,
        hydro_power_mask=HYDROPOWER_MASK["discrete_turbine"]
    )
fig = plot_reservoir_level_measurement(
    basin_height_measurement=smallflex_input_schema.basin_height_measurement,
    basin_volume_table=deterministic_first_stage.basin_volume_table)
fig.update_layout(
    width=width,
    height=height)

fig.write_image(f"{plot_folder}/reservoir_level_measurement.svg", width=width, height=height, scale=1)
