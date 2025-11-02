# %% 

import os

os.chdir(os.getcwd().replace("/src", ""))

from results_display import *

# %% 
file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)
plot_folder = ".cache/plots/static_plots/"

build_non_existing_dirs(plot_folder)

year = 2022
hydro_power_mask = "continuous_turbine_pump"
battery = "battery_2_MW_4MWh"
data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    total_scenarios_synthesized=smallflex_input_schema.discharge_volume_synthesized["scenario"].max(), # type: ignore, # type: ignore
    year=year
)


row_titles = [
            "<b>Market prices [EUR]<b>",
            "<b>Reservoir level [%]<b>",
            "<b>Hydro power [MW]<b>",
        ]

width=650 * 2
height=300 * len(row_titles)

fig = make_subplots(
    rows=len(row_titles),
    cols=2,
    shared_xaxes=True,
    vertical_spacing=0.02,
    horizontal_spacing=0.035,
    row_titles=row_titles,
)

file_name = f".cache/output/market_price_market_quantile_analysis/{year}_results.duckdb"
result_dict = duckdb_to_dict(file_name)

# %% 


basin_volume_expectation = result_dict["basin_volume_expectation_discrete_turbine_pump_0"]

deterministic_second_stage: DeterministicSecondStage = DeterministicSecondStage(
                    data_config=data_config,
                    smallflex_input_schema=smallflex_input_schema,
                    basin_volume_expectation=basin_volume_expectation,
                    hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask]
                )

for i, window_size in enumerate([ 0, 30]):
    second_stage_result = result_dict[f"discrete_turbine_pump_{window_size}"]
    
    fig = plot_second_stage_result(
        fig=fig,
        col=i+1,
        results=second_stage_result,
        water_basin=deterministic_second_stage.water_basin,
        basin_volume_expectation=deterministic_second_stage.basin_volume_expectation,
        display_battery=False
    )
fig.update_layout(
                margin=dict(t=10, l=25, r=8, b=60),
                width=width,  # Set the width of the figure
                height=height,
                legend_tracegroupgap=215,
                barmode = "overlay"
            )


for ann in fig.layout.annotations: # type: ignore
    if ann.text in row_titles:
        ann.update(font=dict(size=20))  # set your desired size here


fig.update_layout(
    legend=dict(
        x=0.5,
        y=1.2, 
        xanchor="center",
        orientation="h",
        font=dict(size=20)
    )
)
fig.write_image(f"{plot_folder}/market_quantile_analysis.svg", width=width, height=height, scale=1)
