# %%
import os

os.chdir(os.getcwd().replace("/src", ""))
from general_function import duckdb_to_dict
from plotly.subplots import make_subplots
from results_display import *
# %%

file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore
smallflex_input_schema: SmallflexInputSchema = SmallflexInputSchema().duckdb_to_schema(
    file_path=file_names["duckdb_input"]
)
plot_folder = ".cache/plots/static_plots/"

build_non_existing_dirs(plot_folder)

battery = "battery_2_MW_4MWh"
data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    total_scenarios_synthesized=1000 # type: ignore
)
# %%
for market in MARKET:
    for hydro_power_mask in ["discrete_turbine", "continuous_turbine_pump"]:
        plot_name = f"second_stage_deterministic_{hydro_power_mask}_{market}"


        deterministic_first_stage: DeterministicFirstStage = DeterministicFirstStage(
                    data_config=data_config,
                    smallflex_input_schema=smallflex_input_schema,
                    hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask]
                )
            
        row_titles = [
                    "<b>Market prices [EUR]<b>",
                    "<b>Reservoir level [%]<b>",
                    "<b>Hydro power [MW]<b>",
                ]
        if market == "primary_ancillary":
            row_titles.append("<b>Ancillary reserve [MW]<b>")
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

        for i, year in enumerate([2021, 2023]):
            file_name = f".cache/output/full_deterministic_{market}/{year}_results.duckdb"
            result_dict = duckdb_to_dict(file_name)
            data_config.year = year
            first_stage_result = result_dict[f"{hydro_power_mask}_first_stage"]
            second_stage_results = result_dict[f"{hydro_power_mask}_{battery}"]
            basin_volume: pl.DataFrame = extract_basin_volume(
                optimization_results=first_stage_result,
                water_basin=deterministic_first_stage.upstream_water_basin,
                data_config=data_config
            )

            deterministic_second_stage: DeterministicSecondStage = DeterministicSecondStage(
                    data_config=data_config,
                    smallflex_input_schema=smallflex_input_schema,
                    basin_volume_expectation=basin_volume,
                    hydro_power_mask=HYDROPOWER_MASK[hydro_power_mask]
                )
            timeseries = process_timeseries_data(
                    smallflex_input_schema=smallflex_input_schema,
                    data_config=data_config,
                    basin_index_mapping=pl_to_dict(deterministic_second_stage.water_basin["uuid", "B"]),
                )

            deterministic_second_stage.set_timeseries(timeseries=timeseries)

            fig = plot_second_stage_result(
                fig=fig,
                col=i+1,
                results=second_stage_results,
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
        # fig.show()
        fig.write_image(f"{plot_folder}/{plot_name}.svg", width=width, height=height, scale=1)
