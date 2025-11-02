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


data_config: DataConfig = DataConfig(
    nb_scenarios=200,
    first_stage_max_powered_flow_ratio=0.75,
    total_scenarios_synthesized=1000 # type: ignore
)
# %%

for hydro_power_mask in ["discrete_turbine", "discrete_turbine_pump"]:
    plot_name = f"first_stage_deterministic_{hydro_power_mask}"

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
        file_name = f".cache/output/full_deterministic_da_energy/{year}_results.duckdb"
        result_dict = duckdb_to_dict(file_name)
        fig = plot_first_stage_result(
            fig=fig,
            results=result_dict[f"first_stage_{hydro_power_mask}"],
            water_basin=deterministic_first_stage.water_basin,
            col= i + 1)

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
            y=1.15, 
            xanchor="center",
            orientation="h",
            font=dict(size=20)
        )
    )
    # fig.show()
    fig.write_image(f"{plot_folder}/{plot_name}.svg", width=width, height=height, scale=1)
