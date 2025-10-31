
import polars as pl
from general_function import pl_to_dict
import plotly.graph_objs as go
from typing_extensions import Optional

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig
from pipelines.model_manager.deterministic_first_stage import DeterministicFirstStage

from pipelines.result_manager import extract_first_stage_optimization_results, extract_basin_volume

from timeseries_preparation.deterministic_data import process_timeseries_data

from data_display.baseline_plots import plot_first_stage_result


def first_stage_deterministic_pipeline(
    data_config: DataConfig,
    smallflex_input_schema: SmallflexInputSchema,
    hydro_power_mask: pl.Expr,
    plot_result: bool = True,
    fig: Optional[go.Figure] = None
) -> tuple[pl.DataFrame, pl.DataFrame, Optional[go.Figure]]:

    deterministic_first_stage: DeterministicFirstStage = DeterministicFirstStage(
        data_config=data_config,
        smallflex_input_schema=smallflex_input_schema,
        hydro_power_mask=hydro_power_mask
    )

    timeseries = process_timeseries_data(
            smallflex_input_schema=smallflex_input_schema,
            data_config=data_config,
            basin_index_mapping=pl_to_dict(deterministic_first_stage.water_basin["uuid", "B"])
        )

    deterministic_first_stage.set_timeseries(timeseries=timeseries)
    deterministic_first_stage.solve_model()

    optimization_results = extract_first_stage_optimization_results(
            model_instance=deterministic_first_stage.model_instance,
            timeseries=deterministic_first_stage.timeseries
        )


    basin_volume: pl.DataFrame = extract_basin_volume(
            optimization_results=optimization_results,
            water_basin=deterministic_first_stage.upstream_water_basin,
            data_config=data_config
        )
    if plot_result:
        fig = plot_first_stage_result(
            fig=fig,
            results=optimization_results, 
            water_basin=deterministic_first_stage.water_basin)

    return optimization_results, basin_volume, fig