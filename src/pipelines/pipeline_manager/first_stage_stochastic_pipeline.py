
from matplotlib.figure import Figure
import polars as pl
from general_function import pl_to_dict
import plotly.graph_objs as go
from typing_extensions import Optional

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig
from pipelines.model_manager.stochastic_first_stage import StochasticFirstStage

from pipelines.result_manager import extract_first_stage_optimization_results, extract_basin_volume_expectation

from timeseries_preparation.first_stage_stochastic_data import process_first_stage_timeseries_data

from data_display.baseline_plots import plot_scenario_results, plot_second_stage_result


def first_stage_stochastic_pipeline(
    data_config: DataConfig,
    smallflex_input_schema: SmallflexInputSchema,
    hydro_power_mask: pl.Expr,
    plot_result: bool = True,
) -> tuple[pl.DataFrame, pl.DataFrame, Optional[go.Figure]]:

    stochastic_first_stage: StochasticFirstStage = StochasticFirstStage(
        data_config=data_config,
        smallflex_input_schema=smallflex_input_schema,
        hydro_power_mask=hydro_power_mask,
    )

    timeseries = process_first_stage_timeseries_data(
        smallflex_input_schema=smallflex_input_schema,
        data_config=data_config,
        scenario_list=stochastic_first_stage.scenario_list,
        water_basin_mapping=pl_to_dict(stochastic_first_stage.water_basin["uuid", "B"]),
    )
    stochastic_first_stage.set_timeseries(timeseries=timeseries)

    stochastic_first_stage.solve_model()
            
    optimization_results = extract_first_stage_optimization_results(
        model_instance=stochastic_first_stage.model_instance,
        timeseries=stochastic_first_stage.timeseries
    )
    
    basin_volume_expectation = extract_basin_volume_expectation(
        model_instance=stochastic_first_stage.model_instance,
        optimization_results=optimization_results,
        water_basin=stochastic_first_stage.upstream_water_basin,
        data_config=data_config
    )
    if plot_result:
        fig = plot_scenario_results(
            optimization_results=optimization_results, 
            water_basin=stochastic_first_stage.upstream_water_basin,
            data_config=data_config
        )
    else: 
        fig = None
            
    
    return optimization_results, basin_volume_expectation, fig