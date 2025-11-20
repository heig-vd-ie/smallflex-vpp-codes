
import polars as pl
from typing_extensions import Optional
from general_function import pl_to_dict
import plotly.graph_objs as go

from smallflex_data_schema import SmallflexInputSchema
from utility.data_preprocessing import (print_pl)   
from pipelines.data_configs import DataConfig
from pipelines.model_manager.stochastic_second_stage import StochasticSecondStage

from pipelines.result_manager import extract_third_stage_optimization_results


from timeseries_preparation.second_stage_stochastic_data import process_second_stage_timeseries_stochastic_data

from data_display.baseline_plots import plot_second_stage_result


def second_stage_stochastic_pipeline(
    data_config: DataConfig,
    smallflex_input_schema: SmallflexInputSchema,
    basin_volume_expectation: pl.DataFrame,
    hydro_power_mask: pl.Expr,
    plot_result: bool = False,
) -> tuple[pl.DataFrame, float, Optional[go.Figure]]:


    stochastic_second_stage : StochasticSecondStage = StochasticSecondStage(
        data_config=data_config,
        smallflex_input_schema=smallflex_input_schema,
        basin_volume_expectation=basin_volume_expectation,
        hydro_power_mask=hydro_power_mask,
)

    timeseries_forecast, timeseries_measurement = process_second_stage_timeseries_stochastic_data(
        smallflex_input_schema=smallflex_input_schema,
        data_config=data_config)
    
    print_pl(timeseries_forecast)

    stochastic_second_stage.set_timeseries(timeseries_forecast=timeseries_forecast, timeseries_measurement=timeseries_measurement)

    stochastic_second_stage.solve_every_models()

    optimization_results, adjusted_income, imbalance_penalty = extract_third_stage_optimization_results(
        second_stage_model_instances=stochastic_second_stage.second_stage_model_instances,
        third_stage_model_instances=stochastic_second_stage.third_stage_model_instances,
        timeseries=stochastic_second_stage.timeseries_measurement,
    )
    if plot_result:
        fig = plot_second_stage_result(
            results=optimization_results,
            water_basin=stochastic_second_stage.water_basin,
            basin_volume_expectation=stochastic_second_stage.basin_volume_expectation,
        )
    
    else:
        fig = None

    return optimization_results, adjusted_income, fig