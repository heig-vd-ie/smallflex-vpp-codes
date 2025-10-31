
import polars as pl
from typing_extensions import Optional
from general_function import pl_to_dict
import plotly.graph_objs as go

from smallflex_data_schema import SmallflexInputSchema
from pipelines.data_configs import DataConfig
from pipelines.model_manager.deterministic_second_stage import DeterministicSecondStage

from pipelines.result_manager import extract_second_stage_optimization_results


from timeseries_preparation.deterministic_data import process_timeseries_data

from data_display.baseline_plots import plot_second_stage_result


def second_stage_deterministic_pipeline(
    data_config: DataConfig,
    smallflex_input_schema: SmallflexInputSchema,
    basin_volume_expectation: pl.DataFrame,
    hydro_power_mask: pl.Expr,
    pv_power_mask: Optional[pl.Expr] = None,
    wind_power_mask: Optional[pl.Expr] = None,
    display_battery: bool = False,
    plot_result: bool = True,
) -> tuple[pl.DataFrame, float, Optional[go.Figure]]:

    deterministic_second_stage: DeterministicSecondStage = DeterministicSecondStage(
        data_config=data_config,
        smallflex_input_schema=smallflex_input_schema,
        basin_volume_expectation=basin_volume_expectation,
        hydro_power_mask=hydro_power_mask
    )

    timeseries = process_timeseries_data(
        smallflex_input_schema=smallflex_input_schema,
        data_config=data_config,
        basin_index_mapping=pl_to_dict(deterministic_second_stage.water_basin["uuid", "B"]),
        pv_power_mask=pv_power_mask,
        wind_power_mask=wind_power_mask
    )

    deterministic_second_stage.set_timeseries(timeseries=timeseries)

    deterministic_second_stage.solve_every_models()

    optimization_results, adjusted_income = (
            extract_second_stage_optimization_results(
                model_instances=deterministic_second_stage.model_instances,
                timeseries=deterministic_second_stage.timeseries,
                data_config=data_config
            )
        )
    if plot_result:
        fig = plot_second_stage_result(
            results=optimization_results,
            water_basin=deterministic_second_stage.water_basin,
            market_price_quantiles=deterministic_second_stage.market_price_quantiles,
            basin_volume_expectation=deterministic_second_stage.basin_volume_expectation,
            display_battery=display_battery
        )
        
    
    else:
        fig = None

    return optimization_results, adjusted_income, fig