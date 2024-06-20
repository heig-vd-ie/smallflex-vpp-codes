"""
Generate forecast
"""
import warnings
import pmdarima as pm
import polars as pl
import numpy as np
from pmdarima.pipeline import Pipeline
from datetime import timedelta


def predict_days(pipeline: Pipeline, day: int, step_per_day: int, lag: int) -> list:
    result = pipeline.predict_in_sample(
        start=step_per_day*(day+1)-lag-1, end=step_per_day*(day+2)-1, dynamic=True
    )
    return list(result[lag:-1])

def predict_years(
    input_values: pl.Series, step_per_day: int, days_to_predict: int, non_negative:bool, lag: int
) -> pl.Series :
    # Create ARIMA pipeline
    pipeline: Pipeline = Pipeline([(
        'arima', 
        pm.ARIMA(seasonal=True, order=(0, 1, 0), seasonal_order=(0, 1, 0, 24), maxiter=5, 
        suppress_warnings=True, trace=True))
    ])
    # fit values
    pipeline.fit(input_values.to_numpy())
    # predict each days 
    forecast_results: list = list(map(
        lambda day: predict_days(pipeline=pipeline, day=day, step_per_day=step_per_day, lag=lag),
        range(days_to_predict)
    ))
    # Replace last day to first day
    forecast_results_df: pl.Series = pl.Series([forecast_results[-1]] + forecast_results[: -1]).explode()
    # forecast_results_df: pl.Series = pl.Series(forecast_results[1:] + [forecast_results[0]]).explode()
    if non_negative:
        forecast_results_df = forecast_results_df.map_elements(lambda x: 0 if x < 0 else x)
    return forecast_results_df

def day_ahead_forecast_arima_with_lag(
    raw_data_df: pl.DataFrame, d_time:timedelta, lag=4, non_negative=False
)-> pl.DataFrame:
    """
    Forecast day-ahead with lag
    """
    warnings.filterwarnings("ignore")
    scenario_list: list = raw_data_df["scenario"].unique().to_list()
    step_per_day: int = int(timedelta(days=1)/d_time)
    id_vars: list = ["week", "time_step"]
    raw_data_df = raw_data_df.pivot(values="value", index=id_vars, columns="scenario")\
        .sort(id_vars)

    days_to_predict: int = int(raw_data_df.height/step_per_day)

    predicted_data_df: pl.DataFrame = raw_data_df.with_columns([
        predict_years(
            input_values=raw_data_df[scenario], step_per_day=step_per_day, days_to_predict=days_to_predict,
            non_negative=non_negative, lag=lag
        ).alias(scenario)
        for scenario in scenario_list
    ]).melt(id_vars=id_vars, value_vars=scenario_list, variable_name="scenario")

    return predicted_data_df
