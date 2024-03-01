"""
Generate forecast
"""
import warnings
import pmdarima as pm
import polars as pl
from pmdarima.pipeline import Pipeline


def day_ahead_forecast_arima_with_lag(data_df, lag=4, non_negative=False):
    """
    Forecast day-ahead with lag
    """
    warnings.filterwarnings("ignore")
    pipeline = Pipeline([('arima', pm.ARIMA(seasonal=True, order=(0, 1, 0), seasonal_order=(0, 1, 0, 24), maxiter=5, suppress_warnings=True, trace=True))])
    pipeline.fit(data_df)
    predict_in_sample = []
    days_to_predict = int(data_df.shape[0] / 24)
    for i in range(days_to_predict):
        in_sample_temp = pipeline.predict_in_sample(start=24*(i+1)-lag-1, end=24*(i+1)+23, dynamic=True)
        if non_negative:
            in_sample_temp = in_sample_temp.apply(lambda x: x if x>0 else 0)
        predict_in_sample.extend(list(in_sample_temp)[lag:-1])
    return predict_in_sample[-23:] + predict_in_sample[:-23]


def generate_dataframe_forecast(data_df, d_time, non_negative=False):
    """
    Generate dataframe of forecasted data
    """
    d_time_int = int(d_time.split("h")[0]) if "h" in d_time else int(d_time.split("m")[0]) / 60 if "m" in d_time else RuntimeError
    arbitrary_year = pl.col("timestamp").dt.strftime("2035-%m-%d %H:%M:%S").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
    result_df = pl.DataFrame(schema={"timestamp": pl.Datetime(time_unit="ns"), "value": pl.Float64, "scenario": pl.Utf8})
    for scen in data_df["scenario"].unique().to_list():
        df_temp = data_df.filter(pl.col("scenario")==scen).with_columns(arbitrary_year).sort("timestamp").select(["timestamp", "value"]).to_pandas().set_index("timestamp", drop=True)
        df_temp.loc["2035-12-30 23:50:00", "value"] = 0
        df_temp = df_temp.groupby("timestamp").mean().asfreq('1h', method = 'ffill')
        result_temp = day_ahead_forecast_arima_with_lag(df_temp, non_negative=non_negative)
        data_forecast_temp = pl.from_dict({"timestamp": df_temp.index, "value": result_temp}).with_columns([pl.lit(scen).alias("scenario"), pl.col("timestamp").cast(pl.Datetime(time_unit="ns"))])
        result_df = pl.concat([result_df, data_forecast_temp])
    result_df = result_df.with_columns([arbitrary_year.dt.week().alias("week"), pl.col("timestamp").dt.year().alias("year")])
    # define time step
    result_df = result_df.group_by(["week", "year", "scenario"], maintain_order=True).agg([pl.col("timestamp"), pl.col("value")]).with_columns(pl.col("value").map_elements(lambda x: range(len(x))).alias("time_step"))
    result_df = result_df.explode(["timestamp", "time_step", "value"]).select(["timestamp", "year", "week", "time_step", "value", "scenario"])
    # remove first and end weeks to have consistent years
    result_df = result_df.filter((pl.col("week") < 53) & (pl.col("time_step") < int(168 / d_time_int))).with_columns(pl.lit(d_time_int).alias("delta_t").cast(pl.Float64))
    return result_df
