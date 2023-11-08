"""
Generate forecast
"""
import warnings
import pmdarima as pm
from pmdarima.pipeline import Pipeline


def day_ahead_forecast_arima_with_lag(data_df, lag=4, non_negative=True):
    """
    Forecast day-ahead with lag
    """
    warnings.filterwarnings("ignore")
    pipeline = Pipeline([('arima', pm.ARIMA(seasonal=True, order=(0, 1, 0), seasonal_order=(0, 1, 0, 24), maxiter=5, suppress_warnings=True, trace=True))])
    pipeline.fit(data_df)
    predict_in_sample = []
    for i in range(364):
        in_sample_temp = pipeline.predict_in_sample(start=24*(i+1)-lag-1, end=24*(i+1)+23, dynamic=True)
        if non_negative:
            in_sample_temp= in_sample_temp.apply(lambda x: x if x>0 else 0)
        predict_in_sample.extend(list(in_sample_temp)[lag:-1])
    return predict_in_sample[-23:] + predict_in_sample[:-23]
