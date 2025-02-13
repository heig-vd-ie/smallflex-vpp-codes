
import json
import polars as pl
from polars import col as c

from data_federation.input_model import SmallflexInputSchema

from general_function import  generate_log

from data_federation.input_model import SmallflexInputSchema
from data_display.input_data_plots import plot_forecast


from config import settings

if __name__ == "__main__":
    
    log = generate_log(name="file_")
    input_file_names: dict[str, str] = json.load(open(settings.INPUT_FILE_NAMES)) # type: ignore
    output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES)) # type: ignore
    
    small_flex_input_schema = SmallflexInputSchema()
    small_flex_input_schema = small_flex_input_schema.duckdb_to_schema(output_file_names["duckdb_input"])
    
    plot_folder = output_file_names["input_data_plot"]

    weather_forecast = small_flex_input_schema.weather_forecast.filter(
        (c("sub_basin")== "Griessee") & (c("avg_height") == 2704)
    )

    weather_measurement = small_flex_input_schema.weather_measurement.filter(
        (c("sub_basin")== "Griessee") & (c("avg_height") == 2704)
    )

    discharge_flow_forecast  = small_flex_input_schema.discharge_flow_forecast.filter(c("location") == "Griessee")
    discharge_flow_measurement = small_flex_input_schema.discharge_flow_measurement.filter(c("location") == "Griessee")


    plot_forecast(
        measurement_df=discharge_flow_measurement, forecast_df=discharge_flow_forecast, 
        plot_name="discharge_flow_forecast", 
        plot_folder=plot_folder, title='Discharge flow m^3/s',
        nb_days=5, min_boundaries=0.15
        )


    plot_forecast(
        measurement_df=weather_measurement.rename({"irradiation": "value"}), 
        forecast_df=weather_forecast.rename({"irradiation": "forecast"}), 
        plot_name="irradiation_forecast", 
        plot_folder=plot_folder, title='Irradiation W/m2',
        nb_days=4, min_boundaries=0.15
        )

    plot_forecast(
        measurement_df=weather_measurement.rename({"wind": "value"}), 
        forecast_df=weather_forecast.rename({"wind": "forecast"}), 
        plot_name="wind_forecast", 
        plot_folder=plot_folder, title='Wind speed m/s',
        nb_days=4, min_boundaries=0.15
        )