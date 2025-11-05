# %%

import os

os.chdir(os.getcwd().replace("/src", ""))

from results_display import *
# %%

file_name = ".cache/output/imbalance/results.duckdb"

results: pl.DataFrame = duckdb_to_dict(file_name)["with_hydro_battery_2_MW_4MWh"]
date = pl.datetime(2025, 10, 10, time_zone="UTC")

results = results.filter(c("timestamp").is_between(date, date + timedelta(days=14)))
results = results.with_columns(
    (c("hydro_power_0") - c("hydro_power_forecast_0")).alias("hydro_power_diff"),
    (c("wind_power_measured") - c("wind_power_forecast")).alias("wind_power_diff"),
    (c("pv_power_measured") - c("pv_power_forecast")).alias("pv_power_diff"),
).rename({"hydro_power_forecast_0": "hydro_power_forecast"})

showlegend = True
nb_graphs = 7


col = 1
showlegend=True

plot_imbalance_management(results, showlegend=True)

plot_folder = ".cache/plots/static_plots/"
plot_name = "imbalance_management_baseline.png"

fig.write_image(f"{plot_folder}/{plot_name}", width=fig.layout.width, height=fig.layout.height, scale=1) # type: ignore