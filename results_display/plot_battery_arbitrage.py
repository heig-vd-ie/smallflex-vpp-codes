# %%
import os

os.chdir(os.getcwd().replace("/src", ""))

from results_display import *
# %%

file_names: dict[str, str] = json.load(open(settings.FILE_NAMES))  # type: ignore

plot_folder = ".cache/plots/static_plots/"

build_non_existing_dirs(plot_folder)

hydro_power_mask = "continuous_turbine_pump"
battery = "battery_2_MW_4MWh"


market = MARKET[-1]
year = 2021
    
date = pl.datetime(2021, 10, 1, time_zone="UTC")



file_name = f".cache/output/full_deterministic_{market}/{year}_results.duckdb"
result_dict = duckdb_to_dict(file_name)


filtered_results = result_dict[f"{hydro_power_mask}_{battery}"].filter(
    c("timestamp").is_between(date, date + timedelta(days=14))
)

fig = plot_battery_arbitrage(results=filtered_results)
plot_name = f"battery_arbitrage_{hydro_power_mask}_{battery}_{market}_{year}.svg"
fig.write_image(f"{plot_folder}/{plot_name}", width=fig.layout.width, height=fig.layout.height, scale=1) # type: ignore