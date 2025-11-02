#%%

import os

import polars as pl
from polars import col as c


from general_function import duckdb_to_dict
from polars_function import modify_string_col


os.chdir(os.getcwd().replace("/src", ""))

# %%

year = 2022

results_dict = duckdb_to_dict(f".cache/output/battery_size_design/2022_results.duckdb")

income_result = []
for key, results in results_dict.items():
    income_result.append(
        (key, results["da_income", "ancillary_income"].to_numpy().sum() / 1e3)
    )
    
income_result_df = pl.DataFrame(income_result, schema=["design", "total_income"], orient="row").with_columns(
    c("design").str.split("_battery").list.to_struct(fields=["hydropower", "battery"])
).unnest("design").with_columns(
    c("hydropower").pipe(modify_string_col, format_str={"_no": ""}),
    c("battery").pipe(modify_string_col, format_str={r"^_": "", "^$": "0_battery"}),
).pivot(values="total_income", on="hydropower", index="battery").sort("battery").select(
    "battery", "discrete_turbine", "discrete_turbine_pump", "continuous_turbine_pump"
)
min_income = income_result_df.drop("battery").to_numpy().min()
relative_diff = (
    income_result_df.with_columns(
        ((c(col) - min_income) / min_income * 100).alias(col)
        for col in income_result_df.drop("battery").columns
    )
)

with pl.Config(
    tbl_cell_numeric_alignment="RIGHT",
    thousands_separator="'",
    float_precision=1,
    set_tbl_hide_dataframe_shape=True,
    set_tbl_hide_column_data_types=True
):
    print(f"Total income for year {year} [kCHF]:\n{income_result_df}")
    print(f"Income relative difference for year {year} [%]:\n{relative_diff}")
