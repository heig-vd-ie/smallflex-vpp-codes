#%%
from pipelines.pipeline_manager.vpp_design_scheme import vpp_design_scheme


# %%
vpp_design_scheme(
    market_price_file_name=".cache/input/market_prices.csv",
    design_name="vpp_design_scheme"
)