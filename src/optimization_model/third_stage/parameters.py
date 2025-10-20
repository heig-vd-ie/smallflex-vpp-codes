import pyomo.environ as pyo

from optimization_model.deterministic_first_stage import model

def third_stage_parameters(model):
    
    model.market_price = pyo.Param(model.T)
    model.ancillary_market_price = pyo.Param(model.F)
    model.nb_hours = pyo.Param(default=1)
    model.nb_sec = pyo.Param(default=3600) # s
    model.nb_timestamp_per_ancillary = pyo.Param() # -
    
    model.rated_alpha = pyo.Param(model.UP_B) # MW/(m^3/s)
    model.overage_market_price = pyo.Param()
    model.shortage_market_price = pyo.Param()
    model.bound_penalty_factor = pyo.Param(default=1) # -
    model.basin_volume_range = pyo.Param(model.B) # m^3
    
    model.expected_end_basin_volume = pyo.Param(model.B) # MWh
    model.expected_upper_end_basin_volume = pyo.Param(model.B) # MWh
    model.expected_lower_end_basin_volume = pyo.Param(model.B) # MWh
    
    model.start_basin_volume = pyo.Param(model.B, default=0) # m^3
    

    model.spilled_factor = pyo.Param(model.B, default=1) # m^3
    
    model.min_basin_volume = pyo.Param(model.BS, default=0) # m^3
    model.max_basin_volume = pyo.Param(model.BS, default=0) # m^3 
    model.discharge_volume = pyo.Param(model.T, model.B, default=0) # m^3
    
    model.water_factor = pyo.Param(model.B, model.H, default=0) # m^3

    model.max_flow = pyo.Param(model.HS, default=0) #m^3/s 
    model.alpha = pyo.Param(model.HS, default=0) #MW/(Mm^3/s)
    
    model.big_m = pyo.Param(default=1e6)  # Big M value for constraints
    model.total_positive_flex_power = pyo.Param(default=0)
    model.total_negative_flex_power = pyo.Param(default=0)
    
    model.pv_power = pyo.Param(model.T, default=0) # MW
    model.wind_power = pyo.Param(model.T, default=0) # MW
    
    model.battery_capacity = pyo.Param() # MWh
    model.battery_rated_power = pyo.Param() # MW
    model.battery_efficiency = pyo.Param() # -
    model.start_battery_soc = pyo.Param() # %
    
    return model