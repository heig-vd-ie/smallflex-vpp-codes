import pyomo.environ as pyo

def second_stage_parameters(model):
    
    model.market_price = pyo.Param(model.T)
    model.ancillary_market_price = pyo.Param(model.F)
    model.nb_hours = pyo.Param(default=1)
    model.nb_sec = pyo.Param(default=3600) # s
    model.nb_timestamp_per_ancillary = pyo.Param() # -
    model.volume_factor = pyo.Param()
    # model.big_m = pyo.Param()  
    model.powered_volume_enabled = pyo.Param(within=pyo.Binary, default=True)

    model.unpowered_factor_price_pos = pyo.Param(model.H) # CHF/(m^3/s)
    model.unpowered_factor_price_neg = pyo.Param(model.H) # CHF/(m^3/s)
    model.powered_volume_quota = pyo.Param(model.H, default=0) # MW/(m^3/s)
    model.overage_volume_buffer = pyo.Param(model.H, default=0) # MW/(m^3/s)
    model.shortage_volume_buffer = pyo.Param(model.H, default=0) # MW/(m^3/s)
    
    model.neg_unpowered_price = pyo.Param(default=0) # CHF/(m^3/s)
    model.pos_unpowered_price = pyo.Param(default=0) # CHF/(m^3/s)  
    
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