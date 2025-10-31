import pyomo.environ as pyo

def first_stage_parameters(model):
    
    
    model.market_price = pyo.Param(model.T)
    # model.ancillary_market_price = pyo.Param(model.T)
    model.nb_hours = pyo.Param(model.T)
    model.nb_sec = pyo.Param(default=3600) # s
    
    model.max_powered_flow_ratio = pyo.Param()
    
    model.start_basin_volume = pyo.Param(model.B, default=0) # m^3
    model.spilled_factor = pyo.Param(model.B, default=0)
    
    model.min_basin_volume = pyo.Param(model.BS, default=0) # m^3
    model.max_basin_volume = pyo.Param(model.BS, default=0) # m^3 
    model.discharge_volume = pyo.Param(model.T, model.B, default=0) # m^3
    model.basin_volume_range = pyo.Param(model.B) # m^3
    
    model.water_factor = pyo.Param(model.B, model.H, default=0) # m^3
    
    model.max_flow= pyo.Param(model.HS, default=0) #m^3/s 
    model.total_positive_flex_power = pyo.Param(model.S, default=0)
    model.total_negative_flex_power = pyo.Param(model.S, default=0)
    model.alpha = pyo.Param(model.HS, default=0)  #MW/(m^3/s)

    
    return model