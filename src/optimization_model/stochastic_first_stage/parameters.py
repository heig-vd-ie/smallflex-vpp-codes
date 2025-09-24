import pyomo.environ as pyo

def first_stage_parameters(model):
    
    model.market_price = pyo.Param(model.T, model.Ω)
    model.ancillary_market_price = pyo.Param(model.T, model.Ω)
    model.discharge_volume = pyo.Param(model.T, model.Ω, model.B, default=0) # m^3
    
    model.nb_hours = pyo.Param(model.T)
    model.nb_sec = pyo.Param(default=3600) # s
    
    model.max_powered_flow_ratio = pyo.Param()
    
    model.start_basin_volume = pyo.Param(model.B, default=0) # m^3
    model.spilled_factor = pyo.Param(model.B, default=0)
    
    model.min_basin_volume = pyo.Param(model.B, default=0) # m^3
    model.max_basin_volume = pyo.Param(model.B, default=0) # m^3 

    model.water_factor = pyo.Param(model.B, model.H, default=0) # m^3
    model.volume_factor = pyo.Param() 
    
    model.max_flow= pyo.Param(model.H, default=0) #m^3/s 
    model.total_positive_flex_power = pyo.Param(default=0)
    model.total_negative_flex_power = pyo.Param(default=0)
    model.alpha = pyo.Param(model.H, default=0)  #MW/(m^3/s)

    model.unpowered_factor_price_pos= pyo.Param(model.Ω, model.B) # CHF/m3
    model.unpowered_factor_price_neg = pyo.Param(model.Ω, model.B) # CHF/m3
    
    return model