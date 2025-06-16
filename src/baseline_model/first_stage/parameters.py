import pyomo.environ as pyo

def baseline_parameters(model):
    
    
    model.market_price = pyo.Param(model.T)
    model.nb_hours = pyo.Param(model.T)
    model.nb_sec = pyo.Param(default=3600) # s
    
    model.max_turbined_volume_factor = pyo.Param()
    
    model.start_basin_volume = pyo.Param(model.B, default=0) # m^3
    model.spilled_factor = pyo.Param(model.B, default=0)
    
    model.min_basin_volume = pyo.Param(model.BS, default=0) # m^3
    model.max_basin_volume = pyo.Param(model.BS, default=0) # m^3 
    model.discharge_volume = pyo.Param(model.T, model.B, default=0) # m^3
    
    model.water_factor = pyo.Param(model.B, model.H, default=0) # m^3
    model.volume_factor = pyo.Param() 
    
    model.max_flow= pyo.Param(model.HS, default=0) #m^3/s 
    model.alpha = pyo.Param(model.HS, default=0)  #MW/(m^3/s)

    
    return model