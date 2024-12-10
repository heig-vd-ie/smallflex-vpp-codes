import pyomo.environ as pyo

def baseline_parameters(model):
    
    model.market_price = pyo.Param(model.T)
    model.nb_hours = pyo.Param(default=1)
    model.volume_factor = pyo.Param()
    model.nb_sec = pyo.Param(default=3600) # s
    model.big_m = pyo.Param()  
    model.powered_volume_enabled = pyo.Param(within=pyo.Binary, default=True)

    model.neg_unpowered_price = pyo.Param() # MW/(m^3/s)
    model.pos_unpowered_price = pyo.Param() # MW/(m^3/s)
    model.min_alpha = pyo.Param(model.H, default=0) # MW/(m^3/s)
    model.max_alpha = pyo.Param(model.H, default=0) # MW/(m^3/s)
    model.powered_volume = pyo.Param(model.H, default=0) # MW/(m^3/s)
    model.remaining_volume = pyo.Param(model.H, default=0) # MW/(m^3/s)
    model.buffer = pyo.Param(default=0.2) # MW/(m^3/s)
    
    model.start_basin_volume = pyo.Param(model.B, default=0) # m^3
    model.spilled_factor = pyo.Param(model.B, default=0.01) # m^3
    
    model.min_basin_volume = pyo.Param(model.BS, default=0) # m^3
    model.max_basin_volume = pyo.Param(model.BS, default=0) # m^3 
    model.discharge_volume = pyo.Param(model.T, model.B, default=0) # m^3
    
    model.water_factor = pyo.Param(model.B, model.H, default=0) # m^3

    model.min_flow = pyo.Param(model.HSF, default=0) #m^3/s 
    model.d_flow = pyo.Param(model.HSF, default=0) #m^3/s
    model.min_power = pyo.Param(model.HSF, default=0)  #MW/(m^3/s)
    model.d_power = pyo.Param(model.HSF, default=0)  #MW/(m^3/s)
    
    return model