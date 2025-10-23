import pyomo.environ as pyo


def third_stage_parameters(model):

    model.nb_hours = pyo.Param(default=1)
    model.nb_sec = pyo.Param(default=3600) # s

    model.basin_volume_range = pyo.Param(model.B) # m^3

    model.start_basin_volume = pyo.Param(model.B, default=0) # m^3
    

    model.spilled_factor = pyo.Param(model.B, default=1) # m^3
    model.battery_penalty_factor = pyo.Param(default=0.01) 
    model.hydro_power_penalty_factor = pyo.Param(model.H, default=0.05)
    
    model.min_basin_volume = pyo.Param(model.BS, default=0) # m^3
    model.max_basin_volume = pyo.Param(model.BS, default=0) # m^3

    
    model.water_factor = pyo.Param(model.B, model.H, default=0) # m^3

    model.max_flow = pyo.Param(model.HS, default=0) #m^3/s
    model.alpha = pyo.Param(model.HS, default=0) #MW/(Mm^3/s)
    
    model.big_m = pyo.Param(default=1e6)  # Big M value for constraints
    model.rated_alpha = pyo.Param(model.UP_B) # MW/(m^3/s)
    
    model.pv_power_measured = pyo.Param(model.T, default=0) # MW
    model.wind_power_measured = pyo.Param(model.T, default=0) # MW
    model.discharge_volume_measured = pyo.Param(model.T, model.B, default=0) # m^3
    model.hydro_power_forecast = pyo.Param(model.T, model.H, default=0) # MW
    model.total_power_forecast = pyo.Param(model.T, default=0) # MW
    
    model.battery_capacity = pyo.Param() # MWh
    model.battery_rated_power = pyo.Param() # MW
    model.battery_efficiency = pyo.Param() # -
    model.start_battery_soc = pyo.Param() # %
    
    return model