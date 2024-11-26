import pyomo.environ as pyo

def baseline_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    
    model.basin_volume_by_state = pyo.Var(model.T, model.BS, within=pyo.NonNegativeReals) # m^3
    
    model.turbined_power_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3
    model.turbined_flow_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3
    model.pumped_power_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3
    model.pumped_flow_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3
    model.turbined_alpha_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # MWh
    model.pumped_alpha_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals)  # MWh

    model.turbined_flow = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # m^3
    model.pumped_flow = pyo.Var(model.T, model.H, within=pyo.Reals)  # m^3
    model.turbined_power = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # MWh
    model.pumped_power= pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh
    
    model.turbined_alpha = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # MWh
    model.pumped_alpha = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals)  # MWh

    return model