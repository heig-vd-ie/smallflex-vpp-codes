import pyomo.environ as pyo

def baseline_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    
    model.turbined_energy_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3
    model.turbined_volume_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3
    model.pumped_energy_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3
    model.pumped_volume_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3

    model.turbined_volume = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # m^3
    model.pumped_volume = pyo.Var(model.T, model.H, within=pyo.Reals)  # m^3
    model.turbined_energy = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # MWh
    model.pumped_energy = pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh

    return model