import pyomo.environ as pyo

def baseline_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    model.basin_volume_by_state = pyo.Var(model.T, model.BS, within=pyo.Reals) # m^3

    model.turbined_volume = pyo.Var(model.T, model.H, within=pyo.Reals) # m^3
    model.pumped_volume = pyo.Var(model.T, model.H, within=pyo.Reals)  # m^3
    model.turbined_energy = pyo.Var(model.T, model.H, within=pyo.Reals) # MWh
    model.pumped_energy = pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh

    return model