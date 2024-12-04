import pyomo.environ as pyo

def baseline_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    
    model.flow_state = pyo.Var(model.T, model.FS, within=pyo.Binary)
    
    model.basin_volume_by_state = pyo.Var(model.T, model.BS, within=pyo.NonNegativeReals) # m^3
    
    model.flow = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # m^3
    model.power= pyo.Var(model.T, model.H, within=pyo.NonNegativeReals)  # MWh
    
    model.flow_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3
    # model.flow_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals) # m^3

    return model