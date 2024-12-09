import pyomo.environ as pyo

def baseline_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    
    model.end_basin_volume = pyo.Var(model.B, within=pyo.NonNegativeReals)
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    model.flow_state = pyo.Var(model.T, model.HF, within=pyo.Binary)
    
    model.basin_volume_by_state = pyo.Var(model.T, model.BS, within=pyo.NonNegativeReals) # m^3
    
    model.diff_volume_pos = pyo.Var(model.H, within=pyo.NonNegativeReals) # m^3
    model.diff_volume_neg = pyo.Var(model.H, within=pyo.NonNegativeReals) # m^3
    
    model.flow = pyo.Var(model.T, model.H, within=pyo.Reals) # m^3
    model.power= pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh
    
    model.calculated_flow = pyo.Var(model.T, model.HF, within=pyo.Reals) # m^3
    model.calculated_power = pyo.Var(model.T, model.HF, within=pyo.Reals) # m^3
    
    model.flow_by_state = pyo.Var(model.T, model.HF, within=pyo.Reals) # m^3
    model.power_by_state = pyo.Var(model.T, model.HF, within=pyo.Reals) # m^3

    return model