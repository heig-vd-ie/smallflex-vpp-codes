import pyomo.environ as pyo

def second_stage_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    
    model.end_basin_volume = pyo.Var(model.B, within=pyo.NonNegativeReals)
    model.diff_volume_pos = pyo.Var(model.H, within=pyo.NonNegativeReals) # m^3
    model.diff_volume_neg = pyo.Var(model.H, within=pyo.NonNegativeReals) # m^3
    
    model.flow = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # m^3
    model.hydro_power = pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh
    model.ancillary_power= pyo.Var(model.F, model.CH, within=pyo.NonNegativeReals)  # MWh
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    
    model.discrete_hydro_on = pyo.Var(model.T, model.DH, within=pyo.Binary)
    
    model.flow_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals)

    return model