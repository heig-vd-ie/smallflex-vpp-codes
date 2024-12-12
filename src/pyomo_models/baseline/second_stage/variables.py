import pyomo.environ as pyo

def baseline_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    
    model.end_basin_volume = pyo.Var(model.B, within=pyo.NonNegativeReals)
    model.diff_volume_pos = pyo.Var(model.H, within=pyo.NonNegativeReals) # m^3
    model.diff_volume_neg = pyo.Var(model.H, within=pyo.NonNegativeReals) # m^3
    
    model.flow = pyo.Var(model.T, model.H, within=pyo.Reals) # m^3
    model.power= pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    model.flow_state = pyo.Var(model.T, model.HQS, within=pyo.Binary)
    
    # model.calculated_flow = pyo.Var(model.T, model.HQS, within=pyo.Reals) # m^3
    # model.calculated_power = pyo.Var(model.T, model.HQS, within=pyo.Reals) # m^3

    # model.flow_by_state = pyo.Var(model.T, model.HQS, within=pyo.Reals) # m^3
    # model.power_by_state = pyo.Var(model.T, model.HQS, within=pyo.Reals) # m^3
    
    model.basin_volume_by_state = pyo.Var(model.T, model.HQS, within=pyo.NonNegativeReals) # m^3

    return model