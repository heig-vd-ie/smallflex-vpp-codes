import pyomo.environ as pyo

def first_stage_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.Ω, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.Ω, model.B, within=pyo.NonNegativeReals)
    
    model.flow = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # m^3
    model.hydro_power= pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh
    # model.hydro_ancillary_reserve= pyo.Var(model.T, within=pyo.NonNegativeReals)  # MWh

    model.end_basin_volume_overage = pyo.Var(model.B, model.Ω, within=pyo.NonNegativeReals) # m^3
    model.end_basin_volume_shortage = pyo.Var(model.B, model.Ω, within=pyo.NonNegativeReals) # m^3
    model.basin_volume_penalty = pyo.Var(within=pyo.Reals)

    return model