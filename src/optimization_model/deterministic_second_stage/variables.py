import pyomo.environ as pyo

def third_stage_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.end_basin_volume = pyo.Var(model.B, within=pyo.NonNegativeReals)

    model.flow = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # m^3
    model.hydro_power = pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh

    model.battery_charging_power = pyo.Var(model.T, within=pyo.NonNegativeReals, initialize=0)  # MW
    model.battery_discharging_power = pyo.Var(model.T, within=pyo.NonNegativeReals, initialize=0)  # MW
    model.battery_soc = pyo.Var(model.T, within=pyo.NonNegativeReals, bounds=(0, 1), initialize=0)  # MWh
    model.end_battery_soc_shortage = pyo.Var(within=pyo.NonNegativeReals, bounds=(0, 1), initialize=0)  # MWh
    model.end_battery_soc_overage = pyo.Var(within=pyo.NonNegativeReals, bounds=(0, 1), initialize=0)  # MWh
    model.battery_in_charge = pyo.Var(model.T, within=pyo.Binary, initialize=0)  # MW
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    model.discrete_hydro_on = pyo.Var(model.T, model.DH, within=pyo.Binary)
    model.flow_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals)
    
    model.hydro_power_deviation_positive = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals)
    model.hydro_power_deviation_negative = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals)
    model.total_power_deviation_positive = pyo.Var(model.T, within=pyo.NonNegativeReals)
    model.total_power_deviation_negative = pyo.Var(model.T, within=pyo.NonNegativeReals)

    return model