import pyomo.environ as pyo

def second_stage_variables(model):
    
    model.basin_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    model.spilled_volume = pyo.Var(model.T, model.B, within=pyo.NonNegativeReals)
    # model.basin_volume_penalty = pyo.Var(within=pyo.Reals)  # MWh
    # model.market_income = pyo.Var(within=pyo.Reals)  # MWh
    # model.ancillary_market_income = pyo.Var(within=pyo.Reals)  # MWh

    model.end_basin_volume = pyo.Var(model.B, within=pyo.NonNegativeReals)
    model.end_basin_volume_mean_overage = pyo.Var(model.UP_B, within=pyo.NonNegativeReals) # m^3
    model.end_basin_volume_mean_shortage = pyo.Var(model.UP_B, within=pyo.NonNegativeReals) # m^3
    model.end_basin_volume_upper_overage = pyo.Var(model.UP_B, within=pyo.NonNegativeReals) # m^3
    model.end_basin_volume_upper_shortage = pyo.Var(model.UP_B, within=pyo.NonNegativeReals) # m^3
    model.end_basin_volume_lower_overage = pyo.Var(model.UP_B, within=pyo.NonNegativeReals) # m^3
    model.end_basin_volume_lower_shortage = pyo.Var(model.UP_B, within=pyo.NonNegativeReals) # m^3


    model.flow = pyo.Var(model.T, model.H, within=pyo.NonNegativeReals) # m^3
    model.hydro_power = pyo.Var(model.T, model.H, within=pyo.Reals)  # MWh

    model.battery_charging_power = pyo.Var(model.T, within=pyo.NonNegativeReals, initialize=0)  # MW
    model.battery_discharging_power = pyo.Var(model.T, within=pyo.NonNegativeReals, initialize=0)  # MW
    model.battery_soc = pyo.Var(model.T, within=pyo.NonNegativeReals, bounds=(0, 1), initialize=0)  # MWh
    model.end_battery_soc_shortage = pyo.Var(within=pyo.NonNegativeReals, bounds=(0, 1), initialize=0)  # MWh
    model.end_battery_soc_overage = pyo.Var(within=pyo.NonNegativeReals, bounds=(0, 1), initialize=0)  # MWh
    model.battery_in_charge = pyo.Var(model.T, within=pyo.Binary, initialize=0)  # MW
    
    model.hydro_ancillary_reserve= pyo.Var(model.F, within=pyo.NonNegativeReals)  # MWh
    model.battery_ancillary_reserve = pyo.Var(model.F, within=pyo.NonNegativeReals)  # MW
    
    model.basin_state = pyo.Var(model.T, model.BS, within=pyo.Binary)
    
    model.discrete_hydro_on = pyo.Var(model.T, model.DH, within=pyo.Binary)
    
    model.flow_by_state = pyo.Var(model.T, model.HS, within=pyo.NonNegativeReals)
    
    

    return model