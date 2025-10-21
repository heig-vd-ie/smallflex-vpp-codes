import pyomo.environ as pyo
from optimization_model.third_stage.constraints import *
from optimization_model.third_stage.sets import *
from optimization_model.third_stage.parameters import *
from optimization_model.third_stage.variables import *

def third_stage_common_constraints(model: pyo.AbstractModel) -> pyo.AbstractModel:
    ####################################################################################################################
    ### Basin volume evolution constraints #############################################################################  
    #################################################################################################################### 
    model.total_power_deviation_constraint = pyo.Constraint(model.T, rule=total_power_deviation_constraint)
    model.hydro_power_deviation_constraint = pyo.Constraint(model.T, model.H, rule=hydro_power_deviation_constraint)
    ####################################################################################################################
    ### Basin volume evolution constraints #############################################################################  
    #################################################################################################################### 
    model.basin_volume_evolution = pyo.Constraint(model.T, model.B, rule=basin_volume_evolution)
    model.basin_end_volume_constraint = pyo.Constraint(model.B, rule=basin_end_volume_constraint)
    ####################################################################################################################
    ### Basin volume boundary constraints used to determine the state of each basin ####################################
    ####################################################################################################################
    model.basin_max_state_constraint = pyo.Constraint(model.T, model.BS, rule=basin_max_state_constraint)
    model.basin_min_state_constraint = pyo.Constraint(model.T, model.BS, rule=basin_min_state_constraint)
    model.basin_state_constraint = pyo.Constraint(model.T, model.B, rule=basin_state_constraint)
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    model.max_active_flow_by_state_constraint = pyo.Constraint(model.T, model.DHS, rule=max_active_flow_by_state_constraint)
    model.max_inactive_flow_by_state_constraint = pyo.Constraint(model.T, model.DHBS, rule=max_inactive_flow_by_state_constraint)
    model.max_flow_by_state_constraint = pyo.Constraint(model.T, model.HBS, rule=max_flow_by_state_constraint)
    model.flow_constraint = pyo.Constraint(model.T, model.H, rule=flow_constraint)
    model.hydro_power_constraint = pyo.Constraint(model.T, model.H, rule=hydro_power_constraint)

    return model

def third_stage_constraints_with_battery(model: pyo.AbstractModel) -> pyo.AbstractModel:
    model.objective = pyo.Objective(rule=third_stage_objective_with_battery, sense=pyo.minimize)
    model = third_stage_common_constraints(model)
    #####################################################################################################################
    ### Battery constraints #############################################################################################
    #####################################################################################################################
    model.battery_soc_evolution_constraint = pyo.Constraint(model.T, rule=battery_soc_evolution_constraint)
    model.end_battery_soc_constraint = pyo.Constraint(rule=end_battery_soc_constraint)
    model.battery_max_charging_power_constraint = pyo.Constraint(model.T, rule=battery_max_charging_power_constraint)
    model.battery_max_discharging_power_constraint = pyo.Constraint(model.T, rule=battery_max_discharging_power_constraint)
    model.battery_in_charge_constraint = pyo.Constraint(model.T, rule=battery_in_charge_constraint)
    model.battery_in_discharge_constraint = pyo.Constraint(model.T, rule=battery_in_discharge_constraint)
    return model

def third_stage_constraints_without_battery(model: pyo.AbstractModel) -> pyo.AbstractModel:
    model.objective = pyo.Objective(rule=third_stage_objective_without_battery, sense=pyo.minimize)
    model = third_stage_common_constraints(model)
    return model

def third_stage_model_with_battery() -> pyo.AbstractModel:
    model: pyo.AbstractModel = pyo.AbstractModel() # type: ignore
    model = third_stage_sets(model)
    model = third_stage_parameters(model)
    model = third_stage_variables(model)
    model = third_stage_constraints_with_battery(model)
    return model

def third_stage_model_without_battery() -> pyo.AbstractModel:

    model: pyo.AbstractModel = pyo.AbstractModel() # type: ignore
    model = third_stage_sets(model)
    model = third_stage_parameters(model)
    model = third_stage_variables(model)
    model = third_stage_constraints_without_battery(model)
    return model
