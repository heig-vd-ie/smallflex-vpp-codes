from pyexpat import model
import pyomo.environ as pyo
from optimization_model.second_stage.constraints import *
from optimization_model.second_stage.sets import *
from optimization_model.second_stage.parameters import *
from optimization_model.second_stage.variables import *

def second_stage_baseline_constraints(model: pyo.AbstractModel) -> pyo.AbstractModel:
    model.objective = pyo.Objective(rule=second_stage_baseline_objective, sense=pyo.maximize)

    ####################################################################################################################
    ### Basin volume evolution constraints #############################################################################  
    #################################################################################################################### 
    model.basin_volume_evolution = pyo.Constraint(model.T, model.B, rule=basin_volume_evolution)
    model.basin_end_volume_constraint = pyo.Constraint(model.B, rule=basin_end_volume_constraint)
    model.basin_max_end_volume_constraint = pyo.Constraint(model.B, rule=basin_max_end_volume_constraint)
    model.basin_min_end_volume_constraint = pyo.Constraint(model.B, rule=basin_min_end_volume_constraint)
    ####################################################################################################################
    ### Basin volume boundary constraints used to determine the state of each basin ####################################
    ####################################################################################################################
    model.basin_max_state_constraint = pyo.Constraint(model.T, model.BS, rule=basin_max_state_constraint)
    model.basin_min_state_constraint = pyo.Constraint(model.T, model.BS, rule=basin_min_state_constraint)
    model.basin_state_constraint = pyo.Constraint(model.T, model.B, rule=basin_state_constraint)
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    # model.max_active_flow_by_state_constraint = pyo.Constraint(model.T, model.DHS, rule=max_active_flow_by_state_constraint)
    # model.max_inactive_flow_by_state_constraint = pyo.Constraint(model.T, model.DHBS, rule=max_inactive_flow_by_state_constraint)
    model.max_flow_by_state_constraint = pyo.Constraint(model.T, model.HBS, rule=max_flow_by_state_constraint)
    model.flow_constraint = pyo.Constraint(model.T, model.H, rule=flow_constraint)
    model.hydro_power_constraint = pyo.Constraint(model.T, model.H, rule=hydro_power_constraint)
    model.positive_hydro_ancillary_power_constraint = pyo.Constraint(model.TF, rule=positive_hydro_ancillary_power_constraint)
    model.negative_hydro_ancillary_power_constraint = pyo.Constraint(model.TF, rule=negative_hydro_ancillary_power_constraint)
    ####################################################################################################################
    ### Hydropower volume quota constraints ############################################################################
    ####################################################################################################################
    model.max_powered_volume_quota_constraint = pyo.Constraint(model.H, rule=max_powered_volume_quota_constraint)
    model.min_powered_volume_quota_constraint = pyo.Constraint(model.H, rule=min_powered_volume_quota_constraint)
    model.diff_volume_constraint = pyo.Constraint(model.H, rule=diff_volume_constraint)
    return model

def second_stage_baseline_model() -> pyo.AbstractModel:
    model: pyo.AbstractModel = pyo.AbstractModel() # type: ignore
    model = second_stage_sets(model)
    model = second_stage_parameters(model)
    model = second_stage_variables(model)
    model = second_stage_baseline_constraints(model)
    return model
