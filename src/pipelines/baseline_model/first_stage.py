import pyomo.environ as pyo
from optimization_model.first_stage.constraint import *
from optimization_model.first_stage.sets import *
from optimization_model.first_stage.parameters import *
from optimization_model.first_stage.variables import *

def first_stage_baseline_constraints(model):
    model.objective = pyo.Objective(rule=first_stage_baseline_objective, sense=pyo.maximize)
    model.basin_volume_evolution = pyo.Constraint(model.T, model.B, rule=basin_volume_evolution)
    model.basin_end_volume = pyo.Constraint(model.B, rule=basin_end_volume_constraint)
    model.basin_max_state = pyo.Constraint(model.T, model.BS, rule=basin_max_state)
    model.basin_min_state = pyo.Constraint(model.T, model.BS, rule=basin_min_state)
    model.basin_state_total = pyo.Constraint(model.T, model.B, rule=basin_state_total)

    model.max_flow_by_state = pyo.Constraint(model.T, model.HBS, rule=max_flow_by_state)
    model.total_flow = pyo.Constraint(model.T, model.H, rule=total_flow)
    model.total_hydro_power = pyo.Constraint(model.T, model.H, rule=total_hydro_power)
    model.positive_hydro_ancillary_power = pyo.Constraint(model.T, model.CH, rule=positive_hydro_ancillary_power)
    model.negative_hydro_ancillary_power = pyo.Constraint(model.T, model.CH, rule=negative_hydro_ancillary_power)
    
    return model

def first_stage_baseline_model() -> pyo.AbstractModel:
    model: pyo.AbstractModel = pyo.AbstractModel() # type: ignore
    model = first_stage_sets(model)
    model = first_stage_parameters(model)
    model = first_stage_variables(model)
    model = first_stage_baseline_constraints(model)
    return model
