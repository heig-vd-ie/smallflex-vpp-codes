import pyomo.environ as pyo
from optimization_model.stochastic_first_stage.constraints import *
from optimization_model.stochastic_first_stage.sets import *
from optimization_model.stochastic_first_stage.parameters import *
from optimization_model.stochastic_first_stage.variables import *

def stochastic_first_stage_constraints(model):
    model.objective = pyo.Objective(rule=objective, sense=pyo.maximize)

    model.basin_volume_evolution = pyo.Constraint(model.T, model.Ω, model.B, rule=basin_volume_evolution)
    model.basin_end_volume_constraint = pyo.Constraint(model.Ω, model.B, rule=basin_end_volume_constraint)
    model.basin_volume_max_constraint = pyo.Constraint(model.T, model.Ω, model.B, rule=basin_volume_max_constraint)
    model.basin_volume_min_constraint = pyo.Constraint(model.T, model.Ω, model.B, rule=basin_volume_min_constraint)

    model.max_flow_constraint = pyo.Constraint(model.T, model.H, rule=max_flow_constraint)
    model.hydro_power_constraint = pyo.Constraint(model.T, model.H, rule=hydro_power_constraint)
    model.positive_hydro_hydro_ancillary_reserve_constraint = pyo.Constraint(model.T, rule=positive_hydro_hydro_ancillary_reserve_constraint)
    model.negative_hydro_hydro_ancillary_reserve_constraint = pyo.Constraint(model.T, rule=negative_hydro_hydro_ancillary_reserve_constraint)

    return model

def stochastic_first_stage_model() -> pyo.AbstractModel:
    model: pyo.AbstractModel = pyo.AbstractModel() # type: ignore
    model = first_stage_sets(model)
    model = first_stage_parameters(model)
    model = first_stage_variables(model)
    model = stochastic_first_stage_constraints(model)
    return model
