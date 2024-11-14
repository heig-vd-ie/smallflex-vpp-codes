import pyomo.environ as pyo
from optimization.pyomo_models.sets import baseline_sets
from optimization.pyomo_models.parameters import baseline_parameters
from optimization.pyomo_models.variables import baseline_variables
from optimization.pyomo_models.objective import baseline_objective
from optimization.pyomo_models.constraints.basin_volume import basin_volume_constraint
from optimization.pyomo_models.constraints.hydropower_plan import hydropower_plan_constraint

def generate_baseline_model():
    
    model: pyo.AbstractModel = pyo.AbstractModel()
    model = baseline_sets(model)
    model = baseline_parameters(model)
    model = baseline_variables(model)
    model = baseline_objective(model)
    model = basin_volume_constraint(model)
    model = hydropower_plan_constraint(model)

    return model
