import pyomo.environ as pyo
from optimization.pyomo_models.sets import baseline_sets
from optimization.pyomo_models.parameters import baseline_parameters
from optimization.pyomo_models.variables import baseline_variables
from optimization.pyomo_models.objective import baseline_objective
from optimization.pyomo_models.constraints.basin_volume import basin_volume_constraints
from optimization.pyomo_models.constraints.turbine import turbine_constraints, turbine_power_1_constraints, turbine_power_2_constraints
from optimization.pyomo_models.constraints.pump import pump_constraints

def generate_baseline_model(with_multiplication: bool = False) -> pyo.AbstractModel:
    
    model: pyo.AbstractModel = pyo.AbstractModel()
    
    model = baseline_sets(model)
    model = baseline_parameters(model)
    model = baseline_variables(model)
    
    model = baseline_objective(model)
    model = basin_volume_constraints(model)
    model = turbine_constraints(model)
    model = pump_constraints(model)
    if with_multiplication:
        model = turbine_power_2_constraints(model)
    else: 
        model = turbine_power_1_constraints(model)

    return model
