import pyomo.environ as pyo


def generate_baseline_model():
    
    model: pyo.AbstractModel = pyo.AbstractModel()
    model = baseline_sets(model)
    model = baseline_parameters(model)
    model = baseline_variable(model)
    model = baseline_objective(model)
    model = baseline_static_height_constaint(model)
    model = baseline_height_evolution_constaint(model)
    model = baseline_water_volume_constaint(model)
    return model
    

def baseline_sets(model):
    model.T = pyo.Set()
    model.H = pyo.Set()
    
    return model

def baseline_parameters(model):
    model.t_max = pyo.Param()
    model.alpha = pyo.Param()
    model.max_flow = pyo.Param() # m^3/h
    model.min_flow = pyo.Param() # m^3/h
    model.start_basin_height = pyo.Param()
    model.basin_dH_dV_2 = pyo.Param()

    model.max_basin_height = pyo.Param(model.H)
    model.min_basin_height = pyo.Param(model.H)
    model.basin_dH_dV = pyo.Param(model.H)

    model.discharge_volume = pyo.Param(model.T)
    model.market_price = pyo.Param(model.T)
    model.nb_hours = pyo.Param(model.T)
    
    return model

def baseline_variable(model):
    model.basin_state = pyo.Var(model.T, model.H, within=pyo.Binary)
    model.basin_height = pyo.Var(model.T, within=pyo.NonNegativeReals)

    model.basin_volume_state= pyo.Var(model.T, model.H, within=pyo.Reals)
    model.V_tot = pyo.Var(model.T, within=pyo.Reals)

    return model


def baseline_objective(model):
    @model.Objective(sense=pyo.maximize) # type: ignore
    def selling_income(model):
        return sum(model.market_price[t] * model.V_tot[t] * model.alpha  for t in model.T)
    
    return model


def baseline_static_height_constaint(model):
    ### Basin height constraints
    @model.Constraint(model.T, model.H) # type: ignore
    def basin_max_state_height_constraint(model, t, h):
        return model.basin_height[t] <= model.max_basin_height[h] + 1e6 *  (1 - model.basin_state[t, h])

    @model.Constraint(model.T, model.H) # type: ignore
    def basin_min_state_height_constraint(model, t, h):
        return model.basin_height[t] >= model.basin_state[t, h] * model.min_basin_height[h]


    @model.Constraint(model.T) # type: ignore
    def basin_state_constraint(model, t):
        return sum(model.basin_state[t, h] for h in model.H) == 1
    
    return model


def baseline_height_evolution_constaint(model):
    @model.Constraint(model.T) # type: ignore
    def basin_height_evolution(model, t):
        if t == 0:
            return model.basin_height[t] == model.start_basin_height
        else:
            return (
                model.basin_height[t] == model.basin_height[t - 1] +
                sum(
                    (model.discharge_volume[t- 1] * model.basin_state[t - 1, h] - model.basin_volume_state[t - 1, h]) * model.basin_dH_dV[h]
                    for h in model.H
                )
            )
            
    @model.Constraint() # type: ignore
    def basin_end_height_constraint(model):
        return (
            model.start_basin_height == model.basin_height[model.t_max] +
            sum(
                (model.discharge_volume[model.t_max] * model.basin_state[model.t_max, h] - model.basin_volume_state[model.t_max, h]) * model.basin_dH_dV[h]
                for h in model.H
            )
        )
    return model
        
def baseline_water_volume_constaint(model):
    # Basin volume constraints
    @model.Constraint(model.T, model.H) # type: ignore
    def volume_max_inactive_constraint(model, t, h):
        return (
            model.basin_volume_state[t, h] <= model.max_flow * model.nb_hours[t] * model.basin_state[t, h]
        )
        
    @model.Constraint(model.T, model.H) # type: ignore
    def volume_min_inactive_constraint(model, t, h):
        return (
            model.basin_volume_state[t, h] >= model.min_flow * model.nb_hours[t] * model.basin_state[t, h]
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def volume_max_active_constraint(model, t, h):
        return (
            model.basin_volume_state[t, h] <= model.V_tot[t]  - model.min_flow * model.nb_hours[t] * (1 - model.basin_state[t, h])
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def volume_min_active_constraint(model, t, h):
        return (
            model.basin_volume_state[t, h] >= model.V_tot[t]  - model.max_flow * model.nb_hours[t] * (1 - model.basin_state[t, h])
        )

    @model.Constraint(model.T) # type: ignore
    def max_volume_constraint(model, t):
        return (
            model.V_tot[t] <= model.max_flow * model.nb_hours[t]
        )

    @model.Constraint(model.T) # type: ignore
    def min_volume_constraint(model, t):
        return (
            model.V_tot[t] >= model.min_flow * model.nb_hours[t]
        )

    return model