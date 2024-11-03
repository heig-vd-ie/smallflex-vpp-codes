import pyomo.environ as pyo


def generate_baseline_model():
    
    model: pyo.AbstractModel = pyo.AbstractModel()
    model = baseline_sets(model)
    model = baseline_parameters(model)
    model = baseline_variable(model)
    model = baseline_objective(model)
    model = baseline_volume_constaint(model)
    model = baseline_volume_evolution_constaint(model)
    model = baseline_powered_volume_constaint(model)
    return model
    

def baseline_sets(model):
    model.T = pyo.Set()
    model.H = pyo.Set()
    
    return model

def baseline_parameters(model):
    model.t_max = pyo.Param()
    model.h_max = pyo.Param()

    model.max_flow = pyo.Param() # m^3/h
    model.min_flow = pyo.Param() # m^3/h
    model.start_basin_volume = pyo.Param()


    model.max_basin_volume = pyo.Param(model.H)
    model.min_basin_volume = pyo.Param(model.H)
    model.alpha = pyo.Param(model.H)


    model.discharge_volume = pyo.Param(model.T)
    model.market_price = pyo.Param(model.T)
    model.nb_hours = pyo.Param(model.T)
    
    return model

def baseline_variable(model):
    model.basin_state = pyo.Var(model.T, model.H, within=pyo.Binary)
    model.powered_volume_by_state = pyo.Var(model.T, model.H, within=pyo.Reals)
    
    model.powered_volume = pyo.Var(model.T, within=pyo.Reals)
    model.basin_volume = pyo.Var(model.T, within=pyo.NonNegativeReals)

    return model


def baseline_objective(model):
    @model.Objective(sense=pyo.maximize) # type: ignore
    def selling_income(model):
        return sum(model.market_price[t] * sum(model.powered_volume_by_state[t, h] * model.alpha[h] for h in  model.H)  for t in model.T)
    return model


def baseline_volume_constaint(model):
    ### Basin height constraints
    @model.Constraint(model.T, model.H) # type: ignore
    def basin_max_state_height_constraint(model, t, h):
        return model.basin_volume[t] <= model.max_basin_volume[h] + 1e10 *  (1 - model.basin_state[t, h])

    @model.Constraint(model.T, model.H) # type: ignore
    def basin_min_state_height_constraint(model, t, h):
        return model.basin_volume[t] >= model.basin_state[t, h] * model.min_basin_volume[h]

    @model.Constraint(model.T) # type: ignore
    def basin_state_constraint(model, t):
        return sum(model.basin_state[t, h] for h in model.H) == 1
    
    return model


def baseline_volume_evolution_constaint(model):
    @model.Constraint(model.T) # type: ignore
    def basin_volume_evolution(model, t):
        if t == 0:
            return model.basin_volume[t] == model.start_basin_volume
        else:
            return model.basin_volume[t] == model.basin_volume[t - 1] + model.discharge_volume[t - 1] - model.powered_volume[t - 1]
            
    
    @model.Constraint() # type: ignore
    def basin_end_volume_constraint(model):
        return model.start_basin_volume == model.basin_volume[model.t_max] + model.discharge_volume[model.t_max] - model.powered_volume[model.t_max]

    @model.Constraint(model.T) # type: ignore
    def max_volume_constraint(model, t):
        return (
            model.powered_volume[t] <= model.max_flow * model.nb_hours[t]
        )

    @model.Constraint(model.T) # type: ignore
    def min_volume_constraint(model, t):
        return (
            model.powered_volume[t] >= model.min_flow * model.nb_hours[t]
        )
    
    return model

def baseline_powered_volume_constaint(model):
    # Basin volume constraints
    @model.Constraint(model.T, model.H) # type: ignore
    def volume_max_inactive_constraint(model, t, h):
        return (
            model.powered_volume_by_state[t, h] <= model.max_flow * model.nb_hours[t] * model.basin_state[t, h]
        )
        
    @model.Constraint(model.T, model.H) # type: ignore
    def volume_min_inactive_constraint(model, t, h):
        return (
            model.powered_volume_by_state[t, h] >= model.min_flow * model.nb_hours[t]  * model.basin_state[t, h]
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def volume_max_active_constraint(model, t, h):
        return (
            model.powered_volume_by_state[t, h] <= model.powered_volume[t]  - model.min_flow * model.nb_hours[t]  * (1 - model.basin_state[t, h])
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def volume_min_active_constraint(model, t, h):
        return (
            model.powered_volume_by_state[t, h] >= model.powered_volume[t]  -  model.max_flow * model.nb_hours[t] * (1 - model.basin_state[t, h])
        )

    return model

