import pyomo.environ as pyo


def generate_baseline_model():
    
    model: pyo.AbstractModel = pyo.AbstractModel()
    model = baseline_sets(model)
    model = baseline_parameters(model)
    model = baseline_variable(model)
    model = baseline_objective(model)
    model = baseline_volume_constraint(model)
    model = baseline_volume_evolution_constraint(model)
    
    model = baseline_turbined_volume_constraint(model)
    model = baseline_pumped_volume_constraint(model)
    
    return model
    

def baseline_sets(model):
    model.T = pyo.Set()
    model.H = pyo.Set()
    
    return model

def baseline_parameters(model):
    model.t_max = pyo.Param()
    model.h_max = pyo.Param()

    model.start_basin_volume = pyo.Param()
    
    model.max_turbined_flow = pyo.Param(default=1) # m^3/h
    model.min_turbined_flow = pyo.Param(default=0) # m^3/h

    model.max_pumped_flow = pyo.Param(default=1) # m^3/h
    model.min_pumped_flow = pyo.Param(default=0) # m^3/h

    model.max_basin_volume = pyo.Param(model.H)
    model.min_basin_volume = pyo.Param(model.H)
    model.alpha_turbined = pyo.Param(model.H)
    model.alpha_pumped = pyo.Param(model.H)

    model.discharge_volume = pyo.Param(model.T)
    
    model.wind_energy = pyo.Param(model.T, default=0)
    model.market_price = pyo.Param(model.T)
    model.max_market_price = pyo.Param(model.T)
    model.min_market_price = pyo.Param(model.T)
    model.nb_hours = pyo.Param(model.T)
    
    return model

def baseline_variable(model):
    model.basin_state = pyo.Var(model.T, model.H, within=pyo.Binary)
    model.basin_volume = pyo.Var(model.T, within=pyo.NonNegativeReals)
    
    model.turbined_volume = pyo.Var(model.T, within=pyo.Reals)
    model.turbined_volume_by_state = pyo.Var(model.T, model.H, within=pyo.Reals)

    model.pumped_volume = pyo.Var(model.T, within=pyo.Reals)
    model.pumped_volume_by_state = pyo.Var(model.T, model.H, within=pyo.Reals)


    return model


def baseline_objective(model):

    @model.Objective(sense=pyo.maximize) # type: ignore
    def selling_income(model):
        return sum(
            model.market_price[t] * (
            model.wind_energy[t] +
            sum(model.turbined_volume_by_state[t, h] * model.alpha_turbined[h] for h in model.H)/3600 - 
            sum(model.pumped_volume_by_state[t, h] * model.alpha_pumped[h] for h in model.H)/3600
            )  for t in model.T)

    
    return model


def baseline_volume_constraint(model):
    ### Basin height constraints
    @model.Constraint(model.T, model.H) # type: ignore
    def basin_max_state_height_constraint(model, t, h):
        return (
            model.basin_volume[t] <= model.max_basin_volume[h] +
            model.max_basin_volume[model.h_max] *  (1 - model.basin_state[t, h])
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def basin_min_state_height_constraint(model, t, h):
        return model.basin_volume[t] >= model.basin_state[t, h] * model.min_basin_volume[h]

    @model.Constraint(model.T) # type: ignore
    def basin_state_constraint(model, t):
        return sum(model.basin_state[t, h] for h in model.H) == 1
    
    return model

def baseline_volume_evolution_constraint(model):
    
    @model.Constraint(model.T) # type: ignore
    def basin_volume_evolution(model, t):
        if t == 0:
            return model.basin_volume[t] == model.start_basin_volume
        else:
            return model.basin_volume[t] == (
                model.basin_volume[t - 1] + model.discharge_volume[t - 1] +
                model.pumped_volume[t - 1] - model.turbined_volume[t - 1]
            )
    @model.Constraint() # type: ignore
    def basin_end_volume_constraint(model):
        return model.start_basin_volume == (
            model.basin_volume[model.t_max] + model.discharge_volume[model.t_max] +
            model.pumped_volume[model.t_max] - model.turbined_volume[model.t_max]
        )
    @model.Constraint(model.T) # type: ignore
    def max_pumped_volume_constraint(model, t):
        return (
            model.pumped_volume[t] <= model.max_pumped_flow * model.nb_hours[t]
        )

    @model.Constraint(model.T) # type: ignore
    def min_pumped_volume_constraint(model, t):
        return (
            model.pumped_volume[t] >= model.min_pumped_flow * model.nb_hours[t]
        )
            
    @model.Constraint(model.T) # type: ignore
    def max_turbined_volume_constraint(model, t):
        return (
            model.turbined_volume[t] <= model.max_turbined_flow * model.nb_hours[t]
        )

    @model.Constraint(model.T) # type: ignore
    def min_turbined_volume_constraint(model, t):
        return (
            model.turbined_volume[t] >= model.min_turbined_flow * model.nb_hours[t]
        )
    
    return model

def baseline_turbined_volume_constraint(model):
    # Basin volume constraints
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_volume_max_inactive_constraint(model, t, h):
        return (
            model.turbined_volume_by_state[t, h] <= model.max_turbined_flow * model.nb_hours[t] * model.basin_state[t, h]
        )
        
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_volume_min_inactive_constraint(model, t, h):
        return (
            model.turbined_volume_by_state[t, h] >= model.min_turbined_flow * model.nb_hours[t]  * model.basin_state[t, h]
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_volume_max_active_constraint(model, t, h):
        return (
            model.turbined_volume_by_state[t, h] <= model.turbined_volume[t]  - model.min_turbined_flow * model.nb_hours[t]  * (1 - model.basin_state[t, h])
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_volume_min_active_constraint(model, t, h):
        return (
            model.turbined_volume_by_state[t, h] >= model.turbined_volume[t]  -  model.max_turbined_flow * model.nb_hours[t] * (1 - model.basin_state[t, h])
        )

    return model

def baseline_pumped_volume_constraint(model):
    # Basin volume constraints
    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_volume_max_inactive_constraint(model, t, h):
        return (
            model.pumped_volume_by_state[t, h] <= model.max_pumped_flow * model.nb_hours[t] * model.basin_state[t, h]
        )
        
    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_volume_min_inactive_constraint(model, t, h):
        return (
            model.pumped_volume_by_state[t, h] >= model.min_pumped_flow * model.nb_hours[t]  * model.basin_state[t, h]
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_volume_max_active_constraint(model, t, h):
        return (
            model.pumped_volume_by_state[t, h] <= model.pumped_volume[t]  - model.min_pumped_flow * model.nb_hours[t]  * (1 - model.basin_state[t, h])
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_volume_min_active_constraint(model, t, h):
        return (
            model.pumped_volume_by_state[t, h] >= model.pumped_volume[t]  -  model.max_pumped_flow * model.nb_hours[t] * (1 - model.basin_state[t, h])
        )
    return model

