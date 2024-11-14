def basin_volume_constraint(model):
    ### Basin volume evolution constraints #############################################################################
    @model.Constraint(model.T, model.B) # type: ignore
    def basin_volume_evolution(model, t, b):
        if t == model.T.first():
            return model.basin_volume[t, b] == model.start_basin_volume[b]
        else:
            return model.basin_volume[t, b] == (
                model.basin_volume[t - 1, b] + model.discharge_volume[t - 1, b] - model.spilled_volume[t - 1, b] +
                sum(model.pumped_volume[t - 1, h] * model.water_pumped_factor(b, h) + 
                    model.turbined_volume[t - 1, h] * model.water_turbined_factor(b, h) 
                    for h in model.H
                )
            )

    @model.Constraint(model.B) # type: ignore
    def basin_end_volume_constraint(model, b):
        t_max = model.T.first()
        return model.start_basin_volume[b] == (
            model.basin_volume[t_max, b] + model.discharge_volume[t_max, b] - model.spilled_volume[t_max, b] +
            sum(model.pumped_volume[t_max, h] * model.water_pumped_factor(b, h) + 
                model.turbined_volume[t_max, h] * model.water_turbined_factor(b, h) 
                for h in model.H
            )
        )
    ### Basin volume boundary constraints used to determine the state of each basin ####################################
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_max_state_height_constraint(model, t, b, s_b):
        return (
            model.basin_volume[t, b] <= model.max_basin_volume[b, s_b] +
            model.max_basin_volume[b, model.S_b[b].last()] *  (1 - model.basin_state[t, b, s_b])
        )

    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_min_state_height_constraint(model, t, b, s_b):
        return model.basin_volume[t, b] >= model.basin_state[t, b, s_b] * model.min_basin_volume[b, s_b]

    @model.Constraint(model.T) # type: ignore
    def basin_state_constraint(model, t, b):
        return sum(model.basin_state[t, b, s] for s in model.S_b[b]) == 1
    
    ### basin volume per state constraints used to determine the state of each basin ###################################
    @model.Constraint(model.T, model.BS) # type: ignore
    def state_basin_volume_max_inactive_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] <= 
            model.max_basin_volume[b, s_b] * model.basin_state[t, b, s_b] 
        )
        
    @model.Constraint(model.T, model.BS) # type: ignor
    def state_basin_volume_min_inactive_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] >= 
            model.min_basin_volume[b, s_b] * model.basin_state[t, b, s_b]
        )

    @model.Constraint(model.T, model.BS) # type: ignor
    def state_basin_volume_max_active_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] <=
            model.basin_volume[t, b]  - model.min_basin_volume[b, s_b]  * (1 - model.basin_state[t, b, s_b])
        )

    @model.Constraint(model.T, model.BS) # type: ignor
    def state_basin_volume_min_active_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] >= 
            model.basin_volume[t, b] - model.min_basin_volume[b, s_b] * (1 - model.basin_state[t, b, s_b])
        )
    return model

