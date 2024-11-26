def basin_volume_constraints(model):
    ####################################################################################################################
    ### Basin volume evolution constraints #############################################################################  
    #################################################################################################################### 
    @model.Constraint(model.T, model.B) # type: ignore
    def basin_volume_evolution(model, t, b):
        if t == model.T.first():
            return model.basin_volume[t, b] == model.start_basin_volume[b]
        else:
            return model.basin_volume[t, b] == (
                model.basin_volume[t - 1, b] + model.discharge_volume[t - 1, b] - model.spilled_volume[t - 1, b]
                + sum(
                    model.nb_hours[t - 1] * 3600 * (
                    model.water_pumped_factor[b, h] * model.pumped_flow[t - 1, h]  +
                    model.water_turbined_factor[b_1, h] * model.turbined_flow[t - 1, h]
                    ) for h in model.H for b_1 in model.B if b_1 == b
                )
            )

    @model.Constraint(model.B) # type: ignore
    def basin_end_volume_constraint(model, b):
        t_max = model.T.last()
        return model.start_basin_volume[b] == (
            model.basin_volume[t_max, b] + model.discharge_volume[t_max, b] - model.spilled_volume[t_max, b]
            +sum(
                model.nb_hours[t_max] * 3600 * (
                model.water_pumped_factor[b, h] * model.pumped_flow[t_max, h] +
                model.water_turbined_factor[b, h] * model.turbined_flow[t_max, h]
                ) for h in model.H
            )
        )
    ####################################################################################################################
    ### Basin volume boundary constraints used to determine the state of each basin ####################################
    ####################################################################################################################
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_max_state_constraint(model, t, b, s_b):
        return (
            model.basin_volume[t, b] <= model.max_basin_volume[b, s_b] +
            model.max_basin_volume[b, model.S_b[b].last()] * 
            (1 - model.basin_state[t, b, s_b])
        )

    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_min_state_constraint(model, t, b, s_b):
        return model.basin_volume[t, b] >= model.min_basin_volume[b, s_b] * model.basin_state[t, b, s_b]

    @model.Constraint(model.T, model.B) # type: ignore
    def basin_state_constraint(model, t, b):
        return sum(model.basin_state[t, b, s] for s in model.S_b[b]) == 1

    ####################################################################################################################
    ### Basin volume state constraints used to determine the state of each basin #######################################
    ####################################################################################################################

    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_state_max_active_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] <= 
            model.max_basin_volume[b, model.S_b[b].last()] * model.basin_state[t, b, s_b]
        )
        
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_state_max_inactive_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] >= 
            model.basin_volume[t, b] -
            model.max_basin_volume[b, model.S_b[b].last()] * (1 - model.basin_state[t, b, s_b])
        )
    
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_state_min_active_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] >= 
            model.min_basin_volume[b, model.S_b[b].first()] * model.basin_state[t, b, s_b]
        )
        

        
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_state_min_inactive_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] <= 
            model.basin_volume[t, b] - 
            model.min_basin_volume[b, model.S_b[b].first()] * (1 - model.basin_state[t, b, s_b])
        )
        
    return model