def hydropower_plan_constraint(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    @model.Constraint(model.T, model.S_BH) # type: ignore
    def state_turbined_volume_constraint(model, t, h, b, s_b, s_h):
        return (
            model.turbined_volume_by_state[t, h, s_h] <=
            model.min_flow_turbined[h, s_h] * model.basin_state[t, b, s_b] * 3600 * model.nb_hours[t]
        )  
    
    @model.Constraint(model.T, model.S_BH) # type: ignore
    def state_turbined_energy_constraint(model, t, h, b, s_b, s_h):
        return (
            model.turbined_energy_by_state[t, h, s_h] ==
            model.turbined_volume_by_state[t, h, s_h] * model.min_alpha_turbined[h, s_h] / 3600
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_volume_constraint(model, t, h):
        return (
            model.turbined_volume[t, h] ==
            sum(model.turbined_volume_by_state[t, h, s_h] for s_h in model.S_h[h])
        )  
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_energy_constraint(model, t, h):
        return (
            model.turbined_energy[t, h] ==
            sum(model.turbined_energy_by_state[t, h, s_h] for s_h in model.S_h[h])
        )  
        
    ####################################################################################################################
    
    ####################################################################################################################
    @model.Constraint(model.T, model.S_BH) # type: ignore
    def state_pumped_volume_constraint(model, t, h, b, s_b, s_h):
        return (
            model.pumped_volume_by_state[t, h, s_h] <=
            model.min_flow_pumped[h, s_h] * model.basin_state[t, b, s_b] * 3600 * model.nb_hours[t]
        )  
    
    @model.Constraint(model.T, model.S_BH) # type: ignore
    def state_pumped_energy_constraint(model, t, h, b, s_b, s_h):
        return (
            model.pumped_energy_by_state[t, h, s_h] ==
            model.pumped_volume_by_state[t, h, s_h] * model.min_alpha_pumped[h, s_h] / 3600
        )

    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_volume_constraint(model, t, h):
        return (
            model.pumped_volume[t, h] == 
            sum(model.pumped_volume_by_state[t, h, s_h] for s_h in model.S_h[h])
        )  
    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_energy_constraint(model, t, h):
        return (
            model.pumped_energy[t, h] ==
            sum(model.pumped_energy_by_state[t, h, s_h] for s_h in model.S_h[h])
        )  
    # def baseline_turbined_volume_constraint(model):
    # # Basin energy constraints
    # @model.Constraint(model.T, model.S_BH) # type: ignore
    # def turbined_volume_max_inactive_constraint(model, t, h, b, s_b, s_h):
    #     return (
    #         model.turbined_energy_by_state[t, h, s_h] <= 
    #         model.min_alpha_turbined[h, s_h] * model.min_flow_turbined[h, model.S_h[h].last()] * 
    #         model.nb_hours[t] * model.basin_state[t, b, s_b]
    #     )
        
    # @model.Constraint(model.T, model.S_BH) # type: ignore
    # def turbined_volume_min_inactive_constraint(model, t, h, b, s_b, s_h):
    #     return (
    #         model.turbined_energy_by_state[t, h] >=
    #         model.min_alpha_turbined[h, s_h] * model.min_flow_turbined[h, model.S_h[h].first()] * 
    #         model.nb_hours[t] * model.basin_state[t, b, s_b]
    #     )

    # @model.Constraint(model.T, model.H) # type: ignore
    # def turbined_volume_max_active_constraint(model, t, h):
    #     return (
    #         model.turbined_energy_by_state[t, h] <= model.turbined_volume[t]  - model.min_turbined_flow * model.nb_hours[t]  * (1 - model.basin_state[t, h])
    #     )

    # @model.Constraint(model.T, model.H) # type: ignore
    # def turbined_volume_min_active_constraint(model, t, h):
    #     return (
    #         model.turbined_energy_by_state[t, h] >= model.turbined_volume[t]  -  model.max_turbined_flow * model.nb_hours[t] * (1 - model.basin_state[t, h])
    #     )
    
    # @model.Constraint(model.T, model.H) # type: ignore    
    # def pumped_volume_constraint(model, t, h):
    #     return model.pumped_volume[t, h] == sum(
    #         (model.min_flow_turbined[h_1, s_h]  - model.min_basin_volume[b, s_b]) * model.basin_state[t, b, s_b] +
    #         model.d_flow_pumped[h_1, s_h] * model.basin_volume_by_state[t, b, s_b] 
    #         for  h_1, b, s_b, s_h in model.S_BH if h_1 == h) * model.nb_hours[t] 
        

        
    # # @model.Constraint(model.T, model.H) # type: ignore    
    # # def pumped_energy_constraint(model, t, h):
    # #     return model.pumped_energy[t, h] == sum(
    # #         (model.min_power_pumped[h_1, s_h] - model.min_basin_volume[b, s_b]) * model.basin_state[t, b, s_b] +
    # #         model.d_power_pumped[h_1, s_h] * model.basin_volume_by_state[t, b, s_b] 
    # #         for  h_1, b, s_b, s_h in model.S_BH if h_1 == h) * model.nb_hours[t]
    # @model.Constraint(model.T, model.H) # type: ignore    
    # def pumped_volume_constraint(model, t, h):
    #     return model.pumped_volume[t, h] == 0
    
    # @model.Constraint(model.T, model.H) # type: ignore    
    # def pumped_energy_constraint(model, t, h):
    #     return model.pumped_energy[t, h] == 0
    
    # return model
    
    # @model.Constraint(model.T, model.H) # type: ignore
    # def turbined_volume_constraint(model, t, h):
    #     return model.turbined_volume[t, h] == model.min_flow_turbined[h, model.S_h[h].last()] * model.nb_hours[t]*3600
    #         # model.min_flow_turbined[h_1, s_h]
    #         # for  h_1, b, s_b, s_h in model.S_BH if h_1 == h) * model.nb_hours[t]
        
    # # @model.Constraint(model.T, model.H) # type: ignore    
    # # def pumped_volume_constraint(model, t, h):
    # #     return model.pumped_volume[t, h] == 0
        
    # @model.Constraint(model.T, model.H) # type: ignore
    # def turbined_energy_constraint(model, t, h):
    #     return model.turbined_energy[t, h] == model.min_power_turbined[h, model.S_h[h].last()] * model.nb_hours[t]
        
    # @model.Constraint(model.T, model.H) # type: ignore    
    # def pumped_energy_constraint(model, t, h):
    #     return model.pumped_energy[t, h] == 0

    return model
