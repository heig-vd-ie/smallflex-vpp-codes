def turbine_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    @model.Constraint(model.T, model.S_BH) # type: ignore
    def turbined_flow_by_state_constraint(model, t, h, b, s_h, s_b):
        return (
            model.turbined_flow_by_state[t, h, s_h] <= model.min_flow_turbined[h, s_h] * model.basin_state[t, b, s_b]
        ) 
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_flow_constraint(model, t, h):
        return (
            model.turbined_flow[t, h] ==
            sum(model.turbined_flow_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
    
    @model.Constraint(model.T,model.S_BH) # type: ignore
    def alpha_turbined_by_state_constraint(model, t, h, b, s_h, s_b):
        return (
            model.turbined_alpha_by_state[t, h, s_h] ==
            model.d_alpha_turbined[h, s_h] * model.basin_volume_by_state[t, b, s_b]  + model.basin_state[t, b, s_b] * 
            (model.min_alpha_turbined[h, s_h] - model.d_alpha_turbined[h, s_h] * model.min_basin_volume[b, s_b])
        ) 
        
    @model.Constraint(model.T, model.H) # type: ignore
    def alpha_turbined_constraint(model, t, h):
        return (
            model.turbined_alpha[t, h] ==
            sum(model.turbined_alpha_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
        
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_power_constraint(model, t, h):
        return (
            model.turbined_power[t, h] ==
            sum(model.turbined_power_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
        
    ####################################################################################################################
    ## Turbined power constraints ######################################################################################
    #################################################################################################################### 
    @model.Constraint(model.T, model.HS) # type: ignore
    def turbined_power_max_constraint(model, t, h, s_h):
        max_flow = model.min_flow_pumped[h, model.S_h[h].last()]
        return (
            model.turbined_power_by_state[t, h, s_h] >= 
            # model.turbined_alpha_by_state[t, h, s_h] * model.min_flow_turbined[h, s_h] + 
            # model.turbined_flow_by_state[t, h, s_h] * model.max_alpha_turbined[h, s_h] 
            model.turbined_alpha_by_state[t, h, s_h] * max_flow +
            model.turbined_flow[t, h] * model.max_alpha_turbined[h, s_h] 
            - max_flow * model.max_alpha_turbined[h, s_h]
        ) 

    
    @model.Constraint(model.T, model.HS) # type: ignore
    def turbined_power_min_max_constraint(model, t, h, s_h):
        return (
            model.turbined_power_by_state[t, h, s_h] <= 
            model.turbined_flow[t, h] * model.max_alpha_turbined[h, s_h] 
            # model.turbined_flow_by_state[t, h, s_h] * model.max_alpha_turbined[h, s_h] 
        ) 
    
    @model.Constraint(model.T, model.HS) # type: ignore
    def turbined_power_max_min_constraint(model, t, h, s_h):
        max_flow = model.min_flow_pumped[h, model.S_h[h].last()]
        return (
            model.turbined_power_by_state[t, h, s_h] <= 
            model.turbined_alpha_by_state[t, h, s_h] * max_flow
        )  
    
    return model

    
    # @model.Constraint(model.T, model.H) # type: ignore
    # def turbined_power_max_constraint(model, t, h):
    #     return (
    #         model.turbined_power[t, h] >= 
    #         model.turbined_alpha[t, h] * model.min_flow_turbined[h, model.S_h[h].last()] + 
    #         model.turbined_flow[t, h] * model.max_alpha_turbined[h, model.S_h[h].last()] -
    #         model.min_flow_turbined[h, model.S_h[h].last()] * model.max_alpha_turbined[h, model.S_h[h].last()]
    #     ) 
        
    # @model.Constraint(model.T, model.H) # type: ignore
    # def turbined_power_min_constraint(model, t, h):
    #     return (
    #         model.turbined_power[t, h] >= 
    #         model.turbined_flow[t, h] * model.min_alpha_turbined[h, model.S_h[h].first()]
    #     ) 
    
    # @model.Constraint(model.T, model.H) # type: ignore
    # def turbined_power_min_max_constraint(model, t, h):
    #     return (
    #         model.turbined_power[t, h] <= 
    #         model.turbined_flow[t, h] * model.max_alpha_turbined[h, model.S_h[h].last()] 
    #     ) 
    
    # @model.Constraint(model.T, model.H) # type: ignore
    # def turbined_power_max_min_constraint(model, t, h):
    #     return (
    #         model.turbined_power[t, h] <= 
    #         model.turbined_alpha[t, h] * model.min_flow_turbined[h, model.S_h[h].last()] + 
    #         model.turbined_flow[t, h] * model.min_alpha_turbined[h, model.S_h[h].first()] -
    #         model.min_flow_turbined[h, model.S_h[h].last()] * model.min_alpha_turbined[h, model.S_h[h].first()]
    #     )    
    
