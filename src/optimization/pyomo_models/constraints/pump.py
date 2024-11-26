def pump_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    @model.Constraint(model.T, model.S_BH) # type: ignore
    def pumped_flow_constraint(model, t, h, b, s_b, s_h ):
        return (
            model.pumped_flow[t, h] <=
            sum(
                model.min_flow_pumped[h_1, s_h] * model.basin_state[t, b, s_b]
                for h_1, b, s_h, s_b in model.S_BH if h_1 == h
            )
        ) 
    
    @model.Constraint(model.T, model.H) # type: ignore
    def alpha_pumped_constraint(model, t, h):
        return (
            model.pumped_alpha[t, h] ==
            sum(
            model.d_alpha_pumped[h_1, s_h] * model.basin_volume_by_state[t, b, s_b]  + model.basin_state[t, b, s_b] * 
            (model.min_alpha_pumped[h_1, s_h] - model.d_alpha_pumped[h_1, s_h] * model.min_basin_volume[b, s_b])
            for h_1, b, s_b, s_h in model.S_BH if h_1 == h)
        ) 
        
    ####################################################################################################################
    ## pumped power constraints ######################################################################################
    #################################################################################################################### 
    
    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_power_max_constraint(model, t, h):
        max_flow = model.min_flow_pumped[h, model.S_h[h].last()]
        max_alpha = model.max_alpha_pumped[h, model.S_h[h].last()]
        return (
            model.pumped_power[t, h] >= 
            model.pumped_alpha[t, h] * max_flow + 
            model.pumped_flow[t, h] * max_alpha -
            max_flow * max_alpha
        ) 
        
    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_power_min_constraint(model, t, h):
        min_alpha = model.min_alpha_pumped[h, model.S_h[h].first()]
        return (
            model.pumped_power[t, h] >= 
            model.pumped_flow[t, h] * min_alpha
        ) 
    
    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_power_min_max_constraint(model, t, h):
        max_alpha = model.max_alpha_pumped[h, model.S_h[h].last()]
        return (
            model.pumped_power[t, h] <= model.pumped_flow[t, h] * max_alpha
        ) 
    
    @model.Constraint(model.T, model.H) # type: ignore
    def pumped_power_max_min_constraint(model, t, h):
        max_flow = model.min_flow_pumped[h, model.S_h[h].last()]
        min_alpha = model.min_alpha_pumped[h, model.S_h[h].first()]
        return (
            model.pumped_power[t, h] <= 
            model.pumped_alpha[t, h] * max_flow + 
            model.pumped_flow[t, h] * min_alpha -
            min_alpha * max_flow
        )    
    
    return model
