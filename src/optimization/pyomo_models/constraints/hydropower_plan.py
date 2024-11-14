def hydropower_plan_constraint(model):
        
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_volume_constraint(model, t, h):
        return model.pumped_volume[t, h] == sum(
            (model.min_flow_turbined[t, h_1, s_h]  - model.min_basin_volume[b, s_b]) * model.basin_state[t, b, s_b] +
            model.d_flow_turbined[t, h_1, s_h] * model.basin_volume_by_state[t, b, s_b] 
            for  h_1, b, s_b, s_h in model.S_BH if h_1 == h) * model.nb_hours[t]
        
    @model.Constraint(model.T, model.H) # type: ignore    
    def pumped_volume_constraint(model, t, h):
        return model.pumped_volume[t, h] == sum(
            (model.min_flow_turbined[t, h_1, s_h]  - model.min_basin_volume[b, s_b]) * model.basin_state[t, b, s_b] +
            model.d_flow_pumped[t, h_1, s_h] * model.basin_volume_by_state[t, b, s_b] 
            for  h_1, b, s_b, s_h in model.S_BH if h_1 == h) * model.nb_hours[t] 
        
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_energy_constraint(model, t, h):
        return model.turbined_energy[t, h] == sum(
            (model.min_power_turbined[t, h_1, s_h] - model.min_basin_volume[b, s_b]) * model.basin_state[t, b, s_b] +
            model.d_power_turbined[t, h_1, s_h] * model.basin_volume_by_state[t, b, s_b] 
            for  h_1, b, s_b, s_h in model.S_BH if h_1 == h) * model.nb_hours[t]
        
    @model.Constraint(model.T, model.H) # type: ignore    
    def pumped_energy_constraint(model, t, h):
        return model.pumped_energy[t, h] == sum(
            (model.min_power_pumped[t, h_1, s_h] - model.min_basin_volume[b, s_b]) * model.basin_state[t, b, s_b] +
            model.d_power_pumped[t, h_1, s_h] * model.basin_volume_by_state[t, b, s_b] 
            for  h_1, b, s_b, s_h in model.S_BH if h_1 == h) * model.nb_hours[t]
        
    return model