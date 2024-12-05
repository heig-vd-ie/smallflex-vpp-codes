import pyomo.environ as pyo

def powered_volume_constraints(model):
    
    @model.Constraint(model.H) # type: ignore   
    def max_powered_volume_constraint(model, h):
        if model.powered_volume_enabled:
            pos_volume = model.remaining_volume[h] if model.remaining_volume[h] > 0 else 0
            return (
                model.diff_volume_pos[h] <= 
                model.powered_volume[h] *  model.buffer + pos_volume/3
            )
        else:
            return pyo.Constraint.Skip
        
    @model.Constraint(model.H) # type: ignore   
    def min_powered_volume_constraint(model, h):
        if model.powered_volume_enabled:
            neg_volume = - model.remaining_volume[h] if model.remaining_volume[h] < 0 else 0
            return (
                model.diff_volume_neg[h] <= 
                model.powered_volume[h] *  model.buffer + neg_volume/3
            )
        else:
            return pyo.Constraint.Skip
        
    @model.Constraint(model.H) # type: ignore
    def diff_volume_constraint(model, h):
        return (
            model.diff_volume_pos[h] - model.diff_volume_neg[h] ==
            model.remaining_volume[h] + model.powered_volume[h]  - 
            sum(model.flow[t, h] for t in model.T) * (model.nb_hours * model.nb_sec)
        )
    
    return model