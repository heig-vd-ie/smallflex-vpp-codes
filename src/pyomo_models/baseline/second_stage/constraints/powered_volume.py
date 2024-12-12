import pyomo.environ as pyo

def powered_volume_constraints(model):
    model.max_powered_volume_constraint = pyo.Constraint(model.H, rule=max_powered_volume_constraint)
    model.min_powered_volume_constraint = pyo.Constraint(model.H, rule=min_powered_volume_constraint)
    model.diff_volume_constraint = pyo.Constraint(model.H, rule=diff_volume_constraint)
    return model
    
def max_powered_volume_constraint(model, h):
    if model.powered_volume_enabled:
        pos_volume = model.remaining_volume[h] if model.remaining_volume[h] > 0 else 0
        return (
            model.diff_volume_pos[h] <= model.volume_buffer[h] + pos_volume/2
        )
    else:
        return pyo.Constraint.Skip
    
def min_powered_volume_constraint(model, h):
    if model.powered_volume_enabled:
        neg_volume = - model.remaining_volume[h] if model.remaining_volume[h] < 0 else 0
        return (
            model.diff_volume_neg[h] <= model.volume_buffer[h] + neg_volume/2
        )
    else:
        return pyo.Constraint.Skip
    
def diff_volume_constraint(model, h):
    return (
        model.diff_volume_pos[h] - model.diff_volume_neg[h] ==
        model.remaining_volume[h] + model.powered_volume[h]  - 
        sum(model.flow[t, h] for t in model.T) * model.nb_hours * model.nb_sec * model.volume_factor
    )
