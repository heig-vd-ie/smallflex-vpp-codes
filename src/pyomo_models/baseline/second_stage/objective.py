r"""
.. math::
    :label: first-objective
    :nowrap:
    
    \\begin{align}
    \max \sum_{t \in T} c_\\text{DA}^{t} \cdot nb_\\text{HOUR}^{t} \cdot 
    \sum_{h \in H} \left( P_\\text{TUR}^{t,~h} - P_\\text{PUM}^{t,~h} \\right)
    \\end{align}

"""
import pyomo.environ as pyo

def baseline_objective(model, with_penalty: bool):
    if with_penalty:
        @model.Objective(sense=pyo.maximize) # type: ignore
        def selling_income(model):
            return (
                sum(
                    model.nb_hours * sum(model.market_price[t] * model.power[t, h] for t in model.T) +
                    (model.diff_volume_pos[h] * model.min_alpha[h] * model.pos_unpowered_price -
                    model.diff_volume_neg[h] * model.max_alpha[h] * model.neg_unpowered_price) / model.nb_sec
                    for h in model.H) - 
                sum(
                    sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b] 
                    for b in model.B) / model.nb_sec
            )
    else:
        @model.Objective(sense=pyo.maximize) # type: ignore
        def selling_income(model):
            return (
                sum(
                    model.nb_hours * sum(model.market_price[t] * model.power[t, h] for t in model.T) 
                    for h in model.H) - 
                sum(
                    sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b] 
                    for b in model.B) / model.nb_sec
            )
    return model