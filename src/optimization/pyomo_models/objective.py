"""
.. math::
    :label: first-objective
    :nowrap:
    
    \\begin{align}
    \max \sum_{t \in T} c_\\text{DA}^{t} \cdot nb_\\text{HOUR}^{t} \cdot 
    \sum_{h \in H} \left( P_\\text{TUR}^{t,~h} - P_\\text{PUM}^{t,~h} \\right)
    \\end{align}

"""

import pyomo.environ as pyo
def baseline_objective(model):

    @model.Objective(sense=pyo.maximize) # type: ignore
    def selling_income(model):
        return sum(
            model.market_price[t] * model.nb_hours[t] * 
            sum(
                model.turbined_power[t, h] 
                - model.pumped_power[t, h] 
                for h in model.H
            ) for t in model.T
        )

    return model