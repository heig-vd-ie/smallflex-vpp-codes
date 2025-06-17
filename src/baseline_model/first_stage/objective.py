r"""
.. math::
    :label: first-objective
    :nowrap:
    
    \begin{align}
        \max \sum_{t \in T} c_\text{DA}^{t} \cdot nb_\text{HOUR}^{t} \cdot 
        \sum_{h \in H} \left( P_\text{TUR}^{t,~h} - P_\text{PUM}^{t,~h} \right)
    \end{align}

"""

import pyomo.environ as pyo
def baseline_objective(model):

    @model.Objective(sense=pyo.maximize) # type: ignore
    def selling_income(model):
        market_price = sum(
            model.market_price[t] * model.nb_hours[t] * 
            sum(model.hydro_power[t, h] for h in model.H) for t in model.T
        )
        ancillary_market_price = sum(
            1*model.ancillary_market_price[t] * model.nb_hours[t] *
            sum(model.ancillary_power[t, h] for h in model.CH) for t in model.T
        )
        
        spilled_penality = sum(
            sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b] 
            for b in model.B
        ) / (model.nb_sec * model.volume_factor)
        
        return market_price + ancillary_market_price - spilled_penality
    
    return model