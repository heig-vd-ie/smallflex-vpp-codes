r"""
.. math::
    :label: second-objective
    :nowrap:
    
    \begin{align}
    \max  \sum_{t \in T} \left( c_\text{DA}^{t} \cdot  \sum_{h \in H} P^{t,~h} \right) \cdot nb_\text{HOUR} + PEN_\text{POW} - PEN_\text{SPILL}
    \end{align}

where:

.. math::
    :label: power-penality
    :nowrap:
    
    \begin{align}
    PEN_\text{POW} = \sum_{h \in H} \left( 
        dV_\text{+}^{h} \cdot \alpha^{+} \cdot c_\text{UN}^{+} - 
        dV_\text{-}^{h} \cdot \alpha^{-} \cdot c_\text{UN}^{-} \right) \
        / nb_\text{SEC}
    \end{align}

.. math::
    :label: spillage-penality
    :nowrap:
    
    \begin{align}
    PEN_\text{SPILL} = \sum_{b \in B} \left(F_\text{SPIL}^{b} \cdot \sum_{t \in T} V_\text{SPIL}^{t,~b} \right) / nb_\text{SEC}
    \end{align}

"""
import pyomo.environ as pyo

def baseline_objective(model):
    model.objective = pyo.Objective(rule=selling_income, sense=pyo.maximize)
    return model
        
def selling_income(model):
    
    market_price = sum(
            model.nb_hours * sum(model.market_price[t] * model.hydro_power[t, h] for t in model.T)
            for h in model.H
        ) 
    
    ancillary_market_price = sum(
            4*model.ancillary_market_price[f]  *
            sum(model.ancillary_power[f, h] for h in model.CH) for f in model.F
        )
    power_volume_penalty = sum(
            (model.diff_volume_pos[h] * model.alpha_pos[h] * model.pos_unpowered_price -
            model.diff_volume_neg[h] * model.alpha_neg[h] * model.neg_unpowered_price) 
            for h in model.H
        ) / (model.nb_sec * model.volume_factor) 
    spilled_penalty = sum(
            sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b] 
            for b in model.B
        ) / (model.nb_sec * model.volume_factor)
    return market_price + ancillary_market_price - spilled_penalty - power_volume_penalty
        
