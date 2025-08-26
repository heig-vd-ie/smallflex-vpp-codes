r"""
.. math::
    :label: first-objective
    :nowrap:
    
    \begin{align}
        \max \sum_{t \in T} c_\text{DA}^{t} \cdot nb_\text{HOUR}^{t} \cdot 
        \sum_{h \in H} \left( P_\text{TUR}^{t,~h} - P_\text{PUM}^{t,~h} \right)
    \end{align}


1.5.1. Water basin volume evolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-volume-evolution
    :nowrap:
        
    \begin{align}
        V_\text{BAS}^{t,~b} =
        \begin{cases} 
            V_\text{BAS, START}^{b} & \text{if } t = t_0 \\
            V_\text{BAS}^{t - 1,~b} + V_\text{DIS}^{t - 1,~b} - V_\text{SPIL}^{t - 1,~b} + 
            nb_\text{SEC} \cdot nb_\text{HOUR}^{t-1} \cdot 
            \sum_{h \in H} \left( 
                F_\text{TUR}^{b,~h} \cdot Q_\text{TUR}^{t-1,~h} + 
                F_\text{PUM}^{b,~h} \cdot Q_\text{PUM}^{t-1,~h} 
            \right) \quad & \text{if } t \neq t_0
        \end{cases} \qquad \forall \{t\in T, b \in B \}
    \end{align}

.. math::
    :label: basin-end-volume    
    :nowrap:
    
    \begin{align}
    V_\text{BAS, START}^{b} = V_\text{BAS}^{t_{end},~b} + V_\text{DIS}^{t_{end},~b}  - V_\text{SPIL}^{t_{end},~b} + 
    nb_\text{SEC} \cdot nb_\text{HOUR}^{t_{end}} \cdot
        \sum_{h \in H} \left(
            F_\text{TUR}^{b,~h} \cdot Q_\text{TUR}^{t_{end},~h} +
            F_\text{PUM}^{b,~h} \cdot Q_\text{PUM}^{t_{end},~h}
        \right) \qquad \forall \{b \in B \}
    \end{align} 

    
1.5.2. Water basin state
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-max-state
    :nowrap:
    
    \begin{align}
        V_\text{BAS}^{t,~b} \leq V_\text{BAS, MAX}^{b,~s} +  V_\text{BAS, MAX}^{b,~S_B^\text{END}\{b\}} 
        \cdot \left(1 -S_\text{BAS}^{t,~b,~s} \right)
    \qquad \forall \{t\in T~\vert~b \in B~\vert~ s \in S_B\{b\} \}
    
    \end{align} 

.. math::
    :label: basin-min-state
    :nowrap:
    
    \begin{align}
        V_\text{BAS}^{t,~b} \geq V_\text{BAS, MIN}^{b,~s} \cdot S_\text{BAS}^{t,~b,~s}
        \qquad \forall \{t\in T~\vert~b \in B \}
    \end{align}
.. math::
    :label: basin-total-state
    :nowrap:
    
    \begin{align}
        \sum_{s \in S_B\{b\}} S_\text{BAS}^{t,~b,~s} = 1 \qquad \forall \{t\in T~\vert~b \in B \}
    \end{align} 
    

1.5.4. Water pumped
~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: pumped-flow-state
    :nowrap:
    
    \begin{align}
        Q_\text{PUM, S}^{t,~h,~s_h} \leq Q_\text{PUM, MAX}^{h,~s_h} \cdot S_\text{BAS}^{t,~b,~s}
        \qquad \forall \{t\in T~\vert~b,~h,~s_h ~s_h \in S_{BH} \}
    \end{align}
.. math::
    :label: pumped-flow
    :nowrap:
    
    \begin{align}
        Q_\text{PUM}^{t,~h} = \sum_{s \in S_H\{h\}} Q_\text{PUM, S}^{t,~h,~s} 
        \qquad \forall \{t\in T~\vert~h \in H~\}
    \end{align}
    
.. math::
    :label: pumped-power
    :nowrap:
    
    \begin{align}
        P_\text{PUM}^{t,~h} = \sum_{s \in S_H\{h\}} \alpha_\text{PUM, AVG}^{h,~s} \cdot  Q_\text{PUM, S}^{t,~h,~s}
        \qquad \forall \{t\in T~\vert~h \in H\}
    \end{align}

The constraint :eq:`pumped-flow-state` takes the set :math:`S\_BH` as argument, enabling the connection between the 
basin set :math:`B` and the hydro powerplant set :math:`H`.
"""
import pyomo.environ as pyo

def first_stage_baseline_objective(model):
    market_price = sum(
        model.market_price[t] * model.nb_hours[t] * 
        sum(model.hydro_power[t, h] for h in model.H) for t in model.T
    )
    ancillary_market_price = sum(
        model.ancillary_market_price[t] * model.nb_hours[t] *
        sum(model.ancillary_power[t, h] for h in model.CH) for t in model.T
    )
    
    spilled_penalty = sum(
        sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b] 
        for b in model.B
    ) / (model.nb_sec * model.volume_factor)
        
    return market_price + ancillary_market_price - spilled_penalty
    
def basin_volume_evolution(model, t, b):
    if t == model.T.first():
        return model.basin_volume[t, b] == model.start_basin_volume[b]
    else:
        return model.basin_volume[t, b] == (
            model.basin_volume[t - 1, b] + model.discharge_volume[t - 1, b] - model.spilled_volume[t - 1, b] +
            model.nb_sec * model.volume_factor * model.nb_hours[t - 1] *
            sum(model.water_factor[b, h] * model.flow[t - 1, h] for h in model.H)
        )
        
def basin_end_volume_constraint(model, b):
    t_max = model.T.last()
    return model.start_basin_volume[b] == (
        model.basin_volume[t_max, b] + model.discharge_volume[t_max, b] - model.spilled_volume[t_max, b] +
        model.nb_sec * model.volume_factor * model.nb_hours[t_max] *
        sum(model.water_factor[b, h] * model.flow[t_max, h] for h in model.H)
    )

def basin_max_state(model, t, b, s):
    return (
        model.basin_volume[t, b] <= model.max_basin_volume[b, s] +
        model.max_basin_volume[b, model.S_B[b].last()] * 
        (1 - model.basin_state[t, b, s])
    )

def basin_min_state(model, t, b, s):
    return model.basin_volume[t, b] >= model.min_basin_volume[b, s] * model.basin_state[t, b, s]

def basin_state_total(model, t, b):
    return sum(model.basin_state[t, b, s] for s in model.S_B[b]) == 1


def max_flow_by_state(model, t, h, b, s):
    return (
        model.flow_by_state[t, h, s] <=
        model.max_turbined_volume_factor * model.max_flow[h, s] * model.basin_state[t, b, s]
    )

def total_flow(model, t, h):
    return (
        model.flow[t, h] ==
        sum(model.flow_by_state[t, h, s] for s in model.S_H[h])
    )

def total_hydro_power(model, t, h):
    return (
        model.hydro_power[t, h] ==
        sum(
            model.flow_by_state[t, h, s] *  model.alpha[h, s]
        for s in model.S_H[h])
    )

def positive_hydro_ancillary_power(model, t, h):
    return (
        model.hydro_power[t, h] + model.ancillary_power[t, h] <= model.max_power[h]
    )

def negative_hydro_ancillary_power(model, t, h):
    return model.hydro_power[t, h] - model.ancillary_power[t, h] >= 0



