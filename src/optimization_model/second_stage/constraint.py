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
2.5.1 Water basin volume evolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-volume-evolution-2
    :nowrap:
        
    \begin{align}
        V_\text{BAS}^{t,~b} =
        \begin{cases} 
            V_\text{BAS, START}^{b} & \text{if } t = t_0 \\
            V_\text{BAS}^{t - 1,~b} + V_\text{DIS}^{t - 1,~b} - V_\text{SPIL}^{t - 1,~b} + 
            nb_\text{SEC} \cdot nb_\text{HOUR} \cdot 
            \sum_{h \in H} F_\text{TUR}^{b,~h} \cdot Q^{t-1,~h} 
            \quad & \text{if } t \neq t_0
        \end{cases} \qquad \forall \{t\in T, b \in B \}
    \end{align} 
    
.. math::
    :label: end-basin-volume-evolution-2
    :nowrap:
        
    \begin{align}
        V_\text{BAS, END}^{b} = V_\text{BAS}^{t_{end},~b} + V_\text{DIS}^{t_{end},~b}  - V_\text{SPIL}^{t_{end},~b} + 
        nb_\text{SEC} \cdot nb_\text{HOUR} \cdot \sum_{h \in H} F_\text{TUR}^{b,~h} \cdot Q^{t_{end},~h}
    \qquad \forall \{b \in B \}
    \end{align} 
    
.. math::
    :label: max-end-basin-volume-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS, END}^{b} &\leq V_\text{BAS, MAX}^{b,~S_B^\text{END}\{b\}} 
    \qquad \forall \{b \in B \}
    \end{align}

.. math::
    :label: min-end-basin-volume-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS, END}^{b} &\geq V_\text{BAS, MIN}^{b,~S_B^\text{0}\{b\}} 
    \qquad \forall \{b \in B \}
    \end{align}


    
2.5.2 Water basin state
~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: max-basin-state-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS}^{t,~b} &\leq V_\text{BAS, MAX}^{b,~s} +  V_\text{BAS, MAX}^{b,~S_B^\text{END}\{b\}} 
    \cdot \left(1 -S_\text{BAS}^{t,~b,~s} \right)
    \qquad \forall \{t\in T~\vert~b \in B~\vert~ s \in S_B\{b\} \}
    \end{align}
    
.. math::
    :label: min-basin-state-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS}^{t,~b} &\geq V_\text{BAS, MIN}^{b,~s} \cdot S_\text{BAS}^{t,~b,~s}
    \qquad \forall \{t\in T~\vert~b \in B \}
    \end{align}


.. math::
    :label: basin-state-2
    :nowrap:
    
    \begin{align}
    \sum_{s \in S_B\{b\}} S_\text{BAS}^{t,~b,~s} = 1 \qquad \forall \{t\in T~\vert~b \in B \}
    \end{align} 

.. math::
    :label: basin-volume-per-state-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS, S}^{t,~h,~s,~s_q} = V_\text{BAS}^{t,~b_h} \cdot S_\text{FLOW}^{t,~h,~s,~s_q} 
    \qquad \forall \{t\in T~ \vert ~h \in H ~ \vert ~ s \in S_H\{h\} ~ \vert ~ s_q \in S_Q\{h,~ s\}\}
    \end{align} 
    
For constraints :eq:`basin-volume-per-state-2`, the set :math:`B_H` is employed to link basin volumes with hydropower 
plants. This constraint involves the multiplication of a continuous variable with a binary variable. Consequently, 
a Big-M decomposition is required to linearize it.

2.5.4 Discrete hydro powerplants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: flow-state-2
    :nowrap:
    
    \begin{align}
    S_\text{BAS}^{t,B_H\{h\},~SB_H\{h,~s_h\}} \geq \sum_{s_q \in S_Q\{h,~s_h\}} S_\text{FLOW}^{t,~h,~s_h,~s_q} 
    \qquad \forall \{t\in T~\vert~h,\in H~\vert~s_h \in S_H\{h\} \}
    \end{align}
    
    
.. math::
    :label: powered-flow-2
    :nowrap:
    
    \begin{align}
    Q^{t,~h} = \sum_{s_h \in S_H\{h\}} \sum_{s_q \in S_Q\{h,~s_h\}} 
        \Big[ \left(Q_\text{MIN}^{h,~s_h,~s_q} - dQ^{h,~s_h,~s_q} \cdot  V_\text{BAS, MIN}^{B_H\{h\},~SB_H\{h,~s_h\}} \right) 
        \cdot S_\text{FLOW}^{t,~h,~s_h,~s_q} + dQ^{h,~s_h,~s_q} \cdot V_\text{BAS, S}^{t,~h,~s_h,~s_q} \Big]
    \qquad \forall \{t\in T~\vert~h \in H~\}
    \end{align}
    
.. math::
    :label: powered-power-2
    :nowrap:
    
    \begin{align}
    P^{t,~h} = \sum_{s_h \in S_H\{h\}} \sum_{s_q \in S_Q\{h,~s_h\}} 
        \Big[\left(P_\text{MIN}^{h,~s_h,~s_q} - dP^{h,~s_h,~s_q} \cdot  V_\text{BAS, MIN}^{B_H\{h\},~SB_H\{h,~s_h\}} \right) 
        \cdot S_\text{FLOW}^{t,~h,~s_h,~s_q} + dP^{h,~s_h,~s_q} \cdot V_\text{BAS, S}^{t,~h,~s_h,~s_q} \Big]
    \qquad \forall \{t\in T~\vert~h \in H~\}
    \end{align}
    
2.5.3 Powered water volume
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. math::
    :label: diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{+}^{h} - dV_\text{-}^{h} = V_\text{POW}^{h} + dV_\text{LAST}^{h} - 
        nb_\text{SEC} \cdot nb_\text{HOUR}  \cdot \sum_{t \in T} Q^{t,~h} \qquad \forall \{h\in H\}
    
    \end{align} 

.. math::
    :label: max_pos_diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{+}^{h} \leq \begin{cases} 
            V_\text{BUF}^{h} + dV_\text{LAST}^{h} / 2 \qquad &\text{if } dV_\text{LAST}^{h} \geq 0 \\
            V_\text{BUF}^{h} & \text{otherwise }
        \end{cases} \qquad \forall \{h\in H\}
    \end{align} 
    
.. math::
    :label: min_pos_diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{-}^{h} \leq \begin{cases} 
            V_\text{BUF}^{h} - dV_\text{LAST}^{h} / 2 \qquad &\text{if } dV_\text{LAST}^{h} \leq 0 \\
            V_\text{BUF}^{h} & \text{otherwise }
        \end{cases} \qquad \forall \{h\in H\}
    \end{align} 
"""       
def second_stage_baseline_objective(model):
    
    market_price = sum(
            model.nb_hours * sum(model.market_price[t] * model.hydro_power[t, h] for t in model.T)
            for h in model.H
        ) 
    
    ancillary_market_price = sum(
            4*model.ancillary_market_price[f]  * model.ancillary_power[f] for f in model.F
        )
    # powered_volume_penalty = sum(
    #         model.powered_volume_overage[h] * model.unpowered_factor_price_pos[h] -
    #         model.powered_volume_shortage[h] * model.unpowered_factor_price_neg[h] 
    #         for h in model.H
    #     ) / (model.nb_sec * model.volume_factor) 
    spilled_penalty = sum(
            sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b] 
            for b in model.B
        ) / (model.nb_sec * model.volume_factor)
    return market_price + ancillary_market_price - spilled_penalty + sum(model.powered_volume_penalty[h] for h in model.H)

def powered_volume_penalty_constraint(model, h):
    
    
    return (
        model.powered_volume_penalty[h] ==
        (model.powered_volume_overage[h] * model.unpowered_factor_price_pos[h] -
        model.powered_volume_shortage[h] * model.unpowered_factor_price_neg[h] 
    )/ (model.nb_sec * model.volume_factor) )
    

####################################################################################################################
### Basin volume evolution constraints #############################################################################  
#################################################################################################################### 

def basin_volume_evolution(model, t, b):
    if t == model.T.first():
        return model.basin_volume[t, b] == model.start_basin_volume[b]
    else:
        return model.basin_volume[t, b] == (
            model.basin_volume[t - 1, b] + model.discharge_volume[t - 1, b] - model.spilled_volume[t - 1, b] + 
            model.nb_hours * model.nb_sec * model.volume_factor *
            sum(model.water_factor[b, h] * model.flow[t - 1, h] for h in model.H
            )
        )

def basin_end_volume_constraint(model, b):
    t_max = model.T.last()
    return model.end_basin_volume[b] == (
        model.basin_volume[t_max, b] + model.discharge_volume[t_max, b] - model.spilled_volume[t_max, b] +
        model.nb_hours * model.nb_sec * model.volume_factor *
        sum(model.water_factor[b, h] * model.flow[t_max, h] for h in model.H)
    )

def basin_max_end_volume_constraint(model, b):
    return model.end_basin_volume[b] <= model.max_basin_volume[b, model.S_B[b].last()]

def basin_min_end_volume_constraint(model, b):
    return model.end_basin_volume[b] >= model.min_basin_volume[b, model.S_B[b].first()]
####################################################################################################################
### Basin volume boundary constraints used to determine the state of each basin ####################################
####################################################################################################################

def basin_max_state_constraint(model, t, b, s):
    return (
        model.basin_volume[t, b] <= model.max_basin_volume[b, s] +
        model.max_basin_volume[b, model.S_B[b].last()] * 
        (1 - model.basin_state[t, b, s])
    )

def basin_min_state_constraint(model, t, b, s):
    return model.basin_volume[t, b] >= model.min_basin_volume[b, s] * model.basin_state[t, b, s]

def basin_state_constraint(model, t, b):
    return sum(model.basin_state[t, b, s] for s in model.S_B[b]) == 1


def max_active_flow_by_state_constraint(model, t, h, s):

    return model.flow_by_state[t, h, s] <= model.big_m * model.discrete_hydro_on[t, h]


def max_inactive_flow_by_state_constraint(model, t, h, b, s):

    return (
        model.flow_by_state[t, h, s] >=
        model.max_flow[h, s] * model.basin_state[t, b, s] - model.big_m * (1 - model.discrete_hydro_on[t, h]) 
    )

    
def max_flow_by_state_constraint(model, t, h, b, s):
    return (
        model.flow_by_state[t, h, s] <=  model.max_flow[h, s] * model.basin_state[t, b, s]
    )

def flow_constraint(model, t, h):
    return (
        model.flow[t, h] ==
        sum(model.flow_by_state[t, h, s] for s in model.S_H[h])
    )

def hydro_power_constraint(model, t, h):
    return (
        model.hydro_power[t, h] ==
        sum(
            model.flow_by_state[t, h, s] *  model.alpha[h, s]
        for s in model.S_H[h])
    )

def positive_hydro_ancillary_power_constraint(model, t, f):
    
    # return model.ancillary_power[f] <= 0
        
    return (
        model.ancillary_power[f] <= 
        model.total_positive_flex_power - sum(model.hydro_power[t, h] for h in model.CH) 
    ) 

def negative_hydro_ancillary_power_constraint(model, t, f):
    
    return (
        model.ancillary_power[f] <= 
        model.total_negative_flex_power + sum(model.hydro_power[t, h] for h in model.CH) 
    ) 


def diff_volume_constraint(model, h):
    return (
        model.powered_volume_overage[h] - model.powered_volume_shortage[h] == 
        sum(model.flow[t, h] for t in model.T) * model.nb_hours * model.nb_sec * model.volume_factor - 
        model.powered_volume[h]
    )

    
def max_powered_volume_quota_constraint(model, h):
    return (
        model.powered_volume_overage[h] <= model.overage_volume_buffer[h]
    )

    
def min_powered_volume_quota_constraint(model, h):

    return (
        model.powered_volume_shortage[h] <= model.shortage_volume_buffer[h]
    )

    