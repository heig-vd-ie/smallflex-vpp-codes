r"""
1.5.1 Objective
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: first-objective
    :nowrap:
    
    \begin{align}
        \max \sum_{t~\in~T} nb_\text{HOUR}^{t} \cdot \lbrack 
        c_\text{FLEX}^{t} \cdot  P_\text{ANC}^{t} +
        \sum_{h~\in~H} c_\text{DA}^{t} \cdot  P_\text{HYDRO}^{t,~h}
        
        \rbrack
    \end{align}


1.5.2. Water basin volume evolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-volume-evolution
    :nowrap:
        
    \begin{align}
        V_\text{BAS}^{t,~b} =
        \begin{cases} 
            V_\text{START}^{b} & \text{if } t = t_0 \\
            V_\text{BAS}^{t - 1,~b} + V_\text{DIS}^{t - 1,~b} - V_\text{SPIL}^{t - 1,~b} + 
            nb_\text{SEC} \cdot nb_\text{HOUR}^{t-1} \cdot 
            \sum_{h~\in~H} F_\text{HYDRO}^{b,~h} \cdot Q^{t-1,~h}
            \quad & \text{if } t \neq t_0
        \end{cases} \qquad \forall \{t\in T, b \in B \}
    \end{align}

.. math::
    :label: basin-end-volume    
    :nowrap:
    
    \begin{align}
    V_\text{START}^{b} = V_\text{BAS}^{t_{end},~b} + V_\text{DIS}^{t_{end},~b}  - V_\text{SPIL}^{t_{end},~b} + 
    nb_\text{SEC} \cdot nb_\text{HOUR}^{t_{end}} \cdot
        \sum_{h~\in~H} F^{b,~h} \cdot Q^{t_{end},~h} \qquad \forall \{b \in B \}
    \end{align} 

    
1.5.3. Water basin state
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-max-state
    :nowrap:
    
    \begin{align}
        V_\text{BAS}^{t,~b} \leq V_\text{MAX}^{b,~s} +  V_\text{MAX}^{b,~S_B^\text{END}\{b\}} 
        \cdot \left(1 -State^{t,~b,~s} \right)
    \qquad \forall \{t\in T~\vert~(b,~s) \in BS \}
    
    \end{align} 

.. math::
    :label: basin-min-state
    :nowrap:
    
    \begin{align}
        V_\text{BAS}^{t,~b} \geq V_\text{MIN}^{b,~s} \cdot State^{t,~b,~s}
        \qquad \forall \{t\in T~\vert~b \in B \}
    \end{align}
.. math::
    :label: basin-total-state
    :nowrap:
    
    \begin{align}
        \sum_{s~\in~S_B\{b\}} State^{t,~b,~s} = 1 \qquad \forall \{t\in T~\vert~b \in B \}
    \end{align}

1.5.4. Hydropower plants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: flow-state
    :nowrap:
    
    \begin{align}
        Q_\text{S}^{t,~h,~s} \leq Q_\text{MAX}^{h,~s} \cdot State^{t,~b,~s}
        \qquad \forall \{t\in T~\vert~(h,~b,~s) \in HBS \}
    \end{align}
    
.. math::
    :label: flow
    :nowrap:
    
    \begin{align}
        Q^{t,~h} = \sum_{s~\in~S_H\{h\}} Q_\text{S}^{t,~h,~s} 
        \qquad \forall \{t\in T~\vert~h \in H~\}
    \end{align}
    
.. math::
    :label: hydro-power
    :nowrap:
    
    \begin{align}
        P_\text{HYDRO}^{t,~h} = \sum_{s~\in~S_H\{h\}} \alpha^{h,~s} \cdot  Q_\text{S}^{t,~h,~s}
        \qquad \forall \{t\in T~\vert~h \in H\}
    \end{align}

1.5.5. Ancillary services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: positive-hydro-ancillary-power
    :nowrap:
    
    \begin{align}
        P_\text{ANC}^{t} \leq \sum_{b,~s~\in~BS} P_\text{FLEX +}^{~s} \cdot State^{t,~b,~s} - 
        \sum_{h~\in~CH} P_\text{HYDRO}^{t,~h} \qquad \forall \{t\in T\}
    \end{align}
    
.. math::
    :label: hydro-power
    :nowrap:
    
    \begin{align}
        P_\text{ANC}^{t} \leq \sum_{b,~s~\in~BS} P_\text{FLEX -}^{~s} \cdot State^{t,~b,~s} + 
        \sum_{h~\in~CH} P_\text{HYDRO}^{t,~h}\qquad \forall \{t\in T~\}
    \end{align}

"""
########################################################################################################################
# 1.5.1 Water basin volume evolution ###################################################################################
########################################################################################################################

def first_stage_baseline_objective(model):
    market_price = sum(
        model.market_price[t] * model.nb_hours[t] * 
        sum(model.hydro_power[t, h] for h in model.H) for t in model.T
    )
    ancillary_market_price = sum(
        model.ancillary_market_price[t] * model.nb_hours[t] *
        model.hydro_ancillary_reserve[t]  for t in model.T
    )
    
    spilled_penalty = sum(
        sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b] 
        for b in model.B
    ) / (model.nb_sec)
        
    return market_price + ancillary_market_price - spilled_penalty

########################################################################################################################
# 1.5.2 Water basin volume evolution ###################################################################################
########################################################################################################################

def basin_volume_evolution(model, t, b):
    if t == model.T.first():
        return model.basin_volume[t, b] == model.start_basin_volume[b]
    else:
        return model.basin_volume[t, b] == (
            model.basin_volume[t - 1, b] + model.discharge_volume[t - 1, b] - model.spilled_volume[t - 1, b] +
            model.nb_sec * model.nb_hours[t - 1] *
            sum(model.water_factor[b, h] * model.flow[t - 1, h] for h in model.H)
        )
        
def basin_end_volume_constraint(model, b):
    t_max = model.T.last()
    return model.start_basin_volume[b] == (
        model.basin_volume[t_max, b] + model.discharge_volume[t_max, b] - model.spilled_volume[t_max, b] +
        model.nb_sec * model.nb_hours[t_max] *
        sum(model.water_factor[b, h] * model.flow[t_max, h] for h in model.H)
    )

########################################################################################################################
# 1.5.3. Water basin state #############################################################################################
########################################################################################################################

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

########################################################################################################################
# 1.5.4. Hydropower plants #############################################################################################
########################################################################################################################

def max_flow_by_state(model, t, h, b, s):
    return (
        model.flow_by_state[t, h, s] <=
        model.max_powered_flow_ratio * model.max_flow[h, s] * model.basin_state[t, b, s]
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


########################################################################################################################
# 1.5.5. Ancillary services ############################################################################################
########################################################################################################################

def positive_hydro_hydro_ancillary_reserve_constraint(model, t):
    return (
        model.hydro_ancillary_reserve[t] <=
        sum(model.total_positive_flex_power[s] * model.basin_state[t, b, s] for b, s in model.BS) - sum(model.hydro_power[t, h] for h in model.CH)
    ) 

def negative_hydro_hydro_ancillary_reserve_constraint(model, t):
    return (
        model.hydro_ancillary_reserve[t] <=
        sum(model.total_negative_flex_power[s] * model.basin_state[t, b, s] for b, s in model.BS) + sum(model.hydro_power[t, h] for h in model.CH)
    ) 