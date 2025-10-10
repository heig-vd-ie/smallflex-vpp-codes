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


def objective(model):
    market_price = sum(
        sum(
            model.market_price[t, ω]
            * model.nb_hours[t]
            * sum(model.hydro_power[t, h] for h in model.H)
            for t in model.T
        )
        for ω in model.Ω
    )
    ancillary_market_price = sum(
        sum(
            model.ancillary_market_price[t, ω]
            * model.nb_hours[t]
            * model.hydro_ancillary_reserve[t]
            for t in model.T
        )
        for ω in model.Ω
    )
    basin_volume_penalty = sum(
        sum(
            model.end_basin_volume_overage[b, ω] * model.unpowered_factor_price_pos[ω, b]
            - model.end_basin_volume_shortage[b, ω] * model.unpowered_factor_price_neg[ω, b]
            for b in model.B
        )/model.nb_sec
        for ω in model.Ω
    )
    spilled_volume_penalty = sum(
        sum(
            sum(model.spilled_volume[t, ω, b] * model.unpowered_factor_price_neg[ω, b] for t in model.T)
            for b in model.B
        )/model.nb_sec
        for ω in model.Ω
    )

    return market_price + ancillary_market_price - spilled_volume_penalty + basin_volume_penalty


########################################################################################################################
# 1.5.2 Water basin volume evolution ###################################################################################
########################################################################################################################


def basin_volume_evolution(model, t, ω, b):
    if t == model.T.first():
        return model.basin_volume[t, ω, b] == model.start_basin_volume[b]
    else:
        return model.basin_volume[t, ω, b] == (
            model.basin_volume[t - 1, ω, b]
            + model.discharge_volume[t - 1, ω, b]
            - model.spilled_volume[t - 1, ω, b]
            + model.nb_sec
            * model.nb_hours[t - 1]
            * sum(model.water_factor[b, h] * model.flow[t - 1, h] for h in model.H)
        )


def basin_end_volume_constraint(model, ω, b):
    t_max = model.T.last()
    return model.start_basin_volume[b] + model.end_basin_volume_overage[b, ω] - model.end_basin_volume_shortage[b, ω] == (
        model.basin_volume[t_max, ω, b]
        + model.discharge_volume[t_max, ω, b]
        - model.spilled_volume[t_max, ω, b]
        + model.nb_sec
        * model.nb_hours[t_max]
        * sum(model.water_factor[b, h] * model.flow[t_max, h] for h in model.H)
    )


########################################################################################################################
# 1.5.3. Water basin state #############################################################################################
########################################################################################################################


def basin_volume_max_constraint(model, t, ω, b):
    return (
        model.basin_volume[t, ω, b] <= model.max_basin_volume[b]
    )

def basin_volume_min_constraint(model, t, ω, b):
    return model.basin_volume[t, ω, b] >= model.min_basin_volume[b]


########################################################################################################################
# 1.5.4. Hydropower plants #############################################################################################
########################################################################################################################


def max_flow_constraint(model, t, h):
    return (
        model.flow[t, h] <= model.max_powered_flow_ratio * model.max_flow[h]
    )



def hydro_power_constraint(model, t, h):
    return model.hydro_power[t, h] == model.flow[t, h] * model.alpha[h]


########################################################################################################################
# 1.5.5. Ancillary services ############################################################################################
########################################################################################################################
def positive_hydro_hydro_ancillary_reserve_constraint(model, t):
    return model.hydro_ancillary_reserve[t] <= model.total_positive_flex_power - sum(model.hydro_power[t, h] for h in model.CH)

def negative_hydro_hydro_ancillary_reserve_constraint(model, t):
    return model.hydro_ancillary_reserve[t] <= model.total_negative_flex_power + sum(model.hydro_power[t, h] for h in model.CH)