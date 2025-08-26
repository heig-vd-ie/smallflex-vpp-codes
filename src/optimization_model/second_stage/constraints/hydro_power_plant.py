r"""
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

"""
import pyomo.environ as pyo

def hydro_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    model.max_active_flow_by_state_constraint = pyo.Constraint(model.T, model.HS, rule=max_active_flow_by_state_constraint)
    model.max_inactive_flow_by_state_constraint = pyo.Constraint(model.T, model.S_BH, rule=max_inactive_flow_by_state_constraint)
    model.max_flow_by_state_constraint = pyo.Constraint(model.T, model.S_BH, rule=max_flow_by_state_constraint)
    model.flow_constraint = pyo.Constraint(model.T, model.H, rule=flow_constraint)
    model.hydro_power_constraint = pyo.Constraint(model.T, model.H, rule=hydro_power_constraint)
    model.positive_hydro_ancillary_power_constraint = pyo.Constraint(model.TF, model.CH, rule=positive_hydro_ancillary_power_constraint)
    model.negative_hydro_ancillary_power_constraint = pyo.Constraint(model.TF, model.CH, rule=negative_hydro_ancillary_power_constraint)

    return model

def max_active_flow_by_state_constraint(model, t, h, s_h):
    if h in model.DH:
        return model.flow_by_state[t, h, s_h] <= model.big_m * model.active_hydro[t, h]
    else:
        return pyo.Constraint.Skip

def max_inactive_flow_by_state_constraint(model, t, h, b, s_h, s_b):
    if h in model.DH:
        return (
            model.flow_by_state[t, h, s_h] >=
            model.max_flow[h, s_h] * model.basin_state[t, b, s_b] - model.big_m * (1- model.active_hydro[t, h]) 
        )
    else:
        return pyo.Constraint.Skip
    
def max_flow_by_state_constraint(model, t, h, b, s_h, s_b):
    return (
        model.flow_by_state[t, h, s_h] <=  model.max_flow[h, s_h] * model.basin_state[t, b, s_b]
    )

def flow_constraint(model, t, h):
    return (
        model.flow[t, h] ==
        sum(model.flow_by_state[t, h, s_h] for s_h in model.S_H[h])
    )

def hydro_power_constraint(model, t, h):
    return (
        model.hydro_power[t, h] ==
        sum(
            model.flow_by_state[t, h, s_h] *  model.alpha[h, s_h]
        for s_h in model.S_H[h])
    )

def positive_hydro_ancillary_power_constraint(model, t, f, h):
    
    return (
        model.hydro_power[t, h] + model.ancillary_power[f, h] <= model.max_power[h]
    )

def negative_hydro_ancillary_power_constraint(model, t, f, h):
    
    return model.hydro_power[t, h] - model.ancillary_power[f, h] >= 0


