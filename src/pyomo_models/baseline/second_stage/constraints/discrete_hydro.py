"""
Water pumped
~~~~~~~~~~~~~~~

.. math::
    :label: pumped-flow-state
    :nowrap:
    
    \\begin{align}
    Q_\\text{PUM, S}^{t,~h,~s_h} \leq Q_\\text{PUM, MAX}^{h,~s_h} \cdot S_\\text{BAS}^{t,~b,~s_b}
    \qquad \\forall \{t\in T~\\vert~b,~h,~s_h ~s_h \in S_{BH} \}
    \\end{align}
.. math::
    :label: pumped-flow
    :nowrap:
    
    \\begin{align}
    Q_\\text{PUM}^{t,~h} = \sum_{s \in S_H\{h\}} Q_\\text{PUM, S}^{t,~h,~s} 
    \qquad \\forall \{t\in T~\\vert~h \in H~\}
    \\end{align}
    
.. math::
    :label: pumped-power
    :nowrap:
    
    \\begin{align}
    P_\\text{PUM}^{t,~h} = \sum_{s \in S_H\{h\}} \\alpha_\\text{PUM, AVG}^{h,~s} \cdot  Q_\\text{PUM, S}^{t,~h,~s}
    \qquad \\forall \{t\in T~\\vert~h \in H\}
    \\end{align}

The constraint :eq:`pumped-flow-state` takes the set :math:`S\_BH` as argument, enabling the connection between the 
basin set :math:`B` and the hydro powerplant set :math:`H`.
"""
import pyomo.environ as pyo

def discrete_hydro_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    model.flow_state_constraint = pyo.Constraint(model.T, model.HS, rule=flow_state_constraint)
    model.discrete_flow_constraint = pyo.Constraint(model.T, model.H, rule=discrete_flow_constraint)
    model.discrete_power_constraint = pyo.Constraint(model.T, model.H, rule=discrete_power_constraint)
    return model

def flow_state_constraint(model, t, h, s_h):
    b = model.B_H[h].first()
    s_b = model.SB_H[h, s_h].first()
    return (
        sum(model.flow_state[t, h, s_h, s_q] for s_q in model.S_Q[h, s_h]) <= model.basin_state[t, b, s_b]
    )

def discrete_flow_constraint(model, t, h):
    b = model.B_H[h].first()
    return (
        model.flow[t, h] ==
        sum(
            sum(
                model.flow_state[t, h, s_h, s_q] * 
                (model.min_flow[h, s_h, s_q] - model.d_flow[h, s_h, s_q] * model.min_basin_volume[b, model.SB_H[h, s_h].first()]) +
                model.d_flow[h, s_h, s_q] * model.basin_volume_by_state[t, h, s_h, s_q] 
            for s_q in model.S_Q[h, s_h])
        for s_h in model.S_H[h])
    ) 


def discrete_power_constraint(model, t, h):
    b = model.B_H[h].first()
    return (
        model.power[t, h] ==
        sum(
            sum(
                model.flow_state[t, h, s_h, s_q] * 
                (model.min_power[h, s_h, s_q] - model.d_power[h, s_h, s_q] * model.min_basin_volume[b, model.SB_H[h, s_h].first()]) +
                model.d_power[h, s_h, s_q] * model.basin_volume_by_state[t, h, s_h, s_q] 
            for s_q in model.S_Q[h, s_h])
        for s_h in model.S_H[h])
    ) 
    # @model.Constraint(model.T, model.HQS) # type: ignore
    # def flow_state_max_inactive_constraint(model, t, h, s_h, s_q):
    #     return model.flow_by_state[t, h, s_h, s_q] <= model.big_m * model.flow_state[t, h, s_h, s_q]
    
    # @model.Constraint(model.T, model.HQS) # type: ignore
    # def flow_state_min_inactive_constraint(model, t, h, s_h, s_q):
    #     return model.flow_by_state[t, h, s_h, s_q] >= - model.big_m * model.flow_state[t, h, s_h, s_q]
    
    # @model.Constraint(model.T, model.HQS) # type: ignore
    # def flow_state_max_active_constraint(model, t, h, s_h, s_q):
    #     return(
    #         model.flow_by_state[t, h, s_h, s_q] >= 
    #         model.calculated_flow[t, h, s_h, s_q] -
    #         model.big_m * (1 - model.flow_state[t, h, s_h, s_q])
    #     )
    
    # @model.Constraint(model.T, model.HQS) # type: ignore
    # def flow_state_min_active_constraint(model, t, h, s_h, s_q):
    #     return(
    #         model.flow_by_state[t, h, s_h, s_q] <= 
    #         model.calculated_flow[t, h, s_h, s_q] +
    #         model.big_m * (1 - model.flow_state[t, h, s_h, s_q])
    #     )
        
    # @model.Constraint(model.T, model.H) # type: ignore
    # def flow_constraint(model, t, h):
    #     return(
    #         model.flow[t, h] == 
    #         sum(
    #             sum(model.flow_by_state[t, h, s_h, s_q] for s_q in model.S_Q[h, s_h])
    #         for s_h in model.S_H[h])
    #     )
    
    return model