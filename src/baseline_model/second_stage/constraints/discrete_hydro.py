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

def discrete_hydro_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    model.max_active_flow_by_state_constraint = pyo.Constraint(model.T, model.HS, rule=max_active_flow_by_state_constraint)
    model.max_inactive_flow_by_state_constraint = pyo.Constraint(model.T, model.S_BH, rule=max_inactive_flow_by_state_constraint)
    model.min_inactive_flow_by_state_constraint = pyo.Constraint(model.T, model.S_BH, rule=min_inactive_flow_by_state_constraint)
    model.flow_constraint = pyo.Constraint(model.T, model.H, rule=flow_constraint)
    model.hydro_power_constraint = pyo.Constraint(model.T, model.H, rule=hydro_power_constraint)

    return model

def max_active_flow_by_state_constraint(model, t, h, s_h):
    return model.flow_by_state[t, h, s_h] <= model.big_m * model.active_hydro[t, h] 


def max_inactive_flow_by_state_constraint(model, t, h, b, s_h, s_b):
    return (
        model.flow_by_state[t, h, s_h] >= 
        model.max_flow[h, s_h] * model.basin_state[t, b, s_b] - model.big_m * (1- model.active_hydro[t, h]) 
    )

def min_inactive_flow_by_state_constraint(model, t, h, b, s_h, s_b):
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




# def flow_state_constraint(model, t, h, s_h):
#     b = model.B_H[h].first()
#     s_b = model.SB_H[h, s_h].first()
#     return (
#         sum(model.flow_state[t, h, s_h, s_q] for s_q in model.S_Q[h, s_h]) <= model.basin_state[t, b, s_b]
#     )

# def discrete_flow_constraint(model, t, h):
#     b = model.B_H[h].first()
#     return (
#         model.flow[t, h] ==
#         sum(
#             sum(
#                 model.flow_state[t, h, s_h, s_q] * 
#                 (model.min_flow[h, s_h, s_q] - model.d_flow[h, s_h, s_q] * model.min_basin_volume[b, model.SB_H[h, s_h].first()]) +
#                 model.d_flow[h, s_h, s_q] * model.basin_volume_by_state[t, h, s_h, s_q] 
#             for s_q in model.S_Q[h, s_h])
#         for s_h in model.S_H[h])
#     ) 


# def discrete_power_constraint(model, t, h):
#     b = model.B_H[h].first()
#     return (
#         model.power[t, h] ==
#         sum(
#             sum(
#                 model.flow_state[t, h, s_h, s_q] * 
#                 (model.min_power[h, s_h, s_q] - model.d_power[h, s_h, s_q] * model.min_basin_volume[b, model.SB_H[h, s_h].first()]) +
#                 model.d_power[h, s_h, s_q] * model.basin_volume_by_state[t, h, s_h, s_q] 
#             for s_q in model.S_Q[h, s_h])
#         for s_h in model.S_H[h])
#     ) 
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