"""
Water pumped
~~~~~~~~~~~~~~~

.. math::
    :label: pumped-power-state
    :nowrap:
    
    \\begin{align}
    Q_\\text{PUM, S}^{t,~h,~s_h} \leq Q_\\text{PUM, MAX}^{h,~s_h} \cdot S_\\text{BAS}^{t,~b,~s_b}
    \qquad \\forall \{t\in T~\\vert~b,~h,~s_h ~s_h \in S_{BH} \}
    \\end{align}
.. math::
    :label: pumped-power
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

The constraint :eq:`pumped-power-state` takes the set :math:`S\_BH` as argument, enabling the connection between the 
basin set :math:`B` and the hydro powerplant set :math:`H`.
"""


def power_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    @model.Constraint(model.T, model.H) # type: ignore
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
    # def power_by_state_constraint(model, t, h, s_h, s_q):
    #     b = model.B_H[h].first()
    #     s_b = model.SB_H[h, s_h].first()
    #     return (
    #         model.calculated_power[t, h, s_h, s_q] ==
    #         model.basin_state[t, b, s_b] * 
    #         (model.min_power[h, s_h, s_q] - model.d_power[h, s_h, s_q] * model.min_basin_volume[b, s_b]) +
    #         model.d_power[h, s_h, s_q] * model.basin_volume_by_state[t, b, s_b] 
    #     ) 

    # @model.Constraint(model.T, model.HQS) # type: ignore
    # def power_state_max_inactive_constraint(model, t, h, s_h, s_q):
    #     return model.power_by_state[t, h, s_h, s_q] <= model.big_m * model.power_state[t, h, s_h, s_q]
    
    # @model.Constraint(model.T, model.HQS) # type: ignore
    # def power_state_min_inactive_constraint(model, t, h, s_h, s_q):
    #     return model.power_by_state[t, h, s_h, s_q] >= - model.big_m * model.power_state[t, h, s_h, s_q]
    
    # @model.Constraint(model.T, model.HQS) # type: ignore
    # def power_state_max_active_constraint(model, t, h, s_h, s_q):
    #     return(
    #         model.power_by_state[t, h, s_h, s_q] >= 
    #         model.calculated_power[t, h, s_h, s_q] -
    #         model.big_m * (1 - model.power_state[t, h, s_h, s_q])
    #     )
    
    # @model.Constraint(model.T, model.HQS) # type: ignore
    # def power_state_min_active_constraint(model, t, h, s_h, s_q):
    #     return(
    #         model.power_by_state[t, h, s_h, s_q] <= 
    #         model.calculated_power[t, h, s_h, s_q] +
    #         model.big_m * (1 - model.power_state[t, h, s_h, s_q])
    #     )
        
    # @model.Constraint(model.T, model.H) # type: ignore
    # def power_constraint(model, t, h):
    #     return(
    #         model.power[t, h] == 
    #         sum(
    #             sum(
    #                 model.power_by_state[t, h, s_h, s_q] 
    #                 for s_q in model.S_Q[h, s_h])
    #         for s_h in model.S_H[h])
    #     )
    return model