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


def flow_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################

    @model.Constraint(model.T, model.HF) # type: ignore
    def flow_by_state_constraint(model, t, h, f):
        return (
            model.calculated_flow[t, h, f] ==
            sum(
                model.basin_state[t, b, s_b] * 
                (model.min_flow[h_1, s_h, f] - model.d_flow[h_1, s_h, f] * model.min_basin_volume[b, s_b]) +
                model.d_flow[h_1, s_h, f] * 
                model.basin_volume_by_state[t, b, s_b] 
                for h_1, b, s_b, s_h in model.S_BH if h_1 == h
            )
        ) 
        
    @model.Constraint(model.T, model.H) # type: ignore
    def flow_state_constraint(model, t, h):
        return sum(model.flow_state[t, h, f] for f in model.F[h]) == 1

    @model.Constraint(model.T, model.HF) # type: ignore
    def flow_state_max_inactive_constraint(model, t, h, f):
        return model.flow_by_state[t, h, f] <= model.big_m * model.flow_state[t, h, f]
    
    @model.Constraint(model.T, model.HF) # type: ignore
    def flow_state_min_inactive_constraint(model, t, h, f):
        return model.flow_by_state[t, h, f] >= - model.big_m * model.flow_state[t, h, f]
    
    @model.Constraint(model.T, model.HF) # type: ignore
    def flow_state_max_active_constraint(model, t, h, f):
        return(
            model.flow_by_state[t, h, f] >= 
            model.calculated_flow[t, h, f] -
            model.big_m * (1 - model.flow_state[t, h, f])
        )
    
    @model.Constraint(model.T, model.HF) # type: ignore
    def flow_state_min_active_constraint(model, t, h, f):
        return(
            model.flow_by_state[t, h, f] <= 
            model.calculated_flow[t, h, f] +
            model.big_m * (1 - model.flow_state[t, h, f])
        )
        
    @model.Constraint(model.T, model.H) # type: ignore
    def flow_constraint(model, t, h):
        return(
            model.flow[t, h] == sum(model.flow_by_state[t, h, f] for f in model.F[h])
        )
        

    return model