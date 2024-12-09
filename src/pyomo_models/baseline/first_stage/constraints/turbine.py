"""
Water turbined
~~~~~~~~~~~~~~~

.. math::
    :label: turbined-flow-state
    :nowrap:
    
    \\begin{align}
    Q_\\text{TUR, S}^{t,~h,~s_h} \leq Q_\\text{TUR, MAX}^{h,~s_h} \cdot S_\\text{BAS}^{t,~b,~s_b}
    \qquad \\forall \{t\in T~\\vert~b,~h,~s_h ~s_h \in S_{BH} \}
    \\end{align}
.. math::
    :label: turbined-flow
    :nowrap:
    
    \\begin{align}
    Q_\\text{TUR}^{t,~h} = \sum_{s \in S_H\{h\}} Q_\\text{TUR, S}^{t,~h,~s} 
    \qquad \\forall \{t\in T~\\vert~h \in H~\}
    \\end{align}
    
.. math::
    :label: turbined-power
    :nowrap:
    
    \\begin{align}
    P_\\text{TUR}^{t,~h} = \sum_{s \in S_H\{h\}} \\alpha_\\text{TUR, AVG}^{h,~s} \cdot  Q_\\text{TUR, S}^{t,~h,~s}
    \qquad \\forall \{t\in T~\\vert~h \in H\}
    \\end{align}

The constraint :eq:`turbined-flow-state` takes the set :math:`S\_BH` as argument, enabling the connection between the 
basin set :math:`B` and the hydro powerplant set :math:`H`.
"""

def turbine_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    @model.Constraint(model.T, model.S_BH) # type: ignore
    def turbined_flow_by_state_constraint(model, t, h, b, s_h, s_b):
        return (
            model.turbined_flow_by_state[t, h, s_h] <= 
            model.turbine_factor * model.max_flow_turbined[h, s_h] * model.basin_state[t, b, s_b]
        ) 
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_flow_constraint(model, t, h):
        return (
            model.turbined_flow[t, h] ==
            sum(model.turbined_flow_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
    
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_power_constraint(model, t, h):
        return (
            model.turbined_power[t, h] ==
            sum(
                model.turbined_flow_by_state[t, h, s_h] *  model.alpha_turbined[h, s_h]
            for s_h in model.S_h[h])
        )
    return model
