r"""
1.5.4. Water pumped
~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: pumped-flow-state
    :nowrap:
    
    \begin{align}
        Q_\text{PUM, S}^{t,~h,~s_h} \leq Q_\text{PUM, MAX}^{h,~s_h} \cdot S_\text{BAS}^{t,~b,~s_b}
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

def hydro_power_plant_constraints(model):
    ####################################################################################################################
    ### basin volume per state constraints used to determine the state of each basin ###################################
    ####################################################################################################################
    @model.Constraint(model.T, model.S_BH) # type: ignore
    def flow_by_state_constraint(model, t, h, b, s_h, s_b):
        return (
            model.flow_by_state[t, h, s_h] <=
            model.max_turbined_volume_factor * model.max_flow[h, s_h] * model.basin_state[t, b, s_b]
        ) 
    @model.Constraint(model.T, model.H) # type: ignore
    def flow_constraint(model, t, h):
        return (
            model.flow[t, h] ==
            sum(model.flow_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
    
    @model.Constraint(model.T, model.H) # type: ignore
    def hydro_power_constraint(model, t, h):
        return (
            model.hydro_power[t, h] ==
            sum(
                model.flow_by_state[t, h, s_h] *  model.alpha[h, s_h]
            for s_h in model.S_h[h])
        )
    
    @model.Constraint(model.T, model.CH) # type: ignore
    def positive_hydro_ancillary_power_constraint(model, t, h):
        
        return (
            model.hydro_power[t, h] + model.ancillary_power[t, h] <= model.max_power[h]
        )
    
        
    @model.Constraint(model.T, model.CH) # type: ignore
    def negative_hydro_ancillary_power_constraint(model, t, h):
        
        return model.hydro_power[t, h] - model.ancillary_power[t, h] >= 0

    return model


