"""
Water turbined
~~~~~~~~~~~~~~~

.. math::
    :label: turbined-flow-state
    :nowrap:
    
    \\begin{align}
    Q_\\text{TUR, S}^{t,~h,~s\_h} \leq Q_\\text{TUR, MAX}^{h,~s\_h} \cdot S_\\text{BAS}^{t,~b,~s\_b}
    \\end{align}
.. math::
    :label: turbined-flow
    :nowrap:
    
    \\begin{align}
    Q_\\text{TUR}^{t,~h} = \sum_{s \in S\_H\{h\}} Q_\\text{TUR, S}^{t,~h,~s} 
    \\end{align}
    
.. math::
    :label: turbined-power
    :nowrap:
    
    \\begin{align}
    P_\\text{TUR}^{t,~h} = \sum_{s \in S\_H\{h\}} \\alpha_\\text{TUR, AVG}^{h,~s} \cdot  Q_\\text{TUR, S}^{t,~h,~s}
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
            model.turbined_flow_by_state[t, h, s_h] <= model.min_flow_turbined[h, s_h] * model.basin_state[t, b, s_b]
        ) 
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_flow_constraint(model, t, h):
        return (
            model.turbined_flow[t, h] ==
            sum(model.turbined_flow_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
    
    @model.Constraint(model.T,model.S_BH) # type: ignore
    def alpha_turbined_by_state_constraint(model, t, h, b, s_h, s_b):
        return (
            model.turbined_alpha_by_state[t, h, s_h] ==
            model.d_alpha_turbined[h, s_h] * model.basin_volume_by_state[t, b, s_b]  + model.basin_state[t, b, s_b] * 
            (model.min_alpha_turbined[h, s_h] - model.d_alpha_turbined[h, s_h] * model.min_basin_volume[b, s_b])
        ) 
        
    @model.Constraint(model.T, model.H) # type: ignore
    def alpha_turbined_constraint(model, t, h):
        return (
            model.turbined_alpha[t, h] ==
            sum(model.turbined_alpha_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
        
    return model


def turbine_power_1_constraints(model):
    @model.Constraint(model.T, model.HS) # type: ignore
    def turbined_power_by_state_constraint(model, t, h, s_h):
        return (
            model.turbined_power_by_state[t, h, s_h] == 
            model.turbined_flow_by_state[t, h, s_h] * 
            (model.max_alpha_turbined[h, s_h] + model.min_alpha_turbined[h, s_h]) / 2
        ) 
    
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_power_constraint(model, t, h):
        return (
            model.turbined_power[t, h] ==
            sum(model.turbined_power_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
    return model

def turbine_power_2_constraints(model):
    ####################################################################################################################
    ## Turbined power constraints ######################################################################################
    #################################################################################################################### 
    @model.Constraint(model.T, model.HS) # type: ignore
    def turbined_power_max_constraint(model, t, h, s_h):
        return (
            model.turbined_power_by_state[t, h, s_h] >= 
            model.turbined_alpha_by_state[t, h, s_h] * model.min_flow_turbined[h, s_h] + 
            model.turbined_flow_by_state[t, h, s_h] * model.max_alpha_turbined[h, s_h] 
            - model.min_flow_turbined[h,s_h] * model.max_alpha_turbined[h, s_h]
        ) 

    @model.Constraint(model.T, model.HS) # type: ignore
    def turbined_power_min_max_constraint(model, t, h, s_h):
        return (
            model.turbined_power_by_state[t, h, s_h] <= 
            model.turbined_flow_by_state[t, h, s_h] * model.max_alpha_turbined[h, s_h] 
        ) 
    
    @model.Constraint(model.T, model.HS) # type: ignore
    def turbined_power_max_min_constraint(model, t, h, s_h):
        return (
            model.turbined_power_by_state[t, h, s_h] <= 
            model.turbined_alpha_by_state[t, h, s_h] * model.min_flow_turbined[h, s_h]
        )  
    @model.Constraint(model.T, model.H) # type: ignore
    def turbined_power_constraint(model, t, h):
        return (
            model.turbined_power[t, h] ==
            sum(model.turbined_power_by_state[t, h, s_h] for s_h in model.S_h[h])
        ) 
    return model