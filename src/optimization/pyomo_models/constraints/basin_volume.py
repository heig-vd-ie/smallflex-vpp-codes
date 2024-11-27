"""
Water basin volume evolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-volume-evolution
    
    \\begin{align}
        V_\\text{BAS}^{t,~b} =
        \\begin{cases} 
            &V_\\text{BAS, START}^{b} & \\text{if } t = t_0 \\\\
            &V_\\text{BAS}^{t - 1,~b} - V_\\text{SPIL}^{t - 1,~b} + nb_\\text{SEC} \cdot nb_\\text{HOUR}^{t-1} \cdot 
            \sum_{h \in H} \left( 
                F_\\text{TUR}^{b,~h} \cdot Q_\\text{TUR}^{t-1,~h} + 
                F_\\text{PUM}^{b,~h} \cdot Q_\\text{PUM}^{t-1,~h} 
            \\right) \quad & \\text{if } t \\neq t_0
        \end{cases}
    \\end{align}

.. math::
    :label: basin-end-volume
    
    V_\\text{BAS, START}^{b} = V_\\text{BAS}^{t_{end},~b} - V_\\text{SPIL}^{t_{end},~b} + 
    nb_\\text{SEC} \cdot nb_\\text{HOUR}^{t_{end}} \cdot
        \sum_{h \in H} \left(
            F_\\text{TUR}^{b,~h} \cdot Q_\\text{TUR}^{t_{end},~h} +
            F_\\text{PUM}^{b,~h} \cdot Q_\\text{PUM}^{t_{end},~h}
        \\right)

    
Water basin state
~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-max-state

    V_\\text{BAS}^{t,~b} \leq V_\\text{BAS, MAX}^{b,~s\_b} +  V_\\text{BAS, MAX}^{b,~S\_B_\\text{MAX}\{b\}} 
    \cdot \left(1 -S_\\text{BAS}^{t,~b,~s\_b} \\right)
  
.. math::
    :label: basin-min-state

    V_\\text{BAS}^{t,~b} \geq V_\\text{BAS, MIN}^{b,~s\_b} \cdot S_\\text{BAS}^{t,~b,~s\_b}

    
.. math::
    :label: basin-total-state

    \sum_{s \in S\_B\{b\}} S_\\text{BAS}^{t,~b,~s} = 1

    
.. math::
    :label: basin-volume-state

    V_\\text{BAS, S}^{t,~b,~s\_b} =  V_\\text{BAS}^{t,~b} \cdot S_\\text{BAS}^{t,~b,~s}

The constraint :eq:`basin-volume-state` involves the multiplication of a floating-point variable and a binary variable. To 
address this, the constraint requires the application of a *Big M Linearization* technique.
"""



def basin_volume_constraints(model):
    ####################################################################################################################
    ### Basin volume evolution constraints #############################################################################  
    #################################################################################################################### 
    @model.Constraint(model.T, model.B) # type: ignore
    def basin_volume_evolution(model, t, b):
        if t == model.T.first():
            return model.basin_volume[t, b] == model.start_basin_volume[b]
        else:
            return model.basin_volume[t, b] == (
                model.basin_volume[t - 1, b] + model.discharge_volume[t - 1, b] - model.spilled_volume[t - 1, b]
                + sum(
                    model.nb_hours[t - 1] * 3600 * (
                    model.water_pumped_factor[b, h] * model.pumped_flow[t - 1, h]  +
                    model.water_turbined_factor[b, h] * model.turbined_flow[t - 1, h]
                    ) for h in model.H
                )
            )

    @model.Constraint(model.B) # type: ignore
    def basin_end_volume_constraint(model, b):
        t_max = model.T.last()
        return model.start_basin_volume[b] == (
            model.basin_volume[t_max, b] + model.discharge_volume[t_max, b] - model.spilled_volume[t_max, b]
            +sum(
                model.nb_hours[t_max] * 3600 * (
                model.water_pumped_factor[b, h] * model.pumped_flow[t_max, h] +
                model.water_turbined_factor[b, h] * model.turbined_flow[t_max, h]
                ) for h in model.H
            )
        )
    ####################################################################################################################
    ### Basin volume boundary constraints used to determine the state of each basin ####################################
    ####################################################################################################################
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_max_state_constraint(model, t, b, s_b):
        return (
            model.basin_volume[t, b] <= model.max_basin_volume[b, s_b] +
            model.max_basin_volume[b, model.S_b[b].last()] * 
            (1 - model.basin_state[t, b, s_b])
        )

    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_min_state_constraint(model, t, b, s_b):
        return model.basin_volume[t, b] >= model.min_basin_volume[b, s_b] * model.basin_state[t, b, s_b]

    @model.Constraint(model.T, model.B) # type: ignore
    def basin_state_constraint(model, t, b):
        return sum(model.basin_state[t, b, s] for s in model.S_b[b]) == 1

    ####################################################################################################################
    ### Basin volume state constraints used to determine the state of each basin #######################################
    ####################################################################################################################

    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_state_max_active_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] <= 
            model.max_basin_volume[b, model.S_b[b].last()] * model.basin_state[t, b, s_b]
        )
        
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_state_max_inactive_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] >= 
            model.basin_volume[t, b] -
            model.max_basin_volume[b, model.S_b[b].last()] * (1 - model.basin_state[t, b, s_b])
        )
    
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_state_min_active_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] >= 
            model.min_basin_volume[b, model.S_b[b].first()] * model.basin_state[t, b, s_b]
        )
        

        
    @model.Constraint(model.T, model.BS) # type: ignore
    def basin_state_min_inactive_constraint(model, t, b, s_b):
        return (
            model.basin_volume_by_state[t, b, s_b] <= 
            model.basin_volume[t, b] - 
            model.min_basin_volume[b, model.S_b[b].first()] * (1 - model.basin_state[t, b, s_b])
        )
        
    return model