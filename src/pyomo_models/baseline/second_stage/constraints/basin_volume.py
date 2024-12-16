r"""
Water basin volume evolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-volume-evolution-2
    :nowrap:
        
    \begin{align}
        V_\text{BAS}^{t,~b} =
        \begin{cases} 
            V_\text{BAS, START}^{b} & \text{if } t = t_0 \\
            V_\text{BAS}^{t - 1,~b} + V_\text{DIS}^{t - 1,~b} - V_\text{SPIL}^{t - 1,~b} + 
            nb_\text{SEC} \cdot nb_\text{HOUR} \cdot 
            \sum_{h \in H} F_\text{TUR}^{b,~h} \cdot Q^{t-1,~h} 
            \quad & \text{if } t \neq t_0
        \end{cases} \qquad \forall \{t\in T, b \in B \}
    \end{align} 
    
.. math::
    :label: end-basin-volume-evolution-2
    :nowrap:
        
    \begin{align}
        V_\text{BAS, END}^{b} = V_\text{BAS}^{t_{end},~b} + V_\text{DIS}^{t_{end},~b}  - V_\text{SPIL}^{t_{end},~b} + 
        nb_\text{SEC} \cdot nb_\text{HOUR} \cdot \sum_{h \in H} F_\text{TUR}^{b,~h} \cdot Q^{t_{end},~h}
    \qquad \forall \{b \in B \}
    \end{align} 
    
.. math::
    :label: max-end-basin-volume-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS, END}^{b} &\leq V_\text{BAS, MAX}^{b,~S_B^\text{END}\{b\}} 
    \qquad \forall \{b \in B \}
    \end{align}

.. math::
    :label: min-end-basin-volume-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS, END}^{b} &\geq V_\text{BAS, MIN}^{b,~S_B^\text{0}\{b\}} 
    \qquad \forall \{b \in B \}
    \end{align}


    
Water basin state
~~~~~~~~~~~~~~~~~~

.. math::
    :label: max-basin-state-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS}^{t,~b} &\leq V_\text{BAS, MAX}^{b,~s_b} +  V_\text{BAS, MAX}^{b,~S_B^\text{END}\{b\}} 
    \cdot \left(1 -S_\text{BAS}^{t,~b,~s_b} \right)
    \qquad \forall \{t\in T~\vert~b \in B~\vert~ s_b \in S_B\{b\} \}
    \end{align}
    
.. math::
    :label: min-basin-state-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS}^{t,~b} &\geq V_\text{BAS, MIN}^{b,~s_b} \cdot S_\text{BAS}^{t,~b,~s_b}
    \qquad \forall \{t\in T~\vert~b \in B \}
    \end{align}


.. math::
    :label: basin-state-2
    :nowrap:
    
    \begin{align}
    \sum_{s \in S_B\{b\}} S_\text{BAS}^{t,~b,~s} = 1 \qquad \forall \{t\in T~\vert~b \in B \}
    \end{align} 

.. math::
    :label: basin-volume-per-state-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS, S}^{t,~h,~s_h,~s_q} = V_\text{BAS}^{t,~b_h} \cdot S_\text{FLOW}^{t,~h,~s_h,~s_q} 
    \qquad \forall \{t\in T~ \vert ~h \in H ~ \vert ~ s_h \in S_H\{h\} ~ \vert ~ s_q \in S_Q\{h,~ s_h\}\}
    \end{align} 
    
For constriants :eq:`basin-volume-per-state-2`, the set :math:`B_H` is employed to link basin volumes with hydropower 
plants. This constraint involves the multiplication of a continuous variable with a binary variable. Consequently, 
a Big-M decomposition is required to linearize it.
"""
import pyomo.environ as pyo

def basin_volume_constraints(model):
    ####################################################################################################################
    ### Basin volume evolution constraints #############################################################################  
    #################################################################################################################### 
    model.basin_volume_evolution = pyo.Constraint(model.T, model.B, rule=basin_volume_evolution)
    model.basin_end_volume_constraint = pyo.Constraint(model.B, rule=basin_end_volume_constraint)
    model.basin_max_end_volume_constraint = pyo.Constraint(model.B, rule=basin_max_end_volume_constraint)
    model.basin_min_end_volume_constraint = pyo.Constraint(model.B, rule=basin_min_end_volume_constraint)
    ####################################################################################################################
    ### Basin volume boundary constraints used to determine the state of each basin ####################################
    ####################################################################################################################
    model.basin_max_state_constraint = pyo.Constraint(model.T, model.BS, rule=basin_max_state_constraint)
    model.basin_min_state_constraint = pyo.Constraint(model.T, model.BS, rule=basin_min_state_constraint)
    model.basin_state_constraint = pyo.Constraint(model.T, model.B, rule=basin_state_constraint)
    ###################################################################################################################
    ## Basin volume state constraints used to determine the state of each basin #######################################
    ###################################################################################################################
    model.basin_state_max_active_constraint = pyo.Constraint(model.T, model.HQS, rule=basin_state_max_active_constraint)
    model.basin_state_max_inactive_constraint = pyo.Constraint(model.T, model.HQS, rule=basin_state_max_inactive_constraint)
    model.basin_state_min_active_constraint = pyo.Constraint(model.T, model.HQS, rule=basin_state_min_active_constraint)
    model.basin_state_min_inactive_constraint = pyo.Constraint(model.T, model.HQS, rule=basin_state_min_inactive_constraint)
    return model
    

def basin_volume_evolution(model, t, b):
    if t == model.T.first():
        return model.basin_volume[t, b] == model.start_basin_volume[b]
    else:
        return model.basin_volume[t, b] == (
            model.basin_volume[t - 1, b] + model.discharge_volume[t - 1, b] - model.spilled_volume[t - 1, b] + 
            model.nb_hours * model.nb_sec * model.volume_factor *
            sum(model.water_factor[b, h] * model.flow[t - 1, h] for h in model.H
            )
        )


def basin_end_volume_constraint(model, b):
    t_max = model.T.last()
    return model.end_basin_volume[b] == (
        model.basin_volume[t_max, b] + model.discharge_volume[t_max, b] - model.spilled_volume[t_max, b] +
        model.nb_hours * model.nb_sec * model.volume_factor *
        sum(model.water_factor[b, h] * model.flow[t_max, h] for h in model.H)
    )

def basin_max_end_volume_constraint(model, b):
    return model.end_basin_volume[b] <= model.max_basin_volume[b, model.S_B[b].last()]

def basin_min_end_volume_constraint(model, b):
    return model.end_basin_volume[b] >= model.min_basin_volume[b, model.S_B[b].first()]
####################################################################################################################
### Basin volume boundary constraints used to determine the state of each basin ####################################
####################################################################################################################

def basin_max_state_constraint(model, t, b, s_b):
    return (
        model.basin_volume[t, b] <= model.max_basin_volume[b, s_b] +
        model.max_basin_volume[b, model.S_B[b].last()] * 
        (1 - model.basin_state[t, b, s_b])
    )

def basin_min_state_constraint(model, t, b, s_b):
    return model.basin_volume[t, b] >= model.min_basin_volume[b, s_b] * model.basin_state[t, b, s_b]

def basin_state_constraint(model, t, b):
    return sum(model.basin_state[t, b, s] for s in model.S_B[b]) == 1

###################################################################################################################
## Basin volume state constraints used to determine the state of each basin #######################################
###################################################################################################################

def basin_state_max_active_constraint(model, t, h, s_h, s_q):
    b = model.B_H[h].first()
    return (
        model.basin_volume_by_state[t,  h, s_h, s_q] <= 
        model.max_basin_volume[b, model.S_B[b].last()] * model.flow_state[t, h, s_h, s_q]
    )

def basin_state_max_inactive_constraint(model, t,  h, s_h, s_q):
    b = model.B_H[h].first()
    return (
        model.basin_volume_by_state[t, h, s_h, s_q] >= 
        model.basin_volume[t, b] -
        model.max_basin_volume[b, model.S_B[b].last()] * (1 - model.flow_state[t, h, s_h, s_q])
    )

def basin_state_min_active_constraint(model, t, h, s_h, s_q):
    b = model.B_H[h].first()
    return (
        model.basin_volume_by_state[t, h, s_h, s_q] >= 
        model.min_basin_volume[b, model.S_B[b].first()] * model.flow_state[t, h, s_h, s_q]
    )
    
def basin_state_min_inactive_constraint(model, t, h, s_h, s_q):
    b = model.B_H[h].first()
    return (
        model.basin_volume_by_state[t, h, s_h, s_q] <= 
        model.basin_volume[t, b] - 
        model.min_basin_volume[b, model.S_B[b].first()] * (1 - model.flow_state[t, h, s_h, s_q])
    )
    
