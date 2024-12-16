r"""
Powered water volume
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. math::
    :label: diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{+}^{h} - dV_\text{-}^{h} = V_\text{POW}^{h} + dV_\text{LAST}^{h} - 
        nb_\text{SEC} \cdot nb_\text{HOUR}  \cdot \sum_{t \in T} Q^{t,~h} \qquad \forall \{h\in H\}
    
    \end{align} 

.. math::
    :label: max_pos_diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{+}^{h} \leq \begin{cases} 
            V_\text{BUF}^{h} + dV_\text{LAST}^{h} / 2 \qquad &\text{if } dV_\text{LAST}^{h} \geq 0 \\
            V_\text{BUF}^{h} & \text{otherwise }
        \end{cases} \qquad \forall \{h\in H\}
    \end{align} 
    
.. math::
    :label: min_pos_diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{-}^{h} \leq \begin{cases} 
            V_\text{BUF}^{h} - dV_\text{LAST}^{h} / 2 \qquad &\text{if } dV_\text{LAST}^{h} \leq 0 \\
            V_\text{BUF}^{h} & \text{otherwise }
        \end{cases} \qquad \forall \{h\in H\}
    \end{align} 

"""

import pyomo.environ as pyo

def powered_volume_constraints(model):
    model.max_powered_volume_constraint = pyo.Constraint(model.H, rule=max_powered_volume_constraint)
    model.min_powered_volume_constraint = pyo.Constraint(model.H, rule=min_powered_volume_constraint)
    model.diff_volume_constraint = pyo.Constraint(model.H, rule=diff_volume_constraint)
    return model


def diff_volume_constraint(model, h):
    return (
        model.diff_volume_pos[h] - model.diff_volume_neg[h] ==
        model.remaining_volume[h] + model.powered_volume[h]  - 
        sum(model.flow[t, h] for t in model.T) * model.nb_hours * model.nb_sec * model.volume_factor
    )

    
def max_powered_volume_constraint(model, h):
    if model.powered_volume_enabled:
        pos_volume = model.remaining_volume[h] if model.remaining_volume[h] > 0 else 0
        return (
            model.diff_volume_pos[h] <= model.volume_buffer[h] + pos_volume/2
        )
    else:
        return pyo.Constraint.Skip
    
def min_powered_volume_constraint(model, h):
    if model.powered_volume_enabled:
        neg_volume = - model.remaining_volume[h] if model.remaining_volume[h] < 0 else 0
        return (
            model.diff_volume_neg[h] <= model.volume_buffer[h] + neg_volume/2
        )
    else:
        return pyo.Constraint.Skip
    
