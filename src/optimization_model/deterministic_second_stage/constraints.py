r"""
2.5.1 Objective
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. math::
    :label: second-objective
    :nowrap:
    
    \begin{align}
    \max  \sum_{t~\in~T} \sum_{h~\in~H}  nb_\text{HOUR} \cdot c_\text{DA}^{t} \cdot  P_\text{HYDRO}^{t,~h} 
    + \sum_{f~\in~F} c_\text{FLEX}^{f} \cdot P_\text{ANC}^{f}  
    + \sum_{h~\in~H} \left( dV_\text{+}^{h} \cdot F_\text{dV +}^{h} - dV_\text{-}^{h} \cdot F_\text{dV -}^{h} \right)
    - F_\text{SPIL} \cdot \sum_{b~\in~B} V_\text{SPIL}^{b}
    \end{align}

2.5.2 Water basin volume evolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: basin-volume-evolution-2
    :nowrap:
        
    \begin{align}
        V_\text{BAS}^{t,~b} =
        \begin{cases} 
            V_\text{START}^{b} & \text{if } t = t_0 \\
            V_\text{BAS}^{t - 1,~b} + V_\text{DIS}^{t - 1,~b} - V_\text{SPIL}^{t - 1,~b} + 
            nb_\text{SEC} \cdot nb_\text{HOUR} \cdot 
            \sum_{h~\in~H} F_\text{HYDRO} ^{b,~h} \cdot Q^{t-1,~h} 
            \quad & \text{if } t \neq t_0
        \end{cases} \qquad \forall \{t~\in~T, b~\in~B \}
    \end{align} 
    
.. math::
    :label: end-basin-volume-evolution-2
    :nowrap:
        
    \begin{align}
        V_\text{END}^{b} = V_\text{BAS}^{t_{end},~b} + V_\text{DIS}^{t_{end},~b}  - V_\text{SPIL}^{t_{end},~b} + 
        nb_\text{SEC} \cdot nb_\text{HOUR} \cdot \sum_{h~\in~H} F_\text{HYDRO}^{b,~h} \cdot Q^{t_{end},~h}
    \qquad \forall \{b~\in~B \}
    \end{align} 
    
.. math::
    :label: max-end-basin-volume-2
    :nowrap:
    
    \begin{align}
    V_\text{END}^{b} &\leq V_\text{MAX}^{b,~S_B^\text{END}\{b\}} 
    \qquad \forall \{b~\in~B \}
    \end{align}

.. math::
    :label: min-end-basin-volume-2
    :nowrap:
    
    \begin{align}
    V_\text{END}^{b} &\geq V_\text{MIN}^{b,~S_B^\text{0}\{b\}} 
    \qquad \forall \{b~\in~B \}
    \end{align}

2.5.3 Water basin state
~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: max-basin-state-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS}^{t,~b} &\leq V_\text{MAX}^{b,~s} +  V_\text{MAX}^{b,~S_B^\text{END}\{b\}} 
    \cdot \left(1 -State^{t,~b,~s} \right)
    \qquad \forall \{t~\in~T~\vert~(b,~s)~\in~BS \}
    \end{align}
    
.. math::
    :label: min-basin-state-2
    :nowrap:
    
    \begin{align}
    V_\text{BAS}^{t,~b} &\geq V_\text{MIN}^{b,~s} \cdot State^{t,~b,~s}
    \qquad \forall \{t~\in~T~\vert~b~\in~B \}
    \end{align}


.. math::
    :label: basin-state-2
    :nowrap:
    
    \begin{align}
    \sum_{s~\in~S_B\{b\}} State^{t,~b,~s} = 1 \qquad \forall \{t~\in~T~\vert~b~\in~B \}
    \end{align} 

2.5.4 Hydro powerplants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: flow-state-2
    :nowrap:
    
    \begin{align}
        Q_\text{S}^{t,~h,~s} \leq Q_\text{MAX}^{h,~s} \cdot State^{t,~b,~s}
        \qquad \forall \{t~\in~T~\vert~(h,~b,~s)~\in~HBS \}
    \end{align}
    
.. math::
    :label: flow-2
    :nowrap:
    
    \begin{align}
        Q^{t,~h} = \sum_{s~\in~S_H\{h\}} Q_\text{S}^{t,~h,~s} 
        \qquad \forall \{t~\in~T~\vert~h~\in~H~\}
    \end{align}
    
.. math::
    :label: hydro-power-2
    :nowrap:
    
    \begin{align}
        P_\text{HYDRO}^{t,~h} = \sum_{s~\in~S_H\{h\}} \alpha^{h,~s} \cdot  Q_\text{S}^{t,~h,~s}
        \qquad \forall \{t~\in~T~\vert~h~\in~H\}
    \end{align}
    
2.5.4.1 ON/OFF Hydro powerplants
++++++++++++++++++++++++++++++++++

.. math::
    :label: max-active-flow-by-state-constraint
    :nowrap:
    
    \begin{align}
    Q_\text{S}^{t,~h,~s} \leq M \cdot Run_\text{HYDRO}^{t,~h} 
    \qquad \forall \{t~\in~T~\vert~(h,~s)~\in~DHS \}
    \end{align} 
    
.. math::
    :label: max_inactive_flow_by_state_constraint
    :nowrap:
    
    \begin{align}
    Q_\text{S}^{t,~h,~s} \geq Q_\text{MAX}^{h,~s} \cdot State^{t,~b,~s} - M \cdot \left(1 - Run_\text{HYDRO}^{t,~h} \right)
    \qquad \forall \{t~\in~T~\vert~(h,~s)~\in~DHS \}
    \end{align} 
    
    
2.5.5. Ancillary services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
    :label: positive-hydro-ancillary-power-2
    :nowrap:
    
    \begin{align}
        P_\text{ANC}^{f} \leq \sum_{b,~s~\in~BS} P_\text{FLEX +}^{~s} \cdot State^{t,~b,~s} - 
        \sum_{h~\in~CH} P_\text{HYDRO}^{t,~h} \qquad \forall \{(t,~f)~\in~TF\}
    \end{align}
    
.. math::
    :label: negative-hydro-ancillary-power-2
    :nowrap:
    
    \begin{align}
        P_\text{ANC}^{f} \leq \sum_{b,~s~\in~BS} P_\text{FLEX -}^{~s} \cdot State^{t,~b,~s} + 
        \sum_{h~\in~CH} P_\text{HYDRO}^{t,~h}\qquad \forall \{(t,~f)~\in~TF\}
    \end{align}

    
2.5.6 Powered water quota
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. math::
    :label: diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{+}^{h} - dV_\text{-}^{h} = V_\text{QUOTA}^{h} - 
        nb_\text{SEC} \cdot nb_\text{HOUR}  \cdot \sum_{t~\in~T} Q^{t,~h} \qquad \forall \{h~\in~H\}
    
    \end{align} 

.. math::
    :label: max_pos_diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{+}^{h} \leq V_\text{BUF +}^{h} \qquad \forall \{h~\in~H\}
    \end{align} 
    
.. math::
    :label: min_pos_diff-volume
    :nowrap:
        
    \begin{align}
        dV_\text{-}^{h} \leq V_\text{BUF -}^{h} \qquad \forall \{h~\in~H\}
    \end{align} 
    
"""

########################################################################################################################
# 2.5.1 Water basin volume evolution ###################################################################################
########################################################################################################################

def total_power_constraint_with_battery(model, t):
    return (
        model.total_power[t] ==
        model.pv_power[t] +
        model.wind_power[t] -
        model.battery_charging_power[t] +
        model.battery_discharging_power[t] +
        sum(model.hydro_power[t, h] for h in model.H)
    )


def total_power_constraint_without_battery(model, t):
    return (
        model.total_power[t] ==
        model.pv_power[t] +
        model.wind_power[t] +
        sum(model.hydro_power[t, h] for h in model.H)
    )


########################################################################################################################
# 2.5.2 Water basin volume evolution ###################################################################################
########################################################################################################################


def basin_volume_evolution(model, t, b):
    if t == model.T.first():
        return model.basin_volume[t, b] == model.start_basin_volume[b]
    else:
        return model.basin_volume[t, b] == (
            model.basin_volume[t - 1, b] + (
                model.discharge_volume[t - 1, b]
                - model.spilled_volume[t - 1, b]
                + model.nb_hours * model.nb_sec
                * sum(model.water_factor[b, h] * model.flow[t - 1, h] for h in model.H)
            ) / model.basin_volume_range[b]
        )


def basin_end_volume_constraint(model, b):
    t_max = model.T.last()
    return model.end_basin_volume[b] == (
        model.basin_volume[t_max, b] + (
            model.discharge_volume[t_max, b]
            - model.spilled_volume[t_max, b]
            + model.nb_hours * model.nb_sec
            * sum(model.water_factor[b, h] * model.flow[t_max, h] for h in model.H)
        )/ model.basin_volume_range[b]
    )


def basin_max_end_volume_constraint(model, b):
    return model.end_basin_volume[b] <= model.max_basin_volume[b, model.S_B[b].last()]


def basin_min_end_volume_constraint(model, b):
    return model.end_basin_volume[b] >= model.min_basin_volume[b, model.S_B[b].first()]

def end_basin_volume_mean_constraint(model, b):
    return (
        model.end_basin_volume[b] - model.expected_end_basin_volume[b] == 
        model.end_basin_volume_mean_overage[b] - model.end_basin_volume_mean_shortage[b]
    )

def end_basin_volume_upper_limit_constraint(model, b, q):
    return (
        model.end_basin_volume[b] - model.expected_upper_end_basin_volume[b, q] == 
        model.end_basin_volume_upper_overage[b, q] - model.end_basin_volume_upper_shortage[b, q]
    )
    q
def end_basin_volume_lower_limit_constraint(model, b, q):
    return (
        model.end_basin_volume[b] - model.end_basin_volume_lower_limit[b, q] ==
        model.end_basin_volume_lower_overage[b, q] - model.end_basin_volume_lower_shortage[b, q]
    )

########################################################################################################################
# 2.5.3. Water basin state #############################################################################################
########################################################################################################################


def basin_max_state_constraint(model, t, b, s):
    return model.basin_volume[t, b] <= model.max_basin_volume[
        b, s
    ] + model.max_basin_volume[b, model.S_B[b].last()] * (
        1 - model.basin_state[t, b, s]
    )


def basin_min_state_constraint(model, t, b, s):
    return (
        model.basin_volume[t, b]
        >= model.min_basin_volume[b, s] * model.basin_state[t, b, s]
    )


def basin_state_constraint(model, t, b):
    return sum(model.basin_state[t, b, s] for s in model.S_B[b]) == 1


########################################################################################################################
# 2.5.4. Hydropower plants #############################################################################################
########################################################################################################################


def max_flow_by_state_constraint(model, t, h, b, s):
    return (
        model.flow_by_state[t, h, s]
        <= model.max_flow[h, s] * model.basin_state[t, b, s]
    )


def flow_constraint(model, t, h):
    return model.flow[t, h] == sum(model.flow_by_state[t, h, s] for s in model.S_H[h])


def hydro_power_constraint(model, t, h):
    return model.hydro_power[t, h] == sum(
        model.flow_by_state[t, h, s] * model.alpha[h, s] for s in model.S_H[h]
    )

def max_active_flow_by_state_constraint(model, t, h, s):

    return model.flow_by_state[t, h, s] <= model.big_m * model.discrete_hydro_on[t, h]


def max_inactive_flow_by_state_constraint(model, t, h, b, s):

    return model.flow_by_state[t, h, s] >= model.max_flow[h, s] * model.basin_state[
        t, b, s
    ] - model.big_m * (1 - model.discrete_hydro_on[t, h])


########################################################################################################################
# 2.5.5. Ancillary services ############################################################################################
########################################################################################################################


def positive_hydro_ancillary_reserve_constraint(model, t, f):

    return model.hydro_ancillary_reserve[f] <= model.total_positive_flex_power - sum(
        model.hydro_power[t, h] for h in model.CH
    )


def negative_hydro_ancillary_reserve_constraint(model, t, f):

    return model.hydro_ancillary_reserve[f] <= model.total_negative_flex_power + sum(
        model.hydro_power[t, h] for h in model.CH
    )



########################################################################################################################
# 2.5.6 Battery  #######################################################################################################
########################################################################################################################


def battery_soc_evolution_constraint(model, t):
    if t == model.T.first():
        return model.battery_soc[t] == model.start_battery_soc
    else:
        return (
            model.battery_soc[t]
            == model.battery_soc[t - 1]
            + (
                model.battery_charging_power[t - 1] * model.battery_efficiency
                - model.battery_discharging_power[t - 1] / model.battery_efficiency
            )
            * model.nb_hours / model.battery_capacity
        )
        
def end_battery_soc_constraint(model):
    t_max = model.T.last()
    return (
        model.start_battery_soc + model.end_battery_soc_overage - model.end_battery_soc_shortage == 
        model.battery_soc[t_max] + (
                model.battery_charging_power[t_max] * model.battery_efficiency
                - model.battery_discharging_power[t_max] / model.battery_efficiency
            )
            * model.nb_hours
            / model.battery_capacity
        )
    
def end_battery_soc_overage_constraint(model):
    return model.start_battery_soc  + model.end_battery_soc_overage <= 1

def end_battery_soc_shortage_constraint(model):
    return model.start_battery_soc - model.end_battery_soc_shortage >= 0
    

def battery_in_charge_constraint(model, t):
    return model.battery_charging_power[t] <= model.battery_rated_power * model.battery_in_charge[t]

def battery_in_discharge_constraint(model, t):
    return model.battery_discharging_power[t] <= model.battery_rated_power * (1 - model.battery_in_charge[t])


def battery_max_charging_power_constraint(model, t):
    return model.battery_charging_power[t] <= model.battery_rated_power

def battery_max_discharging_power_constraint(model, t):
    return model.battery_discharging_power[t] <= model.battery_rated_power


########################################################################################################################
# 2.5.6 Battery ancillary services #####################################################################################
########################################################################################################################

def battery_max_charging_power_with_ancillary_constraint(model, t, f):
    return model.battery_charging_power[t] + model.battery_ancillary_reserve[f] <= model.battery_rated_power

def battery_max_discharging_power_with_ancillary_constraint(model, t, f):
    return model.battery_discharging_power[t] + model.battery_ancillary_reserve[f] <= model.battery_rated_power

def battery_positive_energy_reserve_constraint(model, t, f):
    nb_hours = model.nb_timestamp_per_ancillary - t + model.nb_timestamp_per_ancillary * f
    return (1 - model.battery_soc[t]) * model.battery_capacity >= nb_hours * model.battery_ancillary_reserve[f]

def battery_negative_energy_reserve_constraint(model, t, f):
    nb_hours = model.nb_timestamp_per_ancillary - t + model.nb_timestamp_per_ancillary * f
    return model.battery_soc[t] * model.battery_capacity >= nb_hours * model.battery_ancillary_reserve[f]

    
