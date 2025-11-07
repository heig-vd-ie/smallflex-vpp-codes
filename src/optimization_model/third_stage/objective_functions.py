

def third_stage_objective_with_battery(model):


    return (
        total_power_deviation_therm(model) +
        hydro_power_deviation_therm(model) +
        hydro_power_forced_deviation_therm(model) +
        battery_power_term(model) +
        spilled_penalty_therm(model)
    )


def third_stage_objective_without_battery(model):

    return (
        total_power_deviation_therm(model) +
        hydro_power_deviation_therm(model) +
        hydro_power_forced_deviation_therm(model) +
        spilled_penalty_therm(model)
    )

def total_power_deviation_therm(model):
    return sum(
        model.total_power_increase[t] + model.total_power_decrease[t]
        for t in model.T    
    )
def hydro_power_deviation_therm(model):
    return sum(
        sum(
            (model.hydro_power_increase[t, h] + model.hydro_power_decrease[t, h]) 
            * model.hydro_power_penalty_factor[h]
        for h in model.H)
    for t in model.T)

def battery_power_term(model):
    return model.battery_penalty_factor * sum(
        model.battery_charging_power[t] + model.battery_discharging_power[t]
        for t in model.T
    )

def hydro_power_forced_deviation_therm(model):
    return sum(
        sum(
            (model.hydro_power_forced_increase[t, h] + model.hydro_power_forced_decrease[t, h]) 
            * model.hydro_power_forced_penalty_factor[h]
        for h in model.H)
    for t in model.T)

def spilled_penalty_therm(model):
    return (
        sum(
            sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b]
            for b in model.B
        ) / model.nb_sec 
    )