

def third_stage_objective_with_battery(model):

    battery_power = model.battery_penalty_factor * sum(
        model.battery_charging_power[t] + model.battery_discharging_power[t]
        for t in model.T
    )
    return (
        total_power_deviation_therm(model) +
        hydro_power_deviation_therm(model) +
        spilled_penalty_therm(model) +
        battery_power
    )


def third_stage_objective_without_battery(model):

    return (
        total_power_deviation_therm(model) +
        hydro_power_deviation_therm(model) +
        spilled_penalty_therm(model)
    )

def total_power_deviation_therm(model):
    return sum(
        model.total_power_deviation_positive[t] + model.total_power_deviation_negative[t]
        for t in model.T    
    )
def hydro_power_deviation_therm(model):
    return sum(
        sum(
            (model.hydro_power_deviation_positive[t, h] + model.hydro_power_deviation_negative[t, h]) 
            * model.hydro_power_penalty_factor[h]
        for h in model.H)
    for t in model.T)

def spilled_penalty_therm(model):
    return (
        sum(
            sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b]
            for b in model.B
        ) / model.nb_sec 
    )