

def second_stage_objective_with_battery_with_ancillary(model):

    ancillary_market_income = sum(
        model.ancillary_market_price[f] *
        (model.hydro_ancillary_reserve[f] + model.battery_ancillary_reserve[f])
        for f in model.F
    )
    
    battery_capacity_penalty = (
        (model.end_battery_soc_overage * model.overage_market_price
        - model.end_battery_soc_shortage * model.shortage_market_price)
        * model.battery_capacity
    )
    
    return (
        market_income_term(model)
        + ancillary_market_income
        + spilled_penalty_term(model)
        + basin_volume_penalty_term(model)
        + battery_capacity_penalty
    )

def second_stage_objective_without_battery_with_ancillary(model):

    ancillary_market_income = sum(
        model.ancillary_market_price[f] *
        (model.hydro_ancillary_reserve[f])
        for f in model.F
    )

    return (
        market_income_term(model)
        + ancillary_market_income
        + spilled_penalty_term(model)
        + basin_volume_penalty_term(model)
    )
    
def second_stage_objective_with_battery_without_ancillary(model):

    battery_capacity_penalty = (
        (model.end_battery_soc_overage * model.overage_market_price
        - model.end_battery_soc_shortage * model.shortage_market_price)
        * model.battery_capacity
    )
    
    return (
        market_income_term(model)
        + spilled_penalty_term(model)
        + basin_volume_penalty_term(model)
        + battery_capacity_penalty
    )

def second_stage_objective_without_battery_without_ancillary(model):


    return (
        market_income_term(model)
        + spilled_penalty_term(model)
        + basin_volume_penalty_term(model)
    )
    

def spilled_penalty_term(model):
    return (
        sum(
            - sum(model.spilled_volume[t, b] for t in model.T) * model.spilled_factor[b]
            for b in model.B
        ) / model.nb_sec
    )

def market_income_term(model):
    return (
        model.nb_hours
        * sum(
            model.market_price[t] * model.total_power[t]
            for t in model.T
        )
    )


def basin_volume_penalty_term(model):
    return (
        sum(
            model.rated_alpha[b] * model.basin_volume_range[b] * (
                model.end_basin_volume_mean_overage[b] * model.overage_market_price
                - model.end_basin_volume_mean_shortage[b] * model.shortage_market_price
                - sum(
                    model.bound_penalty_factor[q] * 
                    model.shortage_market_price *
                    (model.end_basin_volume_upper_overage[b, q] + model.end_basin_volume_lower_shortage[b, q])
                    for q in model.Q
                )
            ) for b in model.UP_B)
    / model.nb_sec
)