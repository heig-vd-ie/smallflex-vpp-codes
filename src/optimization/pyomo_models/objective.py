import pyomo.environ as pyo
def baseline_objective(model):

    @model.Objective(sense=pyo.maximize) # type: ignore
    def selling_income(model):
        return sum(model.market_price[t] * sum(
            model.turbined_energy[t, h] 
            - model.pumped_energy[t, h] 
            for h in model.H
        ) for t in model.T)

    return model