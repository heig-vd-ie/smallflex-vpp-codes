import pyomo.environ as pyo
def baseline_objective(model):

    @model.Objective(sense=pyo.maximize) # type: ignore
    def selling_income(model):
        return sum(model.market_price[t] * (model.turbined_energy[t]- model.pumped_energy[t]) for t in model.T)

    return model