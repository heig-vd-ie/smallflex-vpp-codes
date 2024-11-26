import pyomo.environ as pyo

def baseline_sets(model):
    model.T = pyo.Set()
    model.H = pyo.Set()
    model.B = pyo.Set()
    # index gathering the state per basin and the hydro powerplants
    model.S_b = pyo.Set(model.B)
    model.S_h = pyo.Set(model.H)
    # index gathering the state of every basin and hydro powerplants
    model.BS = pyo.Set(dimen=2, initialize=lambda model: [(b, s_b) for b in model.B for s_b in model.S_b[b]])
    model.HS = pyo.Set(dimen=2, initialize=lambda model: [(h, s_h) for h in model.H for s_h in model.S_h[h]])
    # index (gathering h, b, s_h, s_b) to make the correspondence between the state of basin and hydro powerplants
    model.S_BH = pyo.Set(dimen=4) 
    
    return model
