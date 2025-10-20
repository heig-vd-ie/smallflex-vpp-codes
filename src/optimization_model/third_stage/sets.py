import pyomo.environ as pyo


def second_stage_sets(model):
    model.T = pyo.Set()
    model.F = pyo.Set()
    model.TF = pyo.Set(dimen=2, within=model.T * model.F) # type: ignore
    model.H = pyo.Set()
    model.B = pyo.Set()
    model.UP_B = pyo.Set(within=model.B)
    
    model.DH = pyo.Set(within=model.H)
    model.CH = pyo.Set(initialize=lambda m: [h for h in m.H if h not in m.DH])
    
    # index gathering the state per basin and the hydro powerplantss
    model.S_B = pyo.Set(model.B)
    model.S_H = pyo.Set(model.H)

    
    # index (gathering h, b, s_h, s_b) to make the correspondence between the state of basin and hydro powerplants
    model.HBS = pyo.Set(dimen=3)

    model.DHBS = pyo.Set(dimen=3, initialize=lambda model: [(h, b, s) for h, b, s in model.HBS if h in model.DH])
    model.CHBS = pyo.Set(dimen=3, initialize=lambda model: [(h, b, s) for h, b, s in model.HBS if h in model.CH])
    
    model.BS = pyo.Set(dimen=2, initialize=lambda model: [(b, s_b) for b in model.B for s_b in model.S_B[b]])
    model.HS = pyo.Set(dimen=2, initialize=lambda model: [(h, s_h) for h in model.H for s_h in model.S_H[h]])
    model.CHS = pyo.Set(dimen=2, initialize=lambda model: [(h, s_h) for h in model.H if h in model.CH for s_h in model.S_H[h]])
    model.DHS = pyo.Set(dimen=2, initialize=lambda model: [(h, s_h) for h in model.H if h in model.DH for s_h in model.S_H[h]])

    return model
