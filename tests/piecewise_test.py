# %%
import numpy as np
import pyomo.environ as pyo
import os
os.chdir(os.getcwd().replace("/src", ""))
# os.environ['GRB_LICENSE_FILE'] = os.environ["HOME"] + "/gurobi_license/gurobi.lic

# %%


data = {
    None: {
        "I": {None: list(range(3))},
        "T": {None: list(range(2))},
        "S": {None: list(range(5))},
        "z_par": {
            (0, 0): 0, (1, 0): 10, (2, 0): 30.0,
            (0, 1): 0, (1, 1): 10, (2, 1): 35.0},
    }
}

x = {
    (0, 0): [0.0, 5.0, 9.0, 10.0, 50.0], (1, 0): [0.0, 6.0, 50.0], (2, 0): [0.0, 5.0, 9.0, 10.0, 50.0],
    (0, 1): [0.0, 5.0, 9.0, 10.0, 50.0], (1, 1): [0.0, 6.0, 50.0], (2, 1): [0.0, 5.0, 9.0, 10.0, 50.0]
}
s_x = {
    (0, 0): [0.0, 2.0, 3.0, 10.0, 20.0], (1, 0): [10.0, 5.0, 200.0], (2, 0): [0.0, 2.0, 3.0, 10.0, 20.0],
    (0, 1): [0.0, 2.0, 3.0, 10.0, 20.0], (1, 1): [10.0, 5.0, 200.0], (2, 1): [0.0, 2.0, 3.0, 10.0, 20.0]
}

model = pyo.AbstractModel()
model.I = pyo.Set()
model.T = pyo.Set()
model.x = pyo.Param(model.I)
model.s_x = pyo.Param(model.I)
model.z_par = pyo.Param(model.I, model.T)

model.z = pyo.Var(model.I, model.T, domain=pyo.NonNegativeReals, bounds=(0,50), initialize = 0)
model.y = pyo.Var(model.I, model.T, domain=pyo.NonNegativeReals)

def propagation(model, i, t):

    return model.z_par[i, t] == model.z[i, t]

model.propagation = pyo.Constraint(model.I, model.T, rule=propagation)

instance: pyo.Model = model.create_instance(data)
instance.piecewise = pyo.Piecewise(instance.I, instance.T, instance.y, instance.z, pw_pts=x, f_rule=s_x, pw_constr_type='LB', pw_repn='SOS2')
solver = pyo.SolverFactory('gurobi')

solver.solve(instance, tee=False)
instance.y.pprint()