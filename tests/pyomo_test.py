import pyomo.environ as pyo

if __name__=="__main__":
    
    model = pyo.AbstractModel()

    model.T = pyo.Set()
    model.I = pyo.Set()
    model.J = pyo.Set(model.I)
    model.JI = pyo.Set(dimen=2, initialize=lambda model: [(i, j) for i in model.I for j in model.J[i]])

    model.x = pyo.Var(model.JI, domain=pyo.NonNegativeReals)
    model.z = pyo.Param(model.I, default=20.0)

    data = {
        None: {
            'T': {None: list(range(4))},
            'I': {None: list(range(4))},
            'J': {
                0: [0],
                1: [1],
                2: [2, 3],
                3: [3, 4, 5]},
            'z': {0 : 100.0, 1: 50.0, 2: 200.0}
        }
    }

    @model.Constraint(model.I) # type: ignore
    def test(model, i):
            return model.z[i] == sum(model.x[i, j] for j in model.J[i])

    @model.Constraint(model.JI) # type: ignore
    def test_2(model, i, j):
            return model.x[i, j] >= model.z[model.I.last()]/10

    instance: pyo.Model = model.create_instance(data)

    solver = pyo.SolverFactory('gurobi')

    solver.solve(instance)
    for c in instance.component_objects(pyo.Param, active=True):
        print(f"Constraint: {c.name}")
        print(c.display())
    
    
    
    # print(instance.x.extract_values()) # type: ignore


