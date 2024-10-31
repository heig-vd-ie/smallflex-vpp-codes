
import pyomo.environ as pyo

if __name__=="__main__":
    
    # import gurobipy as gp
    # Attempt to create a basic model to check for license validity
    # model = gp.Model()
    # print("Gurobi license is correctly installed and functioning.")

    solver = pyo.SolverFactory('gurobi')
    print(solver)
