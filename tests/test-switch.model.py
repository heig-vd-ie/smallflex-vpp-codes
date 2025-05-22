import pyomo.environ as pyo

# Use a ConcreteModel since all data are provided directly.
model = pyo.ConcreteModel()

# -----------------------------
# 1. Sets and Data
# -----------------------------
# Buses: example with 8 buses (0 is the slack bus)
model.I = pyo.Set(initialize=[0, 1, 2, 3, 4, 5, 6, 7])

# Lines: example with 9 lines (0 to 8)
model.L = pyo.Set(initialize=[0, 1, 2, 3, 4, 5, 6, 7, 8])

# Define "from" and "to" for each line (adjust to your network topology)
line_from_data = {0: 0, 1: 1, 2: 1, 3: 2, 4: 2, 5: 5, 6: 3, 7: 4, 8: 7}
line_to_data   = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 6, 7: 7, 8: 6}
model.line_from = pyo.Param(model.L, initialize=line_from_data)
model.line_to   = pyo.Param(model.L, initialize=line_to_data)

# Define which lines are switchable.
# (Here we assume lines 1,2,4,5,6, and 8 are switchable.)
model.S = pyo.Set(initialize=[1, 2, 4, 5, 6, 8])
model.nS = pyo.Set(initialize=[l for l in model.L if l not in model.S])

# Line parameters (resistance and reactance)
r_data = {0: 0.01, 1: 0.02, 2: 0.02, 3: 0.03, 4: 0.03,
          5: 0.01, 6: 0.02, 7: 0.02, 8: 0.04}
x_data = {0: 0.05, 1: 0.04, 2: 0.04, 3: 0.05, 4: 0.05,
          5: 0.02, 6: 0.03, 7: 0.03, 8: 0.06}
model.r = pyo.Param(model.L, initialize=r_data)
model.x = pyo.Param(model.L, initialize=x_data)

# Bus load data (real and reactive), assumed positive (consumption)
p_load_data = {0: 0.0, 1: 1.0, 2: 1.2, 3: 1.5, 4: 0.8, 5: 1.0, 6: 1.2, 7: 0.6}
q_load_data = {0: 0.0, 1: 0.3, 2: 0.4, 3: 0.5, 4: 0.2, 5: 0.3, 6: 0.4, 7: 0.2}
model.p_load = pyo.Param(model.I, initialize=p_load_data)
model.q_load = pyo.Param(model.I, initialize=q_load_data)

# In the convex QP formulation from the paper, voltages are fixed at 1.0 p.u.
fixed_voltage = {i: 1.0 for i in model.I}
model.V = pyo.Param(model.I, initialize=fixed_voltage)

# -----------------------------
# 2. Decision Variables
# -----------------------------
# Real and reactive power flow on each line
model.P = pyo.Var(model.L, domain=pyo.Reals)
model.Q = pyo.Var(model.L, domain=pyo.Reals)

# Binary switch variable for each switchable line
model.y = pyo.Var(model.S, domain=pyo.Binary)

# Expression for y_fixed: equals y for switchable lines; 1 for non-switchable.
def y_fixed_rule(model, l):
    if l in model.S:
        return model.y[l]
    else:
        return 1
model.y_fixed = pyo.Expression(model.L, rule=y_fixed_rule)

# -----------------------------
# 3. Constraints
# -----------------------------
# (a) Power Balance Constraints (for buses i ≠ 0, the slack)
# For each bus i ≠ 0:
#     (sum of incoming flows) - (sum of outgoing flows) = p_load[i]
def power_balance_real_rule(model, i):
    if i == 0:
        return pyo.Constraint.Skip  # Skip slack bus; its generation is free.
    incoming = sum(model.P[l] for l in model.L if model.line_to[l] == i)
    outgoing = sum(model.P[l] for l in model.L if model.line_from[l] == i)
    return incoming - outgoing == model.p_load[i]
model.power_balance_real = pyo.Constraint(model.I, rule=power_balance_real_rule)

def power_balance_reactive_rule(model, i):
    if i == 0:
        return pyo.Constraint.Skip
    incoming = sum(model.Q[l] for l in model.L if model.line_to[l] == i)
    outgoing = sum(model.Q[l] for l in model.L if model.line_from[l] == i)
    return incoming - outgoing == model.q_load[i]
model.power_balance_reactive = pyo.Constraint(model.I, rule=power_balance_reactive_rule)

# (b) Radiality Constraint:
# Each bus (except slack) must have exactly one incoming closed line.
def radiality_rule(model, i):
    if i == 0:
        return pyo.Constraint.Skip
    incoming_status = sum(model.y_fixed[l] for l in model.L if model.line_to[l] == i)
    return incoming_status == 1
model.radiality = pyo.Constraint(model.I, rule=radiality_rule)

# (c) Big-M Constraints: If a line’s switch is open, then its flows must be zero.
M = 1000  # Big-M constant
def bigM_P_upper_rule(model, l):
    return model.P[l] <= M * model.y_fixed[l]
model.bigM_P_upper = pyo.Constraint(model.L, rule=bigM_P_upper_rule)

def bigM_P_lower_rule(model, l):
    return model.P[l] >= -M * model.y_fixed[l]
model.bigM_P_lower = pyo.Constraint(model.L, rule=bigM_P_lower_rule)

def bigM_Q_upper_rule(model, l):
    return model.Q[l] <= M * model.y_fixed[l]
model.bigM_Q_upper = pyo.Constraint(model.L, rule=bigM_Q_upper_rule)

def bigM_Q_lower_rule(model, l):
    return model.Q[l] >= -M * model.y_fixed[l]
model.bigM_Q_lower = pyo.Constraint(model.L, rule=bigM_Q_lower_rule)

# -----------------------------
# 4. Objective
# -----------------------------
# For example, minimize estimated line losses (a quadratic objective)
def objective_rule(model):
    return sum(model.r[l]*(model.P[l]**2) + model.x[l]*(model.Q[l]**2) for l in model.L)
model.objective = pyo.Objective(rule=objective_rule, sense=pyo.minimize)

# -----------------------------
# 5. Solve the Model
# -----------------------------
solver = pyo.SolverFactory('gurobi')
results = solver.solve(model, tee=True)

# Display the solution.
model.display()
