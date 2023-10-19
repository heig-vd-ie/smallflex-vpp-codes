import itertools
import pyomo.environ as pyo
import logging
import pandas as pd
from pyomo.environ import PositiveReals, Binary, Any
from schema.schema import Base, DesignScheme, DesignSchemeMapping, HydroPower, Pump, PiecewiseHydro, DischargeFlowNorm
from schema.schema import get_table
from schema.constraints import check_constraint
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import text


# Test
engine = create_engine(f"sqlite+pysqlite:///:memory:", echo=False)
Base.metadata.create_all(engine)
ds1 = DesignScheme(name="design1")
ds2 = DesignScheme(name="design2")
unit1 = HydroPower(name="ab098", exist=True, p_max=100, v_min=10, v_max=1000)
unit2 = HydroPower(name="bz011", exist=True, p_max=120, v_min=20, v_max=100)
unit4 = HydroPower(name="da222", exist=False, p_max=0, v_min=1, v_max=100)
unit3 = Pump(name="p1", exist=True, p_max=100, q_max=10, hp_unit=[unit1], hp_up=[unit2])
dsm1 = DesignSchemeMapping(design_scheme=ds1, resource=unit1)
dsm2 = DesignSchemeMapping(design_scheme=ds1, resource=unit2)
dsm3 = DesignSchemeMapping(design_scheme=ds2, resource=unit1)
dsm4 = DesignSchemeMapping(design_scheme=ds1, resource=unit3)
dsm5 = DesignSchemeMapping(design_scheme=ds1, resource=unit4)

piece1 = PiecewiseHydro(resource=unit1, head_index=0, beta_index=0, head=10, beta=1, v_min_piece=10, v_max_piece=1000)
piece2 = PiecewiseHydro(resource=unit1, head_index=0, beta_index=1, head=20, beta=2, v_min_piece=10, v_max_piece=1000)
piece3 = PiecewiseHydro(resource=unit2, head_index=0, beta_index=1, head=10, beta=1, v_min_piece=20, v_max_piece=50)
piece4 = PiecewiseHydro(resource=unit2, head_index=1, beta_index=1, head=15, beta=2, v_min_piece=50, v_max_piece=70)
piece5 = PiecewiseHydro(resource=unit2, head_index=2, beta_index=1, head=15, beta=3, v_min_piece=70, v_max_piece=100)
piece6 = PiecewiseHydro(resource=unit4, head_index=0, beta_index=0, head=15, beta=3, v_min_piece=1, v_max_piece=100)

dfn1 = DischargeFlowNorm(resource=unit1, week=1, time_step=1, horizon="DA", scenario="1", q_min=0, q_max=1, q_dis=0.5)
dfn2 = DischargeFlowNorm(resource=unit1, week=1, time_step=2, horizon="DA", scenario="1", q_min=0, q_max=2, q_dis=0.75)
dfn3 = DischargeFlowNorm(resource=unit2, week=1, time_step=1, horizon="DA", scenario="1", q_min=0, q_max=1, q_dis=0.5)
dfn4 = DischargeFlowNorm(resource=unit2, week=1, time_step=2, horizon="DA", scenario="1", q_min=0.25, q_max=1, q_dis=0.5)
dfn5 = DischargeFlowNorm(resource=unit4, week=1, time_step=1, horizon="DA", scenario="1", q_min=0, q_max=1, q_dis=0.5)
dfn6 = DischargeFlowNorm(resource=unit4, week=1, time_step=2, horizon="DA", scenario="1", q_min=0, q_max=2, q_dis=0.75)

with Session(engine) as session:
    session.add_all([ds1, ds2, unit1, unit2, unit3, unit4, dsm1, dsm2, dsm3, dsm4, dsm5, piece1, piece2, piece3, piece4, piece5, piece6])
    session.add_all([dfn1, dfn2, dfn3, dfn4, dfn5, dfn6])
    session.commit()
    check_constraint(session)

# Function for Planning optimization

# Query
dsm_table = get_table(sess=session, class_object=DesignScheme, uuid_columns="uuid")
hps_table = get_table(sess=session, class_object=HydroPower, uuid_columns=["resource_fk","pump_fk", "pump_dn_fk"])
pms_table = get_table(sess=session, class_object=Pump, uuid_columns="resource_fk")
pcw_table = get_table(sess=session, class_object=PiecewiseHydro, uuid_columns="resource_fk")
dfn_table = get_table(sess=session, class_object=DischargeFlowNorm, uuid_columns="resource_fk")
time_index_table = pd.DataFrame(session.execute(text('SELECT week, time_step, horizon, scenario, delta_t FROM TimeIndex ORDER BY time_step')).all()).drop_duplicates()


# get inputs
set_r_hp = hps_table.resource_fk.to_list()
set_r_pm = pms_table.resource_fk.to_list()
set_w = list(set(time_index_table["week"].to_list()))
set_t = list(set(time_index_table["time_step"].to_list()))
set_z = list(set(time_index_table["horizon"].to_list()))
set_s = list(set(time_index_table["scenario"].to_list()))
set_l = dsm_table["uuid"].to_list()
set_k = pcw_table[["resource_fk", "head_index"]].set_index("resource_fk").groupby("resource_fk").agg({"head_index": list}).head_index.to_dict()
set_j = pcw_table[["resource_fk", "beta_index"]].set_index("resource_fk").groupby("resource_fk").agg({"beta_index": list}).beta_index.to_dict()

# parameter inputs
h_hat_hp = pcw_table[["resource_fk", "head_index", "beta_index", "head"]].set_index(["resource_fk", "head_index", "beta_index"]).groupby(["resource_fk", "head_index", "beta_index"], group_keys=True).mean()["head"].to_dict()
beta_hp = pcw_table[["resource_fk", "head_index", "beta_index", "beta"]].set_index(["resource_fk", "head_index", "beta_index"]).groupby(["resource_fk", "head_index", "beta_index"], group_keys=True).mean()["beta"].to_dict()
p_max_hp = hps_table[["resource_fk", "p_max"]].set_index("resource_fk")["p_max"].to_dict()
v_lim_hp = hps_table[["resource_fk", "v_min", "v_max"]].set_index("resource_fk").to_dict()
v_piece_lim_hp = pcw_table[["resource_fk", "head_index", "v_min_piece", "v_max_piece"]].set_index(["resource_fk", "head_index"]).groupby(["resource_fk", "head_index"], group_keys=True).mean().to_dict()
q_min_hp = dfn_table[["resource_fk", "week", "time_step", "horizon", "scenario", "q_min"]].set_index(["resource_fk", "week", "time_step", "horizon", "scenario"]).groupby(["resource_fk", "week", "time_step", "horizon", "scenario"], group_keys=True).mean()["q_min"].to_dict()
q_max_hp = dfn_table[["resource_fk", "week", "time_step", "horizon", "scenario", "q_max"]].set_index(["resource_fk", "week", "time_step", "horizon", "scenario"]).groupby(["resource_fk", "week", "time_step", "horizon", "scenario"], group_keys=True).mean()["q_max"].to_dict()
q_dis_hp = dfn_table[["resource_fk", "week", "time_step", "horizon", "scenario", "q_dis"]].set_index(["resource_fk", "week", "time_step", "horizon", "scenario"]).groupby(["resource_fk", "week", "time_step", "horizon", "scenario"], group_keys=True).mean()["q_dis"].to_dict()
delta_t = time_index_table.set_index(["week", "time_step", "horizon", "scenario"]).groupby(["week", "time_step", "horizon", "scenario"], group_keys=True).mean()["delta_t"].to_dict()
next_time = dict(zip(set_t, [*set_t[1:], None]))
pump_hp = hps_table[["resource_fk", "pump_fk"]].set_index("resource_fk")["pump_fk"].apply(lambda x: [x] if str(x)!="None" else []).to_dict()
pump_dn_hp = hps_table[["resource_fk", "pump_dn_fk"]].set_index("resource_fk")["pump_dn_fk"].apply(lambda x: [x] if str(x)!="None" else []).to_dict()

p_max_pm = pms_table[["resource_fk", "p_max"]].set_index("resource_fk")["p_max"].to_dict()
q_max_pm = pms_table[["resource_fk", "q_max"]].set_index("resource_fk")["q_max"].to_dict()


# initial points that must be integrated in database
v0 = 0
q_avg_weekly = {d: 0 for d in set(itertools.chain(*[list(itertools.product(*[[set_r_hp[r]], set_w, set_s, set_l])) for r, r_val in enumerate(set_r_hp)]))}

model = pyo.ConcreteModel()

log = logging.getLogger(__name__)

if set(set_k.keys()) != set(set_r_hp):
    log.error("The sets set_k and set_r_hp are not consistent.")
if set(set_j.keys()) != set(set_r_hp):
    log.error("The sets set_j and set_r_hp are not consistent.")
if set(h_hat_hp.keys()) != set(itertools.chain(*[list(itertools.product(*[[set_r_hp[r]], set_k[r_val], set_j[r_val]])) for r, r_val in enumerate(set_r_hp)])):
    log.error("The sets h_hat_hp and set_r_hp and set_k and set_j are not consistent.")
if set(beta_hp.keys()) != set(itertools.chain(*[list(itertools.product(*[[set_r_hp[r]], set_k[r_val], set_j[r_val]])) for r, r_val in enumerate(set_r_hp)])):
    log.error("The sets beta_hp and set_r_hp and set_k and set_j are not consistent.")
if set(p_max_hp.keys()) != set(set_r_hp):
    log.error("The sets p_max_hp and set_r_hp are not consistent.")
if set(p_max_pm.keys()) != set(set_r_pm):
    log.error("The sets p_max_pm and set_r_pm are not consistent.")
if set(q_max_pm.keys()) != set(set_r_pm):
    log.error("The sets q_max_pm and set_r_pm are not consistent.")

# Setting of optimization problem

# Constants
rho_hp = 998

# Define sets in pyomo
model.R_HP = pyo.Set(initialize=set_r_hp)
model.Time = pyo.Set(initialize=set_t)
model.Week = pyo.Set(initialize=set_w)
model.Horizon = pyo.Set(initialize=set_z)
model.Scenario = pyo.Set(initialize=set_s)
model.Designs = pyo.Set(initialize=set_l)
model.Index = pyo.Set(initialize=(itertools.product(*[set_w, set_t, set_z, set_s, set_l])), dimen=5)
model.IndexWithoutL = pyo.Set(initialize=(itertools.product(*[set_w, set_t, set_z, set_s])), dimen=4)
model.K_HP = pyo.Set(initialize=set(itertools.chain(*[list(itertools.product(*[[set_r_hp[r]], set_k[r_val]])) for r, r_val in enumerate(set_r_hp)])), dimen=2)
model.J_HP = pyo.Set(initialize=set(itertools.chain(*[list(itertools.product(*[[set_r_hp[r]], set_k[r_val], set_j[r_val]])) for r, r_val in enumerate(set_r_hp)])), dimen=3)

model.R_PM = pyo.Set(initialize=set_r_pm)

# Define parameters in pyomo
model.H_HP = pyo.Param(model.J_HP, initialize=h_hat_hp)
model.BETA_HP = pyo.Param(model.J_HP, initialize=beta_hp)
model.P_HP_MAX = pyo.Param(model.R_HP, initialize=p_max_hp)
model.V_HP_MIN = pyo.Param(model.R_HP, initialize=v_lim_hp["v_min"])
model.V_HP_MAX = pyo.Param(model.R_HP, initialize=v_lim_hp["v_max"])
model.V_PIECE_HP_MIN = pyo.Param(model.K_HP, initialize=v_piece_lim_hp["v_min_piece"])
model.V_PIECE_HP_MAX = pyo.Param(model.K_HP, initialize=v_piece_lim_hp["v_max_piece"])
model.Q_HP_MIN = pyo.Param(model.R_HP, model.IndexWithoutL, initialize=q_min_hp)
model.Q_HP_MAX = pyo.Param(model.R_HP, model.IndexWithoutL, initialize=q_max_hp)
model.Q_HP_DIS = pyo.Param(model.R_HP, model.IndexWithoutL, initialize=q_dis_hp)
model.Delta_T = pyo.Param(model.IndexWithoutL, initialize=delta_t)
model.Next_Time = pyo.Param(model.Time, initialize=next_time, within=Any)
model.First_Step = pyo.Param(initialize=set_t[0])
model.Q_HP_AVG = pyo.Param(model.R_HP, model.Week, model.Scenario, model.Designs, initialize=q_avg_weekly)
model.Pump_HP = pyo.Param(model.R_HP, initialize=pump_hp, within=Any)
model.Pump_DN_HP = pyo.Param(model.R_HP, initialize=pump_dn_hp, within=Any)

model.P_PM_MAX = pyo.Param(model.R_PM, initialize=p_max_pm)
model.Q_PM_MAX = pyo.Param(model.R_PM, initialize=q_max_pm)

# Define variables in pyomo
model.p_hp = pyo.Var(model.R_HP, model.Index, domain=PositiveReals)
model.p_hat_hp = pyo.Var(model.K_HP, model.Index, domain=PositiveReals)
model.q_hp = pyo.Var(model.R_HP, model.Index, domain=PositiveReals)
model.v_hp = pyo.Var(model.R_HP, model.Index, domain=PositiveReals)
model.zeta_hp = pyo.Var(model.K_HP, model.Index, domain=Binary)
model.delta_q_pos = pyo.Var(model.R_HP, model.Week, model.Horizon, model.Scenario, model.Designs, domain=PositiveReals)
model.delta_q_neg = pyo.Var(model.R_HP, model.Week, model.Horizon, model.Scenario, model.Designs, domain=PositiveReals)

model.p_pm = pyo.Var(model.R_PM, model.Index, domain=PositiveReals)
model.q_pm = pyo.Var(model.R_PM, model.Index, domain=PositiveReals)
model.phi_pm = pyo.Var(model.R_PM, model.Index, domain=PositiveReals)

model.q_hp_in = pyo.Var(model.R_HP, model.Index, domain=PositiveReals)

def define_constraint_hp_flow(model):
    # constraint 47
    model.hp_linear1 = pyo.Constraint(model.R_HP, model.Index, rule=lambda m, r, w, t, z, s, l: m.p_hp[r, w, t, z, s, l] == sum([m.p_hat_hp[r, k[1], w, t, z, s, l] for k in m.K_HP if k[0] == r]))

    # constraint 49
    model.hp_linear2 = pyo.Constraint(model.J_HP, model.Index, rule=lambda m, r, k, j, w, t, z, s, l: m.p_hat_hp[r, k, w, t, z, s, l] <= rho_hp * (model.H_HP[r, k, j] * model.q_hp[r, w, t, z, s, l] + model.BETA_HP[r, k, j]))
    model.hp_linear3 = pyo.Constraint(model.K_HP, model.Index, rule=lambda m, r, k, w, t, z, s, l: m.p_hat_hp[r, k, w, t, z, s, l] >= 0)

    # constraint 50
    model.hp_linear4 = pyo.Constraint(model.K_HP, model.Index, rule=lambda m, r, k, w, t, z, s, l: m.p_hat_hp[r, k, w, t, z, s, l] <= m.P_HP_MAX[r] * m.zeta_hp[r, k, w, t, z, s, l])

    # constraint 51
    model.hp_linear5 = pyo.Constraint(model.K_HP, model.Index, rule=lambda m, r, k, w, t, z, s, l: m.v_hp[r, w, t, z, s, l] >= m.V_HP_MIN[r] * (1 - m.zeta_hp[r, k, w, t, z, s, l]) + m.V_PIECE_HP_MIN[r, k] * m.zeta_hp[r, k, w, t, z, s, l])
    model.hp_linear6 = pyo.Constraint(model.K_HP, model.Index, rule=lambda m, r, k, w, t, z, s, l: m.v_hp[r, w, t, z, s, l] <= m.V_HP_MAX[r] * (1 - m.zeta_hp[r, k, w, t, z, s, l]) + m.V_PIECE_HP_MAX[r, k] * m.zeta_hp[r, k, w, t, z, s, l])

    # Constraint 52
    model.hp_linear7 = pyo.Constraint(model.R_HP, model.Index, rule=lambda m, r, w, t, z, s, l: sum([m.zeta_hp[r, k[1], w, t, z, s, l] for k in m.K_HP if k[0] == r]) == 1)

    # Constraint 9
    model.hp_flow1 = pyo.Constraint(model.R_HP, model.Index, rule=lambda m, r, w, t, z, s, l: m.q_hp[r, w, t, z, s, l] <= m.Q_HP_MAX[r, w, t, z, s])
    model.hp_flow2 = pyo.Constraint(model.R_HP, model.Index, rule=lambda m, r, w, t, z, s, l: m.q_hp[r, w, t, z, s, l] >= m.Q_HP_MIN[r, w, t, z, s])

    # Constraint 10
    model.hp_flow3 = pyo.Constraint(model.R_HP, model.Index, rule=lambda m, r, w, t, z, s, l: m.V_HP_MIN[r] <= m.v_hp[r, w, t, z, s, l] + (m.q_hp_in[r, w, t, z, s, l] - m.q_hp[r, w, t, z, s, l]) * m.Delta_T[w, t, z, s])
    model.hp_flow4 = pyo.Constraint(model.R_HP, model.Index, rule=lambda m, r, w, t, z, s, l: m.V_HP_MAX[r] >= m.v_hp[r, w, t, z, s, l] + (m.q_hp_in[r, w, t, z, s, l] - m.q_hp[r, w, t, z, s, l]) * m.Delta_T[w, t, z, s])

    # Constraint 19
    model.hp_flow5 = pyo.Constraint(model.R_HP, model.Index, rule=lambda m, r, w, t, z, s, l: m.q_hp_in[r, w, t, z, s, l] == m.Q_HP_DIS[r, w, t, z, s] - sum([m.q_pm[r1, w, t, z, s, l] for r1 in m.Pump_HP[r]]) + sum([m.q_pm[r1, w, t, z, s, l] for r1 in m.Pump_DN_HP[r]]))
    return model

def define_constraint_hp_volume(model):

    # Constraint 11 and 15
    model.hp_volume1 = pyo.Constraint(model.R_HP, model.Index, rule=lambda m, r, w, t, z, s, l: m.v_hp[r, w, m.Next_Time[t], z, s, l] <= m.v_hp[r, w, t, z, s, l] + (m.q_hp_in[r, w, t, z, s, l] - m.q_hp[r, w, t, z, s, l]) * m.Delta_T[w, t, z, s] if m.Next_Time[t] is not None else m.v_hp[r, w, m.First_Step, z, s, l] == v0)
    return model

def define_constraint_hp_balance(model):
    # Constraint 12, 13, 14
    model.hp_balance1 = pyo.Constraint(model.R_HP, model.Week, model.Horizon, model.Scenario, model.Designs, rule=lambda m, r, w, z, s, l: m.delta_q_pos[r, w, z, s, l] - m.delta_q_neg[r, w, z, s, l] == m.Q_HP_AVG[r, w, s, l] - sum([m.q_hp[r, w, t, z, s, l] * m.Delta_T[w, t, z, s] for t in m.Time]) / sum([m.Delta_T[w, t, z, s] for t in m.Time]))
    model.hp_balance2 = pyo.Constraint(model.R_HP, model.Week, model.Horizon, model.Scenario, model.Designs, rule=lambda m, r, w, z, s, l: m.delta_q_pos[r, w, z, s, l] >= 0)
    model.hp_balance3 = pyo.Constraint(model.R_HP, model.Week, model.Horizon, model.Scenario, model.Designs, rule=lambda m, r, w, z, s, l: m.delta_q_neg[r, w, z, s, l] >= 0)
    return model

def define_constraint_pm(model):
    # Constraint 16, 17, 18
    model.pm_flow1 = pyo.Constraint(model.R_PM, model.Index, rule=lambda m, r, w, t, z, s, l: m.p_pm[r, w, t, z, s, l] == m.P_PM_MAX[r] * m.phi_pm[r, w, t, z, s, l] / m.Delta_T[w, t, z, s])
    model.pm_flow2 = pyo.Constraint(model.R_PM, model.Index, rule=lambda m, r, w, t, z, s, l: m.q_pm[r, w, t, z, s, l] == m.Q_PM_MAX[r] * m.phi_pm[r, w, t, z, s, l] / m.Delta_T[w, t, z, s])
    model.pm_flow3 = pyo.Constraint(model.R_PM, model.Index, rule=lambda m, r, w, t, z, s, l: m.phi_pm[r, w, t, z, s, l] >= 0)
    model.pm_flow4 = pyo.Constraint(model.R_PM, model.Index, rule=lambda m, r, w, t, z, s, l: m.phi_pm[r, w, t, z, s, l] <= m.Delta_T[w, t, z, s])
    return model

model = define_constraint_hp_flow(model)
model = define_constraint_hp_volume(model)
model = define_constraint_hp_balance(model)
model = define_constraint_pm(model)
