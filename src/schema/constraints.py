import pandas as pd
from schema.schema import PiecewiseHydro, HydroPower


def check_constraint(sess):
    # Query
    pcw = sess.query(PiecewiseHydro).all()
    pcw_table = pd.DataFrame([r.__dict__ for r in pcw])
    hp = sess.query(HydroPower).all()
    hp_table = pd.DataFrame([r.__dict__ for r in hp])

    # Constraint of v_min_piece and v_max_piece are not inserted differently for one head_index
    pcw_table_grouped = pcw_table[["resource_fk", "head_index", "v_min_piece", "v_max_piece"]].groupby(["resource_fk", "head_index"])
    if not pcw_table_grouped.max().to_dict() == pcw_table_grouped.min().to_dict():
        raise RuntimeError("The values of piecewiseHydro for minimums and maximums are not correctly inserted.")

    # Constraint that table of HydroPower and pieces have same min and max volume columns
    v_min_units_piece = pcw_table[["resource_fk", "v_min_piece"]].groupby(["resource_fk"]).min()["v_min_piece"]
    v_max_units_piece = pcw_table[["resource_fk", "v_max_piece"]].groupby(["resource_fk"]).max()["v_max_piece"]
    v_min_units_hp = hp_table[["resource_fk", "v_min"]].groupby(["resource_fk"]).min()["v_min"]
    v_max_units_hp = hp_table[["resource_fk", "v_max"]].groupby(["resource_fk"]).max()["v_max"]
    if (not (v_min_units_piece == v_min_units_hp).all()) | (not (v_max_units_piece == v_max_units_hp).all()):
        raise RuntimeError("The values of piecewiseHydro and Hydro tables for minimums and maximums are not consistent.")
