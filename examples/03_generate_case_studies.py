
import json
import datetime
from sqlalchemy import create_engine
from schema.schema import Base, HydroPower, Photovoltaic, Pump, WindTurbine, DesignScheme, PiecewiseHydro, DesignSchemeMapping
from sqlalchemy.orm import Session



if __name__ == "__main__":
    units_json = r"src/units.json"
    db_cache_file = r".cache/interim/case.db"

    units = json.load(open(units_json))
    engine = create_engine(f"sqlite+pysqlite:///{db_cache_file}", echo=False)

    Base.metadata.create_all(engine)
    dt = datetime.datetime.fromisoformat

    with Session(engine) as session:
        design_schemes = {}
        possible_design_schemes = sorted(list(set(sum([l["design_scheme"] for l in list(units.values())], []))))
        hydro_units = dict(filter(lambda x: x[1]["type"]=="hydro", units.items()))
        pump_units = dict(filter(lambda x: x[1]["type"]=="pump", units.items()))
        pv_units = dict(filter(lambda x: x[1]["type"]=="pv", units.items()))
        wind_units = dict(filter(lambda x: x[1]["type"]=="wind", units.items()))

        ds = {g: DesignScheme(name=g) for g in possible_design_schemes}
        hp = {g[0]: HydroPower(name=g[0], exist=True, p_max=g[1]["p_max"],
                            v_min=g[1]["v_min"], v_max=g[1]["v_max"],
                            h_n=g[1]["h_n"], q_n=g[1]["q_n"], 
                            production_annual=g[1]["production_annual"],
                            turbine_type=g[1]["turbine_type"], 
                            river=g[1]["river"]) for g in hydro_units.items()}
        pm = {g[0]: Pump(name=g[0], exist=True, p_max=g[1]["p_max"],
                        q_max=g[1]["q_max"], hp_up=g[1]["hp_up"], river=g[1]["river"],
                        v_max=g[1]["v_max"]) for g in pump_units.items()}
        pv = {g[0]: Photovoltaic(name=g[0], exist=True, p_max=g[1]["p_max"],
                                area=g[1]["area"], alt=g[1]["alt"]) for g in pv_units.items()}
        wd = {g[0]: WindTurbine(name=g[0], exist=True, p_max=g[1]["p_max"], area=g[1]["area"], 
                                cpr=g[1]["cpr"], cut_in_speed=g[1]["cut_in_speed"],
                                cut_off_speed=g[1]["cut_off_speed"], eta=g[1]["eta"], 
                                alt=g[1]["alt"]) for g in wind_units.items()}

        pw_hp = {g[0]: [PiecewiseHydro(resource=hp[g[0]], index=pw[0],
                                    vmin_piece=pw[1]["vmin_piece"],
                                    vmax_piece=pw[1]["vmax_piece"],
                                    head=pw[1]["head"], beta=pw[1]["beta"]) for pw in g[1]["piecewise_table"].items()] 
                                    for g in hydro_units.items()}

        dsm1 = {g[0]: [DesignSchemeMapping(resource=hp[g[0]], design_scheme=ds[d]) 
                    for d in g[1]["design_scheme"]] for g in hydro_units.items()}
        dsm2 = {g[0]: [DesignSchemeMapping(resource=pm[g[0]], design_scheme=ds[d]) 
                    for d in g[1]["design_scheme"]] for g in pump_units.items()}
        dsm3 = {g[0]: [DesignSchemeMapping(resource=pv[g[0]], design_scheme=ds[d]) 
                    for d in g[1]["design_scheme"]] for g in pv_units.items()}
        dsm4 = {g[0]: [DesignSchemeMapping(resource=wd[g[0]], design_scheme=ds[d]) 
                    for d in g[1]["design_scheme"]] for g in wind_units.items()}
        
        for i in [HydroPower, Pump, Photovoltaic, WindTurbine, DesignScheme, DesignSchemeMapping, PiecewiseHydro]:
            session.query(i).delete()
        session.commit()

        session.add_all(list(ds.values()))
        session.add_all(list(hp.values()))
        session.add_all(list(pm.values()))
        session.add_all(list(pv.values()))
        session.add_all(list(wd.values()))
        for pw in pw_hp.items():
            session.add_all(pw[1])
        for dsm in dsm1.items():
            session.add_all(dsm[1])
        for dsm in dsm2.items():
            session.add_all(dsm[1])
        for dsm in dsm3.items():
            session.add_all(dsm[1])
        for dsm in dsm4.items():
            session.add_all(dsm[1])

        session.commit()
