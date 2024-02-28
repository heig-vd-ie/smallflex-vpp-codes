
import datetime
from sqlalchemy import ForeignKey, create_engine, Column
from schema.schema import Base, HydroPower, Photovoltaic, Pump, WindTurbine, DesignScheme
from sqlalchemy.orm import Session



engine = create_engine(f"sqlite://", echo=False)

Base.metadata.create_all(engine)
dt = datetime.datetime.fromisoformat

with Session(engine) as session:
    ds = {}
    for g in ["g1", "g2", "g3"]:
        ds[g] = DesignScheme(name=g)

    unit1 = HydroPower(name="Altstafel", geo=[46.470064921863404, 8.368679486729981], exist=True, p_max=0, v_min=0, v_max=0,)
    unit2 = Photovoltaic(name="PV1", area=10)
    unit3 = Pump(name="pm1", p_max=10, q_max=1)
    unit4 = WindTurbine(area=10)

    session.add_all([unit1, unit2, unit3, unit4])
    session.commit()
