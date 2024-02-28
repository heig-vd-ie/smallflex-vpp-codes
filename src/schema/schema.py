import enum
import pandas as pd
from sqlalchemy import ForeignKey, create_engine, Column, null
from sqlalchemy.sql.sqltypes import String, Float, Boolean, DateTime, Integer, Enum
from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy.orm import Mapped, declarative_mixin, relationship
from sqlalchemy.orm import Session
from sqlalchemy_utils import UUIDType
import uuid
import datetime


class Base(DeclarativeBase):
    pass


class ResourceType(enum.Enum):
    Resource = 0
    HydroPower = 1
    Photovoltaic = 2
    WindTurbine = 3
    EnergyStorage = 4
    Pump = 5


class TimeIndexType(enum.Enum):
    TimeIndex = 0
    DischargeFlowNorm = 1
    IrradiationNorm = 2
    WindSpeedNorm = 3
    MarketPriceNorm = 4
    TemperatureNorm = 5



class HasUuid(object):
    uuid = Column(UUIDType, primary_key=True, default=uuid.uuid4)


class DesignSchemeMapping(Base, HasUuid):
    __tablename__ = "DesignSchemeMapping"
    resource_fk: Mapped["UUIDType"] = mapped_column(ForeignKey("Resource.uuid"), primary_key=True)
    design_scheme_fk: Mapped["UUIDType"] = mapped_column(ForeignKey("DesignScheme.uuid"), primary_key=True)
    resource: Mapped["Resource"] = relationship(back_populates="design_scheme_mappings")
    design_scheme: Mapped["DesignScheme"] = relationship(back_populates="design_scheme_mappings")


class DesignScheme(Base, HasUuid):
    __tablename__ = "DesignScheme"
    name: Mapped[str] = Column(String(30), nullable=True)
    design_scheme_mappings: Mapped[list["DesignSchemeMapping"]] = relationship(back_populates='design_scheme')
    resources: Mapped[list["Resource"]] = relationship(secondary="DesignSchemeMapping", back_populates="design_schemes", viewonly=True)

    def __repr__(self) -> str:
        return f"DesignScheme(uuid={self.uuid!r}, name={self.name!r})"


class Resource(Base, HasUuid):
    __tablename__ = "Resource"
    name: Mapped[str] = Column(String(30), nullable=True)
    # geo = Column(list[Float], nullable=True)
    concrete_type = Column(Enum(ResourceType))
    exist: Mapped[Boolean] = Column(Boolean, nullable=False, default=True)
    design_schemes: Mapped[list["DesignScheme"]] = relationship(secondary="DesignSchemeMapping", back_populates="resources", viewonly=True)
    design_scheme_mappings: Mapped[list["DesignSchemeMapping"]] = relationship(back_populates='resource')

    def __repr__(self) -> str:
        return f"Resource(uuid={self.uuid!r}, name={self.name!r}, exist={self.exist!r})"

    __mapper_args__ = {
        'polymorphic_identity': ResourceType.Resource,
        'polymorphic_on': concrete_type
    }


class HydroPower(Resource):
    __tablename__ = "HydroPower"
    resource_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("Resource.uuid"), primary_key=True)
    p_max = Column(Float, nullable=False)  # MW
    v_min = Column(Float, nullable=False)  # m3
    v_max = Column(Float, nullable=False)  # m3
    h_n = Column(Float, nullable=True)  # m
    q_n = Column(Float, nullable=True)  # m3/s
    river =  Column(String(30), nullable=True)
    production_annual = Column(Float, nullable=True) # GWh
    turbine_type = Column(String(30), nullable=True)
    discharge_flow_norm = relationship('DischargeFlowNorm', back_populates='resource')
    piecewise_table = relationship('PiecewiseHydro', back_populates='resource')

    pump_fk = Column(UUIDType, nullable=True)
    pump = relationship("Pump", backref="hp_unit", primaryjoin="HydroPower.pump_fk == Pump.resource_fk", foreign_keys=pump_fk,)

    pump_dn_fk = Column(UUIDType, nullable=True)
    pump_dn = relationship("Pump", backref="hp_up", primaryjoin="HydroPower.pump_dn_fk == Pump.resource_fk", foreign_keys=pump_dn_fk, )

    def __repr__(self) -> str:
        return f"HydroPower(uuid={self.resource_fk!r}, name={self.name!r}, exist={self.exist!r})"

    __mapper_args__ = {
        'polymorphic_identity': ResourceType.HydroPower,
    }


class PiecewiseHydro(Base, HasUuid):
    __tablename__ = "PiecewiseHydro"
    resource = relationship(HydroPower, back_populates="piecewise_table")
    resource_fk = Column(UUIDType, ForeignKey("HydroPower.resource_fk"), nullable=False)
    head_index = Column(Integer, nullable=False)
    beta_index = Column(Integer, nullable=False)
    vmin_piece = Column(Float, nullable=False)
    vmax_piece = Column(Float, nullable=False)
    head = Column(Float, nullable=False)
    beta = Column(Float, nullable=False)


class Photovoltaic(Resource):
    __tablename__ = "Photovoltaic"
    resource_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("Resource.uuid"), primary_key=True)
    area = Column(Float, nullable=False)
    p_max = Column(Float, nullable=False)
    eta_r = Column(Float, default=0.17)
    f_snow = Column(Float, default=1)
    irradiation_norm = relationship('IrradiationNorm', back_populates='resource')

    def __repr__(self) -> str:
        return f"Photovoltaic(uuid={self.resource_fk!r}, name={self.name!r}, exist={self.exist!r})"
    __mapper_args__ = {
        'polymorphic_identity': ResourceType.Photovoltaic,
    }


class WindTurbine(Resource):
    __tablename__ = "WindTurbine"
    resource_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("Resource.uuid"), primary_key=True)
    wind_speed_norm = relationship('WindSpeedNorm', back_populates='resource')
    p_max = Column(Float, nullable=False)
    area = Column(Float, nullable=False)
    cpr = Column(Float, nullable=False, default=0.5)
    cut_in_speed = Column(Float, nullable=False, default=2)
    cut_off_speed = Column(Float, nullable=False, default=34)
    eta = Column(Float, nullable=False, default=0.8)

    def __repr__(self) -> str:
        return f"WindTurbine(uuid={self.resource_fk!r}, name={self.name!r}, exist={self.exist!r})"
    __mapper_args__ = {
        'polymorphic_identity': ResourceType.WindTurbine,
    }


class EnergyStorage(Resource):
    __tablename__ = "EnergyStorage"
    resource_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("Resource.uuid"), primary_key=True)

    def __repr__(self) -> str:
        return f"EnergyStorage(uuid={self.resource_fk!r}, name={self.name!r}, exist={self.exist!r})"
    __mapper_args__ = {
        'polymorphic_identity': ResourceType.EnergyStorage,
    }


class Pump(Resource):
    __tablename__ = "Pump"
    resource_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("Resource.uuid"), primary_key=True)
    p_max = Column(Float, nullable=False)
    q_max = Column(Float, nullable=False)
    hp_up = Column(Float, nullable=True)
    river =  Column(String(30), nullable=True)
    v_max = Column(Float, nullable=True)
    def __repr__(self) -> str:
        return f"Pump(uuid={self.resource_fk!r}, name={self.name!r}, exist={self.exist!r})"
    __mapper_args__ = {
        'polymorphic_identity': ResourceType.Pump,
    }


class TimeIndex(Base, HasUuid):
    __tablename__ = "TimeIndex"
    concrete_type = Column(Enum(TimeIndexType))
    week: Mapped[int] = Column(Integer, nullable=False)
    time_step: Mapped[int] = Column(Integer, nullable=False)
    horizon: Mapped[str] = Column(String(30), nullable=False)
    scenario: Mapped[str] = Column(String(30), nullable=False)
    delta_t: Mapped[float] = Column(Float, default=1, nullable=False)  # in hour
    timestamp: Mapped[datetime.datetime] = Column(DateTime, nullable=False)
    
    __mapper_args__ = {
        'polymorphic_identity': TimeIndexType.TimeIndex,
        'polymorphic_on': concrete_type
    }


class DischargeFlowNorm(TimeIndex):
    __tablename__ = "DischargeFlowNorm"
    
    time_index_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("TimeIndex.uuid"), primary_key=True)
    river: Mapped[String] = Column(String, nullable=False)
    value = Column(Float, nullable=False)

    __mapper_args__ = {
        'polymorphic_identity': TimeIndexType.DischargeFlowNorm,
    }

class TemperatureNorm(TimeIndex):
    __tablename__ = "TemperatureNorm"

    time_index_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("TimeIndex.uuid"), primary_key=True)
    value = Column(Float, nullable=False)
    alt: Mapped[Float] = Column(Float, nullable=False, default=0)

    __mapper_args__ = {
        'polymorphic_identity': TimeIndexType.TemperatureNorm,
    }

class IrradiationNorm(TimeIndex):
    __tablename__ = "IrradiationNorm"

    time_index_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("TimeIndex.uuid"), primary_key=True)
    value = Column(Float, nullable=False)
    alt: Mapped[Float] = Column(Float, nullable=False, default=0)

    __mapper_args__ = {
        'polymorphic_identity': TimeIndexType.IrradiationNorm,
    }

class WindSpeedNorm(TimeIndex):
    __tablename__ = "WindSpeedNorm"

    time_index_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("TimeIndex.uuid"), primary_key=True)
    value = Column(Float, nullable=False)
    alt: Mapped[Float] = Column(Float, nullable=False, default=0)

    __mapper_args__ = {
        'polymorphic_identity': TimeIndexType.WindSpeedNorm,
    }

class MarketPriceNorm(TimeIndex):
    __tablename__ = "MarketPriceNorm"

    time_index_fk: Mapped[uuid.UUID] = Column(UUIDType, ForeignKey("TimeIndex.uuid"), primary_key=True)
    value = Column(Float, nullable=True)
    market: Mapped[String] = Column(String, nullable=False, default="spot")

    def __repr__(self) -> str:
        return f"MarketPriceNorm(market={self.market!r}, price={self.value!r})"
    __mapper_args__ = {
        'polymorphic_identity': TimeIndexType.MarketPriceNorm,
    }

@declarative_mixin
class Record(HasUuid):
    __tablename__ = "Record"
    timestamp: Mapped[datetime.datetime] = Column(DateTime, nullable=False)

class Irradiation(Base, Record):
    __tablename__ = "Irradiation"
    value = Column(Float, nullable=True)
    alt: Mapped[Float] = Column(Float, nullable=False, default=0)

    def __repr__(self) -> str:
        return f"Irradiation(altitude={self.alt!r}, timestamp={self.timestamp!r}, ghi={self.value!r})"

class Temperature(Base, Record):
    __tablename__ = "Temperature"
    value = Column(Float, nullable=True)
    alt: Mapped[Float] = Column(Float, nullable=False, default=0)

    def __repr__(self) -> str:
        return f"Temperature(altitude={self.alt!r}, timestamp={self.timestamp!r}, temperature={self.value!r})"

class DischargeFlow(Base, Record):
    __tablename__ = "DischargeFlow"
    value = Column(Float, nullable=True, default="Rhone")
    river: Mapped[String] = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"DischargeFlow(river={self.river!r}, timestamp={self.timestamp!r}, discharge={self.value!r})"


class WindSpeed(Base, Record):
    __tablename__ = "WindSpeed"
    value = Column(Float, nullable=True)
    alt: Mapped[Float] = Column(Float, nullable=False, default=0)

    def __repr__(self) -> str:
        return f"WindSpeed(altitude={self.alt!r}, timestamp={self.timestamp!r}, wind_speed={self.value!r})"


class MarketPrice(Base, Record):
    __tablename__ = "MarketPrice"
    value = Column(Float, nullable=True)
    market: Mapped[String] = Column(String, nullable=False, default="spot")
    country: Mapped[String] = Column(String, nullable=False, default="CH")
    source: Mapped[String] = Column(String, nullable=False, default="swissgrid")
    direction: Mapped[String] = Column(String, nullable=False, default="pos")

    def __repr__(self) -> str:
        return f"MarketPrice(market={self.market!r}, country={self.country!r}, source={self.source!r}, direction={self.direction!r}, timestamp={self.timestamp!r}, price={self.value!r})"


def get_table(sess, class_object, uuid_columns):
    cl = sess.query(class_object).all()
    cl_table = pd.DataFrame(r.__dict__ for r in cl)
    if type(uuid_columns) is list:
        for uc in uuid_columns:
            cl_table[uc] = cl_table[uc].apply(lambda x: str(x))
    else:
        cl_table[uuid_columns] = cl_table[uuid_columns].apply(lambda x: str(x))
    return cl_table


if __name__ == "__main__":
    engine = create_engine(f"sqlite://", echo=False)

    Base.metadata.create_all(engine)
    dt = datetime.datetime.fromisoformat

    with Session(engine) as session:
        unit1 = HydroPower(name="KWA", exist=True, p_max=10, v_min=1, v_max=100)
        unit2 = Photovoltaic(name="PV1", area=10)
        unit3 = Pump(name="pm1", p_max=10, q_max=1)
        unit4 = WindTurbine(area=10)
        unit5 = EnergyStorage()
        session.add_all([unit1, unit2, unit3, unit4, unit5])
        session.commit()
