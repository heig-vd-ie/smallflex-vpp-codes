CREATE TABLE Resource
(
    id INT PRIMARY KEY,
    name STRING,
    exist BOOL
);

CREATE TABLE Hp
(
    resource_fk int REFERENCES Resource(id)
);

CREATE TABLE Pv
(
    resource_fk int REFERENCES Resource(id)
);

CREATE TABLE Wt
(
    resource_fk int REFERENCES Resource(id)
);

CREATE TABLE Es
(
    resource_fk int REFERENCES Resource(id)
);

CREATE TABLE Pm
(
    resource_fk int REFERENCES Resource(id)
);

CREATE TABLE TimeIndex
(
    datetime TEXT PRIMARY KEY,
    t INTEGER,
    w INTEGER,
    year INTEGER
);

CREATE TABLE Scenario
(
    id INTEGER PRIMARY KEY,
    name STRING,
    prob REAL
);

CREATE TABLE Horizon
(
    id INTEGER PRIMARY KEY,
    name STRING not null check ( name in ("DA", "RT") )
);

CREATE TABLE Market
(
    id INTEGER PRIMARY KEY,
    name STRING not null check ( name in ("DA", "BAL", "FCR_up", "FCR_dn", "RR_up", "RR_dn", "aFRR_up", "aFRR_dn", "mFRR_up", "mFRR_dn") )
);

CREATE TABLE Irradiation
(
    datetime TEXT REFERENCES TimeIndex(datetime),
    scenario_fk INTEGER REFERENCES Scenario(id),
    pv_fk INTEGER REFERENCES Pv(resource_fk),
    horizon_fk INTEGER REFERENCES Horizon(id),
    value REAL not null,
    PRIMARY KEY(datetime, scenario_fk, horizon_fk, pv_fk)
);

CREATE TABLE WindSpeed
(
    datetime TEXT REFERENCES TimeIndex(datetime),
    scenario_fk INTEGER REFERENCES Scenario(id),
    wt_fk INTEGER REFERENCES Wt(id),
    horizon_fk INTEGER REFERENCES Horizon(id),
    value REAL not null,
    PRIMARY KEY(datetime, scenario_fk, horizon_fk, wt_fk)
);

CREATE TABLE WaterDischarge
(
    datetime TEXT REFERENCES TimeIndex(datetime),
    scenario_fk INTEGER REFERENCES Scenario(id),
    hp_fk INTEGER REFERENCES Hp(id),
    horizon_fk INTEGER REFERENCES Horizon(id),
    value REAL not null,
    PRIMARY KEY(datetime, scenario_fk, horizon_fk, hp_fk)
);

CREATE TABLE Price
(
    datetime TEXT REFERENCES TimeIndex(datetime),
    scenario_fk INTEGER REFERENCES Scenario(id),
    market_fk INTEGER REFERENCES Market(id),
    horizon_fk INTEGER REFERENCES Horizon(id),
    value REAL not null,
    PRIMARY KEY(datetime, scenario_fk, horizon_fk, market_fk)
)