from dataclasses import dataclass, field
from datetime import timedelta
import pyomo.environ as pyo
from numpy.random import default_rng, Generator


@dataclass
class BatteryConfig:
    battery_capacity: float = 4 # MWh
    battery_rated_power: float = 1 # MW
    battery_efficiency: float = 0.95
    start_battery_soc: float = 0.5 # %
    
@dataclass
class HydroConfig:
    basin_volume_quantile: list[float] = field(default_factory=lambda: [0.45, 0.35, 0.2])
    basin_volume_quantile_min: list[float] = field(default_factory=lambda: [0.1, 0.05, 0.03])
    bound_penalty_factor: list[float] = field(default_factory=lambda: [0.3, 0.15, 0.05])
    nb_state_dict: dict[int, int] = field(default_factory=lambda: {})
    start_basin_volume_ratio: dict[int, float] = field(default_factory=lambda: {})
    spilled_factor: float = 1e6
    d_height: float = 0.01
    first_stage_max_powered_flow_ratio: float= 0.75
    hydro_participation_to_imbalance: bool = True

@dataclass
class DgrConfig:
    pv_power_rated_power = 9 # MW
    wind_turbine_rated_power = 8 # m/
    wind_speed_cut_in = 3 # m/s
    wind_speed_cut_off = 15 # m/s


@dataclass
class MarketConfig:
    market_country: str = "CH"
    market: str = "DA"
    ancillary_market: str = "FCR-cap"
    market_source: str= "swissgrid"
    market_price_lower_quantile: float = 0.35
    market_price_upper_quantile: float = 0.65
    market_price_window_size: int = 182 # 28 days
    with_ancillary: bool = True

@dataclass
class DataConfig(BatteryConfig, HydroConfig, MarketConfig, DgrConfig):

    first_stage_timestep: timedelta = timedelta(days=1)
    second_stage_sim_horizon: timedelta = timedelta(days=1)
    second_stage_timestep: timedelta = timedelta(hours=1)
    ancillary_market_timestep: timedelta = timedelta(hours=4)
    nb_scenarios: int = 100
    total_scenarios_synthesized: int = 100
    year: int = 2024

    verbose: bool = False
    time_limit: float = 20
    solver_name: str = 'gurobi'
    seed: int = 42


    def __post_init__(self):

        self.rng: Generator = default_rng(self.seed)

        self.first_stage_solver= pyo.SolverFactory(self.solver_name)
        self.second_stage_solver= pyo.SolverFactory(self.solver_name)
        
        assert len(self.basin_volume_quantile) == len(self.bound_penalty_factor)
        # assert self.second_stage_sim_horizon.total_seconds()%self.first_stage_timestep.total_seconds() == 0
        assert self.second_stage_sim_horizon.total_seconds()%self.second_stage_timestep.total_seconds() == 0
        assert self.ancillary_market_timestep.total_seconds()%self.second_stage_timestep.total_seconds() == 0
        assert self.second_stage_sim_horizon.total_seconds()%self.ancillary_market_timestep.total_seconds() == 0
        assert self.total_scenarios_synthesized >= self.nb_scenarios


        self.first_stage_nb_timestamp: int = max(self.second_stage_sim_horizon // self.first_stage_timestep, 1)
        self.second_stage_nb_timestamp: int = self.second_stage_sim_horizon // self.second_stage_timestep
        self.nb_timestamp_per_ancillary: int = self.ancillary_market_timestep // self.second_stage_timestep
        self.nb_quantiles: int = len(self.basin_volume_quantile)
        self.second_stage_solver.options['TimeLimit'] = self.time_limit
        
        self.scenario_list = list(self.rng.choice(
            range(self.total_scenarios_synthesized), 
            size=self.nb_scenarios, replace=False
        ))

