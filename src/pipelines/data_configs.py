from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
import pyomo.environ as pyo
import numpy as np
from typing import Optional, Union

@dataclass
class PipelineConfig:
    """Configuration for the Dig A Plan optimization pipeline"""
    first_stage_timestep: timedelta
    second_stage_sim_horizon: timedelta
    second_stage_timestep: timedelta = timedelta(hours=1)
    ancillary_market_timestep: timedelta = timedelta(hours=4)
    market_country: str = "CH"
    market: str = "DA"
    ancillary_market: str = "FCR-cap"
    market_source: str= "swissgrid"
    max_alpha_error: float = 1.3
    verbose: bool = False
    bound_penalty_factor: float = 1
    first_stage_max_powered_flow_ratio: float= 0.75
    # volume_factor: float = 1e-6
    volume_buffer_ratio: float = 0.2
    spilled_factor: float = 1e6
    solver_name: str = 'gurobi'
    d_height: float = 0.01
    seed: int = 42
    lower_quantile: float = 0.15
    upper_quantile: float = 0.85
    min_quantile_diff: float = 10

    nb_state_dict: dict[int, int] = field(default_factory=lambda: {})
    start_basin_volume_ratio: dict[int, float] = field(default_factory=lambda: {})
    
    def __post_init__(self):
        
        self.rng: np.random.RandomState = np.random.RandomState(self.seed) # type: ignore
        
        self.first_stage_solver= pyo.SolverFactory(self.solver_name)
        self.second_stage_solver= pyo.SolverFactory(self.solver_name)
        
        assert self.second_stage_sim_horizon.total_seconds()%self.first_stage_timestep.total_seconds() == 0
        assert self.second_stage_sim_horizon.total_seconds()%self.second_stage_timestep.total_seconds() == 0
        assert self.ancillary_market_timestep.total_seconds()%self.second_stage_timestep.total_seconds() == 0
        assert self.second_stage_sim_horizon.total_seconds()%self.ancillary_market_timestep.total_seconds() == 0
        
        self.first_stage_nb_timestamp: int = self.second_stage_sim_horizon // self.first_stage_timestep
        self.second_stage_nb_timestamp: int = self.second_stage_sim_horizon // self.second_stage_timestep
        self.nb_timestamp_per_ancillary: int = self.ancillary_market_timestep // self.second_stage_timestep
        # self.ancillary_nb_timestamp: int = self.second_stage_sim_horizon // self.ancillary_market_timestep

@dataclass
class DeterministicConfig(PipelineConfig):
    
    year: int = 2024
    
    battery_capacity: float = 4 # MWh
    battery_rated_power: float = 1 # MW
    battery_efficiency: float = 0.95
    start_battery_soc: float = 0.5 # %
    
    first_stage_max_powered_flow_ratio: float= 0.75
    second_stage_min_volume_ratio: float = 0.1
    market_price_lower_quantile: float = 0.35
    market_price_upper_quantile: float = 0.65
    market_price_window_size: int = 28 # 28 days
    volume_buffer_ratio: float = 0.2
    time_limit: float = 20
    pv_power_rated_power = 9 # MW
    wind_turbine_rated_power = 8 # m/
    wind_speed_cut_in = 3 # m/s
    wind_speed_cut_off = 15 # m/s
    
    def __post_init__(self):
        super().__post_init__()
        self.min_datetime: datetime = datetime(self.year, 1, 1, tzinfo=UTC)
        self.max_datetime: datetime = datetime(self.year + 1, 1, 1, tzinfo=UTC)
        
        self.second_stage_solver.options['TimeLimit'] = self.time_limit


@dataclass
class StochasticConfig(PipelineConfig):
    
    nb_scenarios: int = 100
    
    def __post_init__(self):
        super().__post_init__()
        

DataConfig = Union[DeterministicConfig, StochasticConfig]