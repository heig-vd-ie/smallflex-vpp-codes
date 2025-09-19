from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
import pyomo.environ as pyo

@dataclass
class PipelineConfig:
    """Configuration for the Dig A Plan optimization pipeline"""
    
    year: int
    year_list: list[int] = field(default_factory=lambda: [])
    first_stage_timestep: timedelta = timedelta(days=2)
    second_stage_timestep: timedelta = timedelta(hours=1)
    ancillary_market_timestep: timedelta = timedelta(hours=4)
    second_stage_sim_horizon: timedelta = timedelta(days=4)
    market_country: str = "CH"
    market: str = "DA"
    ancillary_market: str = "FCR-cap"
    market_source: str= "swissgrid"
    max_alpha_error: float = 1.3
    volume_factor: float = 1e-6
    spilled_factor: float = 1e3
    solver_name: str = 'gurobi'
    first_stage_max_powered_flow_ratio: float= 0.75
    second_stage_min_volume_ratio: float = 0.1
    second_stage_quantile: float = 0.15 
    verbose: bool = False
    volume_buffer_ratio: float = 0.2
    d_height: float = 0.01
    time_limit: float = 20

    nb_state_dict: dict[int, int] = field(default_factory=lambda: {})

    def __post_init__(self):
        self.min_datetime: datetime = datetime(self.year, 1, 1, tzinfo=UTC)
        self.max_datetime: datetime = datetime(self.year + 1, 1, 1, tzinfo=UTC)
        self.first_stage_solver= pyo.SolverFactory(self.solver_name)
        self.second_stage_solver= pyo.SolverFactory(self.solver_name)

        self.second_stage_solver.options['TimeLimit'] = self.time_limit
        assert self.second_stage_sim_horizon.total_seconds()%self.first_stage_timestep.total_seconds() == 0
        assert self.second_stage_sim_horizon.total_seconds()%self.second_stage_timestep.total_seconds() == 0
        assert self.ancillary_market_timestep.total_seconds()%self.second_stage_timestep.total_seconds() == 0
        assert self.second_stage_sim_horizon.total_seconds()%self.ancillary_market_timestep.total_seconds() == 0
        
        self.first_stage_nb_timestamp: int = self.second_stage_sim_horizon // self.first_stage_timestep
        self.second_stage_nb_timestamp: int = self.second_stage_sim_horizon // self.second_stage_timestep
        self.nb_timestamp_per_ancillary: int = self.ancillary_market_timestep // self.second_stage_timestep
        self.ancillary_nb_timestamp: int = self.second_stage_sim_horizon // self.ancillary_market_timestep
