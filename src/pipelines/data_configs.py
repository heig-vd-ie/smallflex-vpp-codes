from dataclasses import dataclass
from datetime import timedelta


@dataclass
class PipelineConfig:
    """Configuration for the Dig A Plan optimization pipeline"""
    
    year: int
    first_stage_timestep: timedelta = timedelta(days=1)
    second_stage_timestep: timedelta = timedelta(hours=1)
    second_stage_sim_horizon: timedelta = timedelta(days=4)
    market_country: str = "CH"
    market: str = "DA"
    ancillary_market: str = "FCR-cap"
    market_source: str= "swissgrid"
    max_alpha_error: float = 1.3
    volume_factor: float = 1e-6
    spilled_factor: float = 1e3
    solver_name: str = 'gurobi'