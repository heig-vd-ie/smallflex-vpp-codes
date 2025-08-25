from dataclasses import dataclass
from datetime import timedelta


@dataclass
class PipelineConfig:
    """Configuration for the Dig A Plan optimization pipeline"""
    

    first_stage_timestep: timedelta
    
    second_stage_timestep: timedelta
    second_stage_sim_horizon: timedelta

    year: int
    market_country: str = "CH"
    market: str = "DA"
    ancillary_market: str = "FCR-cap"
    market_source: str= "swissgrid"
    max_alpha_error: float = 1.3
    volume_factor: float = 1e-6
    solver_name: str = 'gurobi'