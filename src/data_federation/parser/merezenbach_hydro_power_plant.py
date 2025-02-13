import os
import polars as pl
from polars import col as c

from data_federation.input_model import SmallflexInputSchema
from data_federation.parser.hydro_power_plant import get_hydro_power_plant_data

from data_federation.parser.morel_hydro_power_plant import DOWNSTREAM_UUID
from general_function import generate_uuid


POWER_PLANT_UUID: str = generate_uuid(base_value="Merezenbach")
TURBINE_UUID: str = generate_uuid(base_value="Merezenbach turbine")
UPSTREAM_UUID: str = generate_uuid(base_value="Merezenbach upstream basin")
DOWNSTREAM_UUID: str = generate_uuid(base_value="Merezenbach downstream basin")

def parse_merezenbach_resources(small_flex_input_schema: SmallflexInputSchema, **kwargs) -> SmallflexInputSchema:

    hydro_power_plant: pl.DataFrame = pl.from_dicts([{
        "name": "Merezenbach", "uuid": POWER_PLANT_UUID, "resource_fk_list": [TURBINE_UUID], 
        "upstream_basin_fk": UPSTREAM_UUID,"downstream_basin_fk": DOWNSTREAM_UUID, "rated_power": 1.9, "rated_flow": 0.5,
        "control": "continuous", "type": "buildup_turbine",
        }])
    resource: pl.DataFrame = pl.from_dicts([
            {"name": "Merezenbach turbine", "uuid": TURBINE_UUID, "type": "hydro_turbine", "rated_power": 1.9,
            "power_plant_fk": POWER_PLANT_UUID},
        ])

    water_basin: pl.DataFrame = pl.from_dicts([
        {
            "name": "Merezenbach upstream basin", "uuid": UPSTREAM_UUID, "power_plant_fk": POWER_PLANT_UUID,
            "volume_max": 1.2e3, "volume_min": 0, "height_max": 1842.7,
        }, {
            "name": "Merezenbach downstream basin", "uuid": DOWNSTREAM_UUID, "power_plant_fk": POWER_PLANT_UUID,
            "height_max": 1337.7, 
        }
    ])

    return (
            small_flex_input_schema.add_table(
                hydro_power_plant=hydro_power_plant, resource=resource, water_basin=water_basin)
        )
    
def parse_merezenbach_performance_table(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], **kwargs) -> SmallflexInputSchema:

    hydro_power_performance_table, head = get_hydro_power_plant_data(file_path=input_file_names["merezenbach_pve_data"]) 
    hydro_power_performance_table = hydro_power_performance_table\
        .select(
            c("Pet_HPP").alias("electrical_power"), # already in MW
            c("Q_HPP").alias("flow"), # already in ms/s
            c("Eta_HPP").alias("efficiency"), # already in %
            pl.lit(head).alias("head"),
            pl.lit(POWER_PLANT_UUID).alias("power_plant_fk"),
        )
    return small_flex_input_schema.add_table(hydro_power_performance_table=hydro_power_performance_table)    


def parse_merezenbach_hydro_power_plant(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]
    ) -> SmallflexInputSchema:
    
    kwargs = {"small_flex_input_schema": small_flex_input_schema, "input_file_names": input_file_names}
    
    kwargs["small_flex_input_schema"] = parse_merezenbach_resources(**kwargs)
    kwargs["small_flex_input_schema"] = parse_merezenbach_performance_table(**kwargs)
    
    return kwargs["small_flex_input_schema"]