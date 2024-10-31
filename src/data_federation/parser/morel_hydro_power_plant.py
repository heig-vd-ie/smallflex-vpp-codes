import os
import polars as pl
from polars import col as c

from data_federation.input_model import SmallflexInputSchema
from data_federation.parser.hydro_power_plant import get_hydro_power_plant_data

from utility.general_function import generate_uuid
from utility.polars_operation import generate_uuid_col

from itertools import product

# Define the UUIDs
POWER_PLANT_UUID: str = generate_uuid(base_value="Morel")
TURBINE1_UUID: str = generate_uuid(base_value="Morel turbine 1")
TURBINE2_UUID: str = generate_uuid(base_value="Morel turbine 2")
TURBINE3_UUID: str = generate_uuid(base_value="Morel turbine 3")
UPSTREAM_UUID: str = generate_uuid(base_value="Morel upstream basin")
DOWNSTREAM_UUID: str = generate_uuid(base_value="Morel downstream basin")

RESOURCE_MAPPING: dict[str, str] = {"Pe_GR1": TURBINE1_UUID, "Pe_GR2": TURBINE2_UUID, "Pe_GR3": TURBINE3_UUID}

def parse_morel_resources(small_flex_input_schema: SmallflexInputSchema, **kwargs) -> tuple[SmallflexInputSchema, pl.DataFrame] :

    hydro_power_plant: pl.DataFrame = pl.from_dicts([{
    "name": "Morel", "uuid": POWER_PLANT_UUID, "resource_fk_list": list(RESOURCE_MAPPING.values()), 
    "upstream_basin_fk": UPSTREAM_UUID, "downstream_basin_fk": DOWNSTREAM_UUID,"rated_power": 51, "rated_flow": 22,
    "control": "continuous", "type": "buildup_pump_turbine",
    }])

    resource: pl.DataFrame = pl.from_dicts([
        {
            "name": "Morel turbine 1", "uuid": TURBINE1_UUID, "type": "hydro_turbine", "rated_power": 17,  
            "power_plant_fk": POWER_PLANT_UUID
        }, {
            "name": "Morel turbine 2", "uuid": TURBINE2_UUID, "type": "hydro_turbine", "rated_power": 17, 
            "power_plant_fk": POWER_PLANT_UUID
        }, {
            "name": "Morel turbine 3", "uuid": TURBINE3_UUID, "type": "hydro_turbine", "rated_power": 17, 
            "power_plant_fk": POWER_PLANT_UUID}
    ])

    power_plant_state: pl.DataFrame = pl.DataFrame({
        "resource_state_list": list(map(list, product(*len(RESOURCE_MAPPING)*[(True, False)])))
        }).with_row_index(name="state_number")\
        .with_columns(
            pl.lit(POWER_PLANT_UUID).alias("power_plant_fk"),
            c("state_number").pipe(generate_uuid_col, added_string=POWER_PLANT_UUID).alias("uuid"),
        )
    water_basin: pl.DataFrame = pl.from_dicts([
        {
            "name": "Morel upstream basin", "uuid": UPSTREAM_UUID, "power_plant_fk": POWER_PLANT_UUID,
            "volume_max": 20e3, "volume_min": 0, "height_max": 985.15, "height_min": 981.36 
        }, {
            "name": "Morel downstream basin", "uuid": DOWNSTREAM_UUID, "power_plant_fk": POWER_PLANT_UUID,
            "height_max": 739.2, 
        }
    ])
    
    return (
        small_flex_input_schema.add_table(
            hydro_power_plant=hydro_power_plant, resource=resource, power_plant_state=power_plant_state,
            water_basin=water_basin
        ), power_plant_state
    )
    

def parse_morel_performance_table(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], 
    power_plant_state: pl.DataFrame, **kwargs) -> SmallflexInputSchema:

    hydro_power_performance_table: pl.DataFrame = pl.DataFrame()

    power_plant_state = power_plant_state.select(
        c("resource_state_list").cast(pl.List(pl.Utf8)).list.join(","),
        c("uuid").alias("power_plant_state_fk"), 
        "state_number" , "power_plant_fk"
    )

    for entry in list(os.scandir(input_file_names["morel_pve_data"])):

        data, head = get_hydro_power_plant_data(file_path=entry.path) 
        data = data\
            .select(
                c("Pet_HPP").alias("electrical_power"), # Convert from W to MW
                c("Q_HPP").alias("flow"), # already in ms/s
                c("Eta_HPP").alias("efficiency"), # already in %
                pl.concat_list(c(RESOURCE_MAPPING.keys()) != 0).cast(pl.List(pl.Utf8)).list.join(",").alias("resource_state_list"),
            ).join(power_plant_state, on="resource_state_list", how="left")\
            .with_columns(
                pl.lit(head).alias("head"),
            )
        
        hydro_power_performance_table = pl.concat([hydro_power_performance_table, data],  how="diagonal_relaxed")
        
    return small_flex_input_schema.add_table(hydro_power_performance_table=hydro_power_performance_table)

def parse_morel_hydro_power_plant(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]
    ) -> SmallflexInputSchema:
    
    kwargs = {"small_flex_input_schema": small_flex_input_schema, "input_file_names": input_file_names}
    
    kwargs["small_flex_input_schema"], kwargs["power_plant_state"] = parse_morel_resources(**kwargs)
    kwargs["small_flex_input_schema"] = parse_morel_performance_table(**kwargs)
    
    return kwargs["small_flex_input_schema"]