import os
import polars as pl
from polars import col as c

from data_federation.input_model import SmallflexInputSchema
from data_federation.parser.hydro_power_plant import get_hydro_power_plant_data

from utility.general_function import generate_uuid
from utility.polars_operation import generate_uuid_col

from itertools import product

# Define the UUIDs
POWER_PLANT_UUID: str = generate_uuid(base_value="Aegina hydro power plant")
TURBINE_UUID: str = generate_uuid(base_value="Aegina turbine")
PUMP1_UUID: str = generate_uuid(base_value="Aegina pump 1")
PUMP2_UUID: str = generate_uuid(base_value="Aegina pump 2")
UPSTREAM_UUID: str = generate_uuid(base_value="Aegina upstream basin")
DOWNSTREAM_UUID: str = generate_uuid(base_value="Aegina downstream basin")
RESOURCE_MAPPING: dict[str, str] = {"Pe_GR1": TURBINE_UUID, "Pe_GR2": PUMP1_UUID, "Pe_GR3": PUMP2_UUID}


def parse_aegina_water_resources(small_flex_input_schema: SmallflexInputSchema, **kwargs) -> tuple[SmallflexInputSchema, pl.DataFrame] :

    hydro_power_plant: pl.DataFrame = pl.from_dicts([{
            "name": "Aegina hydro power plant", "uuid": POWER_PLANT_UUID, "resource_fk_list": list(RESOURCE_MAPPING.values()), 
            "upstream_basin_fk": UPSTREAM_UUID, "downstream_basin_fk": DOWNSTREAM_UUID,
            "rated_power": 9.2, "rated_flow": 2.8, 
            "control": "discrete", "type": "buildup_pump_turbine",
        }])

    resource: pl.DataFrame = pl.from_dicts([
        {
            "name": "Aegina turbine", "uuid": TURBINE_UUID, "type": "hydro_turbine", "rated_power": 9.2, 
            "power_plant_fk": POWER_PLANT_UUID,
        }, {
            "name": "Aegina pump 1", "uuid": PUMP1_UUID, "type": "hydro_pump", "rated_power": 6, 
            "power_plant_fk": POWER_PLANT_UUID, "installed": False
        }, {
            "name": "Aegina pump 2", "uuid": PUMP2_UUID, "type": "hydro_pump", "rated_power": 6,
            "power_plant_fk": POWER_PLANT_UUID, "installed": False
            
        }
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
            "name": "Aegina upstream basin", "uuid": UPSTREAM_UUID, "power_plant_fk": POWER_PLANT_UUID,
            "volume_max": 15.8e6, "volume_min": 56.996e3, "height_max": 2382, "height_min": 2340
        }, {
            "name": "Aegina downstream basin", "uuid": DOWNSTREAM_UUID, "power_plant_fk": POWER_PLANT_UUID,
            "volume_max": 1e6, "height_max": 1970, 
        }
    ])
    
    return (
        small_flex_input_schema.add_table(
            hydro_power_plant=hydro_power_plant, resource=resource, power_plant_state=power_plant_state,
            water_basin=water_basin
        ), power_plant_state
    )
    
def parse_aegina_basin_height_volume_table(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], **kwargs) -> SmallflexInputSchema:
    file_name = input_file_names["aegina_water_volume_table"]

    basin_height_volume_table: pl.DataFrame = pl.read_csv(file_name, separator=",").with_columns(
        pl.lit(UPSTREAM_UUID).alias("water_basin_fk"),
    )
    return small_flex_input_schema.add_table(basin_height_volume_table=basin_height_volume_table)
        
def parse_aegina_basin_height_data(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], **kwargs) -> SmallflexInputSchema:
    file_name = input_file_names["aegina_basin_height_data"]

    basin_height_measurement: pl.DataFrame = pl.read_csv(file_name, separator=";", infer_schema_length=0)\
        .select(
            c("Date").str.to_datetime(format="%d.%m.%Y", time_zone="UTC").alias("timestamp"),
            c("Last").str.replace(",", ".").cast(pl.Float64).alias("height"),
            pl.lit(UPSTREAM_UUID).alias("water_basin_fk")
        )
        

    return small_flex_input_schema.add_table(basin_height_measurement=basin_height_measurement)

def parse_aegina_performance_table(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], 
    power_plant_state: pl.DataFrame, **kwargs) -> SmallflexInputSchema:

    hydro_power_performance_table: pl.DataFrame = pl.DataFrame()

    power_plant_state = power_plant_state.select(
        c("resource_state_list").cast(pl.List(pl.Utf8)).list.join(","),
        c("uuid").alias("power_plant_state_fk"), 
        "state_number" , "power_plant_fk"
    )

    for entry in list(os.scandir(input_file_names["aegina_pve_data"])):
    
        data, head = get_hydro_power_plant_data(file_path=entry.path) 
        data = data\
            .select(
                (c("Pet_HPP")/1e6).alias("electrical_power"), # Convert from W to MW
                c("Q_HPP").alias("flow"), # already in ms/s
                c("Eta_HPP").alias("efficiency"), # already in %
                pl.concat_list(c(RESOURCE_MAPPING.keys()) != 0).cast(pl.List(pl.Utf8)).list.join(",").alias("resource_state_list"),
            ).join(power_plant_state, on="resource_state_list", how="right")\
            .sort("efficiency", descending=True)\
            .unique("power_plant_state_fk").with_columns(
                pl.lit(head).alias("head"),
            )
        
        hydro_power_performance_table = pl.concat([hydro_power_performance_table, data],  how="diagonal_relaxed")
        
    return small_flex_input_schema.add_table(hydro_power_performance_table=hydro_power_performance_table)

def parse_aegina_hydro_power_plant(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]
    ) -> SmallflexInputSchema:
    
    kwargs = {"small_flex_input_schema": small_flex_input_schema, "input_file_names": input_file_names}
    
    kwargs["small_flex_input_schema"], kwargs["power_plant_state"] = parse_aegina_water_resources(**kwargs)
    kwargs["small_flex_input_schema"] = parse_aegina_basin_height_volume_table(**kwargs)
    kwargs["small_flex_input_schema"] = parse_aegina_basin_height_data(**kwargs)
    kwargs["small_flex_input_schema"] = parse_aegina_performance_table(**kwargs)
    
    return kwargs["small_flex_input_schema"]