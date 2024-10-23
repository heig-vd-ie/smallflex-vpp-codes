import os
import polars as pl
from polars import col as c

from data_federation.input_model import SmallflexInputSchema

from utility.general_function import generate_uuid


POWER_PLANT_UUID: str = generate_uuid(base_value="Aegina wind power plant")
TURBINE1_UUID: str = generate_uuid(base_value="Aegina wind turbine 1")
TURBINE2_UUID: str = generate_uuid(base_value="Aegina wind turbine 2")
TURBINE3_UUID: str = generate_uuid(base_value="Aegina wind turbine 3")
TURBINE4_UUID: str = generate_uuid(base_value="Aegina wind turbine 4")
RESOURCE_LIST: list[str] = [TURBINE1_UUID, TURBINE2_UUID, TURBINE3_UUID, TURBINE4_UUID]

COL_MAPPING = {"Average": "avg", "Minimum": "min", "Maximum": "max"}
AMBIGUOUS_MAPPING = {True: "earliest", False: "latest"}
UNIT_MAPPING = {"A": "current", "kV": "voltage", "MW": "active_power", "Mvar": "reactive_power"}

def parse_aegina_wind_resources(small_flex_input_schema: SmallflexInputSchema, **kwargs) -> SmallflexInputSchema:
    
    wind_power_plant: pl.DataFrame = pl.from_dicts([{
        "name": "Aegina wind power plant", "uuid": POWER_PLANT_UUID, "resource_fk_list": RESOURCE_LIST, 
        "rated_power": 8,
    }])

    resource: pl.DataFrame = pl.from_dicts([
        {
            "name": "Aegina wind turbine 1", "uuid": TURBINE1_UUID, "type": "wind_turbine", "rated_power": 2, 
            "power_plant_fk": POWER_PLANT_UUID,
        }, {
            "name": "Aegina wind turbine 2", "uuid": TURBINE2_UUID, "type": "wind_turbine", "rated_power": 2, 
            "power_plant_fk": POWER_PLANT_UUID
        }, {
            "name": "Aegina wind turbine 3", "uuid": TURBINE3_UUID, "type": "wind_turbine", "rated_power": 2,
            "power_plant_fk": POWER_PLANT_UUID 
        }, {
            "name": "Aegina wind turbine 4", "uuid": TURBINE4_UUID, "type": "wind_turbine", "rated_power": 2,
            "power_plant_fk": POWER_PLANT_UUID 
        }
    ])
    return small_flex_input_schema.add_table(wind_power_plant=wind_power_plant, resource=resource)

def parse_aegina_wind_power_production(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str], **kwargs
    ) -> SmallflexInputSchema:
    
    power_production_measurement: pl.DataFrame = pl.DataFrame()

    for entry in list(os.scandir(input_file_names["aegina_wind_turbine_production"])):
        try:
            data: pl.DataFrame = pl.read_csv(entry.path, separator=";", infer_schema_length=0)
        except Exception as e:
            data: pl.DataFrame = pl.read_csv(entry.path, separator=",", infer_schema_length=0)

        data = data.filter(~c("TagName").str.contains("Hauptgruppe"))\
            .rename(COL_MAPPING)\
            .with_columns(
            pl.struct("DateTime", "Unit").is_first_distinct()
            .replace_strict(AMBIGUOUS_MAPPING, default=None).alias("ambiguous")
            ).with_columns(
                c("DateTime").str.to_datetime("%m/%d/%Y %I:%M:%S%.f %p", time_zone="Europe/Zurich", ambiguous=c("ambiguous"))
                .dt.convert_time_zone(time_zone="UTC").alias("timestamp"),
                c("Unit").replace_strict(UNIT_MAPPING, default=None),
                c(list(COL_MAPPING.values())).cast(pl.Float64)
            ).pivot(
                on="Unit", index="timestamp", values=list(COL_MAPPING.values())
            ).with_columns(
                pl.lit(POWER_PLANT_UUID).alias("power_plant_fk"),
            )
            
        power_production_measurement = pl.concat([power_production_measurement, data],  how="diagonal_relaxed")
        
    return small_flex_input_schema.add_table(power_production_measurement=power_production_measurement)

def parse_aegina_wind_power_plant(
    small_flex_input_schema:SmallflexInputSchema, input_file_names: dict[str, str]
    ) -> SmallflexInputSchema:
    
    kwargs = {"small_flex_input_schema": small_flex_input_schema, "input_file_names": input_file_names}
    
    # kwargs["small_flex_input_schema"] = parse_aegina_wind_resources(**kwargs)
    kwargs["small_flex_input_schema"] = parse_aegina_wind_power_production(**kwargs)
    
    return kwargs["small_flex_input_schema"]