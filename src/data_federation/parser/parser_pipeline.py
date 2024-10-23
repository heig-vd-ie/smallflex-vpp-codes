
import json

import tqdm
from data_federation.input_model import SmallflexInputSchema

from data_federation.parser.market_price import parse_market_price
from data_federation.parser.discharge_flow import parse_discharge_flow
from data_federation.parser.weather import parse_weather
from data_federation.parser.aegina_hydro_power_plant import parse_aegina_hydro_power_plant
from data_federation.parser.merezenbach_hydro_power_plant import parse_merezenbach_hydro_power_plant
from data_federation.parser.morel_hydro_power_plant import parse_morel_hydro_power_plant
from data_federation.parser.aegina_wind_power_plant import parse_aegina_wind_power_plant

from config import settings


def input_pipeline():
    input_file_names: dict[str, str] = json.load(open(settings.INPUT_FILE_NAMES))
    output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES))
    with tqdm.tqdm(total=4, desc="Parse small flex input data") as pbar:
        kwargs: dict = {"small_flex_input_schema": SmallflexInputSchema(), "input_file_names": input_file_names}
        kwargs["small_flex_input_schema"] = parse_market_price(**kwargs)
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_discharge_flow(**kwargs)
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_weather(**kwargs, area="greis")
        pbar.update()
        kwargs["small_flex_input_schema"] = parse_aegina_hydro_power_plant(**kwargs)
        kwargs["small_flex_input_schema"] = parse_merezenbach_hydro_power_plant(**kwargs)
        kwargs["small_flex_input_schema"] = parse_morel_hydro_power_plant(**kwargs)
        kwargs["small_flex_input_schema"] = parse_aegina_wind_power_plant(**kwargs)
        pbar.update()
        
    small_flex_input_schema: SmallflexInputSchema = kwargs["small_flex_input_schema"]
    small_flex_input_schema.schema_to_duckdb(output_file_names["duckdb_input"])
    
    return small_flex_input_schema