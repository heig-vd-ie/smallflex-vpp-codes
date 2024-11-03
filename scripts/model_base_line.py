import pyomo.environ as pyo
import json
from datetime import timedelta
import polars as pl

from optimization_model.first_model_baseline import generate_abstract_model
from optimization_model.input_data_prepocessing import generate_first_problem_input_data
from data_federation.input_model import SmallflexInputSchema
from config import settings

if __name__=="__main__":
    
    output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES))

    small_flex_input_schema: SmallflexInputSchema = SmallflexInputSchema()\
    .duckdb_to_schema(file_path=output_file_names["duckdb_input"])
    model: pyo.AbstractModel = generate_abstract_model()
    data = generate_first_problem_input_data(
        small_flex_input_schema=small_flex_input_schema,
        hydro_power_plant_name = "Aegina hydro",
        year = 2020,
        first_time_delta = timedelta(days=3),
        second_time_delta = timedelta(minutes=60))
    
    solver = pyo.SolverFactory('gurobi')
    
    model_instance = model.create_instance({None: data})
    res = solver.solve(model_instance, load_solutions=True, tee=False)
    
    result = pl.DataFrame({
        col : getattr(model_instance, col).extract_values().values() for col in ["basin_height", "V_tot", "market_price"]
    }).with_row_index(name="index")
    
    print(result.to_pandas().to_string())