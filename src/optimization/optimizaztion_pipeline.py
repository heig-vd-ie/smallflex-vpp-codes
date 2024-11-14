import pyomo.environ as pyo
import json
from datetime import timedelta


from optimization_model.first_model_baseline import generate_baseline_model
from optimization_model.input_data_prepocessing import generate_first_problem_input_data
from data_display.first_stage_optimization_plots import plot_first_stage_summarized
from data_federation.input_model import SmallflexInputSchema

from config import settings

def first_stage_pipeline(year: int, nb_days: int, max_flow_factor: float, min_flow_factor: float, n_segments: int):

    output_file_names: dict[str, str] = json.load(open(settings.OUTPUT_FILE_NAMES))

    small_flex_input_schema: SmallflexInputSchema = SmallflexInputSchema()\
    .duckdb_to_schema(file_path=output_file_names["duckdb_input"])
    
    model: pyo.AbstractModel = generate_baseline_model()
    data = generate_first_problem_input_data(
        small_flex_input_schema=small_flex_input_schema,
        hydro_power_plant_name = "Aegina hydro",
        year = year,
        n_segments = n_segments,
        max_flow_factor=max_flow_factor,
        min_flow_factor=min_flow_factor,
        first_time_delta = timedelta(days=nb_days)
    )

    solver = pyo.SolverFactory('gurobi')

    model_instance = model.create_instance({None: data})
    res = solver.solve(model_instance, load_solutions=True, tee=False)

    plot_first_stage_summarized(model_instance=model_instance,max_flow_factor=max_flow_factor, min_flow_factor=min_flow_factor, nb_days=nb_days, year=year)
    
    return model_instance