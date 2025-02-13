from datetime import timedelta
import polars as pl
from polars import col as c
from polars import selectors as cs
import pyomo.environ as pyo
import tqdm
import logging

from pyomo_models.input_data_preprocessing import (
    split_timestamps_per_sim, generate_second_stage_state
)
from utility.pyomo_preprocessing import (
    join_pyomo_variables, generate_datetime_index, extract_optimization_results, pivot_result_table,
    check_infeasible_constraints, generate_clean_timeseries
)
from general_function import pl_to_dict, pl_to_dict_with_tuple, generate_log

from pyomo_models.baseline.baseline_input import BaseLineInput
from pyomo_models.baseline.first_stage.first_stage_pipeline import BaselineFirstStage
from pyomo_models.baseline.second_stage.sets import baseline_sets
from pyomo_models.baseline.second_stage.parameters import baseline_parameters
from pyomo_models.baseline.second_stage.variables import baseline_variables
from pyomo_models.baseline.second_stage.objective import baseline_objective
from pyomo_models.baseline.second_stage.constraints.basin_volume import basin_volume_constraints
from pyomo_models.baseline.second_stage.constraints.powered_volume import powered_volume_constraints
from pyomo_models.baseline.second_stage.constraints.discrete_hydro import discrete_hydro_constraints



log = generate_log(name=__name__)

class BaselineSecondStage(BaseLineInput):
    def __init__(
        self, input_instance: BaseLineInput, first_stage: BaselineFirstStage, timestep: timedelta, model_nb: int = 1,
        buffer: float = 0.2, error_threshold: float = 0.1, powered_volume_enabled: bool = True,
        quantile: float = 0.15, spilled_factor: float = 1e2, with_penalty: bool = True, log_solver_info: bool = False,
        global_price: bool = False, time_limit:float =  120, is_parallel: bool = False
        ):
        self.retrieve_input(input_instance)
        self.sim_nb = 0
        self.sim_tot = 0
        self.is_parallel = is_parallel
        self.model_nb = model_nb
        self.error_threshold = error_threshold
        self.buffer: float = buffer
        self.powered_volume_enabled = powered_volume_enabled
        self.quantile = quantile
        self.spilled_factor = spilled_factor
        self.global_price = global_price
        self.with_penalty = with_penalty
        self.log_solver_info = log_solver_info
        self.timestep = timestep
        self.divisors: int = int(self.timestep / self.real_timestep)
        
        self.index: dict[str, pl.DataFrame] = first_stage.index
        self.first_stage_timestep: timedelta = first_stage.timestep
        self.water_flow_factor: pl.DataFrame = first_stage.water_flow_factor
        self.first_stage_results: pl.DataFrame = first_stage.simulation_results
        self.power_performance_table: list = first_stage.power_performance_table
        
        self.result_flow: pl.DataFrame = pl.DataFrame()
        self.result_power: pl.DataFrame = pl.DataFrame()
        self.result_basin_volume: pl.DataFrame = pl.DataFrame()
        self.result_spilled_volume: pl.DataFrame = pl.DataFrame()
        self.log_book: pl.DataFrame = pl.DataFrame()
        self.data: dict = {}
        
        
        self.solver.options['TimeLimit'] = time_limit
        
        self.generate_index()
        self.initialise_volume()
        self.get_alpha_boundaries()
        self.calculate_powered_volume()
        self.generate_volume_buffer()
        self.process_timeseries()
        self.generate_model()
        self.generate_constant_parameters()
        

    def initialise_volume(self):
        self.start_basin_volume = self.index["water_basin"].select(
            c("B"),
            pl.lit(self.sim_nb ) .alias("sim_nb"),
            c("start_volume").alias("start_basin_volume")
        )
        self.remaining_volume = self.index["hydro_power_plant"]\
            .select(
                c("H"),
                pl.lit(self.sim_nb).alias("sim_nb"), 
                pl.lit(0).alias("remaining_volume")
            )
    
    def generate_model(self):
        self.model: pyo.AbstractModel = pyo.AbstractModel()   
        self.model = baseline_sets(self.model)
        self.model = baseline_parameters(self.model)
        self.model = baseline_variables(self.model)
        
        self.model = baseline_objective(self.model)
        self.model = basin_volume_constraints(self.model)
        self.model = powered_volume_constraints(self.model)
        self.model = discrete_hydro_constraints(self.model)
        
    def retrieve_input(self, input_instance):
        for name, value in input_instance.__dict__.items():
            if name != "small_flex_input_schema":
                setattr(self, name, value)
    
    def calculate_powered_volume(self):
        
        divisors: int = int(self.timestep / self.first_stage_timestep)

        offset = divisors - self.first_stage_results.height%divisors
        self.powered_volume = self.first_stage_results.select(
                c("T"), 
                cs.starts_with("hydro")
                .map_elements(lambda x: x["turbined_volume"]  - x["pumped_volume"], return_dtype=pl.Float64)
                .name.map(lambda c: c.replace("hydro_", "")),
            ).group_by(((c("T") + offset)//divisors).alias("sim_nb"), maintain_order=True)\
            .agg(pl.all().exclude("sim_nb", "T").sum())\
            .unpivot(
                index="sim_nb", variable_name="H", value_name="powered_volume"
            ).with_columns(
                c("H").cast(pl.UInt32).alias("H")
            )
        
    def get_alpha_boundaries(self):

        self.alpha_pos: dict[int, float] = {}
        self.alpha_neg: dict[int, float] = {}
        for data in self.power_performance_table:
            alpha = data["power_performance"].select(cs.contains("alpha"))
            self.alpha_pos[data["H"]] = alpha.select(pl.min_horizontal(pl.all()).alias("min"))["min"].min()
            self.alpha_neg[data["H"]] = alpha.select(pl.max_horizontal(pl.all()).alias("max"))["max"].max()    
            
    def generate_index(self):
        
        divisors: int = int(self.timestep / self.real_timestep)
        
        datetime_index= generate_datetime_index(
            min_datetime=self.min_datetime, 
            max_datetime=self.max_datetime, 
            real_timestep=self.real_timestep, 
        )

        self.index["datetime"] = split_timestamps_per_sim(data=datetime_index, divisors=divisors)
        
        self.sim_tot: int = self.index["datetime"]["sim_nb"].max()  # type: ignore

    def generate_constant_parameters(self):
        
        self.data["H"] = {None: self.index["hydro_power_plant"]["H"].to_list()}
        self.data["B"] = {None: self.index["water_basin"]["B"].to_list()}
        self.data["buffer"] = {None: self.buffer}
        self.data["water_factor"] = pl_to_dict_with_tuple(self.water_flow_factor["BH", "turbined_factor"])
        self.data["alpha_pos"] = self.alpha_pos
        self.data["alpha_neg"] = self.alpha_neg
        self.data["volume_factor"] = {None: self.volume_factor}
        self.data["spilled_factor"] = dict(map(lambda x: (x["B"], self.spilled_factor), self.power_performance_table))
        if self.global_price:
            self.data["neg_unpowered_price"] = {
                None: self.market_price["avg"].quantile(0.5 + self.quantile)}
            self.data["pos_unpowered_price"] = {
                None: self.market_price["avg"].quantile(0.5 - self.quantile)}
            
    def generate_volume_buffer(self):
        rated_volume_dict = pl_to_dict(
            self.index["hydro_power_plant"]
            .select(
                c("H").cast(pl.Utf8), 
                c("rated_flow") * self.real_timestep.total_seconds() * self.volume_factor * self.buffer
            )
        )

        volume_buffer: pl.DataFrame = self.index["datetime"]

        self.volume_buffer = volume_buffer.group_by("sim_nb", maintain_order=True).agg(c("sim_nb").count().alias("nb_timestep"))\
            .with_columns(
                (c("nb_timestep") * volume).alias(str(name))  
                for name, volume in rated_volume_dict.items()
            ).unpivot(
                index=["sim_nb"], on=list(rated_volume_dict.keys()),
                variable_name="H", value_name="volume_buffer"
            ).with_columns(
                c("H").cast(pl.UInt32),
            )
        
            
    def process_timeseries(self):
        ### Discharge_flow ##############################################################################################
        discharge_volume: pl.DataFrame = generate_clean_timeseries(
            data=self.discharge_flow_measurement,
            col_name="discharge_volume", 
            min_datetime=self.min_datetime,
            max_datetime=self.max_datetime,
            timestep=self.real_timestep , 
            agg_type="sum"
        )

        self.discharge_volume = split_timestamps_per_sim(data=discharge_volume, divisors=self.divisors)\
            .with_columns(
                pl.lit(0).alias("B")
            ).with_columns(
                pl.concat_list(["T", "B"]).alias("TB")
        )
        ### Market price ###############################################################################################
        market_price: pl.DataFrame = generate_clean_timeseries(
            data=self.market_price_measurement,
            col_name="avg", 
            min_datetime=self.min_datetime, 
            max_datetime=self.max_datetime, 
            timestep=self.real_timestep, 
            agg_type="mean"
        )
        self.market_price = split_timestamps_per_sim(data=market_price, divisors=self.divisors)
    
    
    def generate_state_index(self):
        start_volume_dict = pl_to_dict(
            self.start_basin_volume.filter(c("sim_nb") == self.sim_nb)[["B", "start_basin_volume"]])

        discharge_volume_tot= pl_to_dict(
            self.discharge_volume.filter(c("sim_nb") == self.sim_nb).group_by("B").agg(c("discharge_volume").sum()))

        self.index = generate_second_stage_state(
            index=self.index, power_performance_table=self.power_performance_table, 
            discharge_volume=discharge_volume_tot, start_volume_dict=start_volume_dict,
            timestep=self.timestep, error_threshold=self.error_threshold, volume_factor=self.volume_factor)

    def extract_result(self):

        flow = extract_optimization_results(
                model_instance=self.model_instance, var_name="flow"
            ).with_columns(
                pl.lit(self.sim_nb).alias("sim_nb")
            )
        power = extract_optimization_results(
                model_instance=self.model_instance, var_name="power"
            ).with_columns(
                pl.lit(self.sim_nb).alias("sim_nb")
            )

        basin_volume = extract_optimization_results(
                model_instance=self.model_instance, var_name="basin_volume"
            ).with_columns(
                pl.lit(self.sim_nb).alias("sim_nb")
            )
        
        spilled_volume = extract_optimization_results(
                model_instance=self.model_instance, var_name="spilled_volume"
            ).with_columns(
                pl.lit(self.sim_nb).alias("sim_nb")
            )
                
        start_basin_volume = extract_optimization_results(
                model_instance=self.model_instance, var_name="end_basin_volume"
            ).with_columns(
                pl.lit(self.sim_nb + 1).alias("sim_nb")
            ).rename({"end_basin_volume": "start_basin_volume"})

        remaining_volume = join_pyomo_variables(
                model_instance=self.model_instance, 
                var_list=["diff_volume_pos", "diff_volume_neg"], 
                index_list=["H"]
            ).select(
                c("H"),
                pl.lit(self.sim_nb + 1).alias("sim_nb"),
                (c("diff_volume_pos") - c("diff_volume_neg")).alias("remaining_volume"),
            )
        
        self.start_basin_volume = pl.concat([self.start_basin_volume , start_basin_volume], how="diagonal_relaxed")

        
        self.remaining_volume = pl.concat([self.remaining_volume, remaining_volume], how="diagonal_relaxed")     
        self.result_flow = pl.concat([self.result_flow, flow], how="diagonal_relaxed")
        self.result_power = pl.concat([self.result_power, power], how="diagonal_relaxed")
        self.result_spilled_volume = pl.concat([self.result_spilled_volume, spilled_volume], how="diagonal_relaxed")
        self.result_basin_volume = pl.concat([self.result_basin_volume, basin_volume], how="diagonal_relaxed")

    def generate_model_instance(self):
        hydropower_state: pl.DataFrame = self.index["state"].drop_nulls("H")
    
        self.data["T"] = {None: self.index["datetime"].filter(c("sim_nb") == self.sim_nb)["T"].to_list()}
        self.data["S_B"] = pl_to_dict(
            self.index["state"].unique("S", keep="first")
            .group_by("B", maintain_order=True).agg("S")
            .with_columns(c("S").list.sort())
        )
        self.data["BS"] = {None: list(map(tuple,self.index["state"]["BS"].to_list()))}
        self.data["HS"] = {None: list(map(tuple,hydropower_state["HS"].to_list()))}
        self.data["HQS"] = {None: list(map(tuple,hydropower_state["HQS"].to_list()))}
        self.data["S_H"] = pl_to_dict(
            hydropower_state.unique("S", keep="first")
            .group_by("H", maintain_order=True)
            .agg("S")
            .with_columns(c("S").list.sort())
            )
        self.data["S_Q"] = pl_to_dict_with_tuple(
            hydropower_state
            .group_by(["HS"], maintain_order=True).agg("S_Q")
            .with_columns(c("S_Q").list.sort())
            )
        
        self.data["B_H"] = pl_to_dict(hydropower_state.group_by("H").agg(c("B").unique()))
        self.data["SB_H"] = pl_to_dict_with_tuple(hydropower_state.group_by("HS").agg(c("S").unique()))
        
        self.data["start_basin_volume"] = pl_to_dict(
            self.start_basin_volume.filter(c("sim_nb") == self.sim_nb)[["B", "start_basin_volume"]])
        self.data["remaining_volume"] = pl_to_dict(self.remaining_volume.filter(c("sim_nb") == self.sim_nb)[["H", "remaining_volume"]])
        self.data["min_basin_volume"] = pl_to_dict_with_tuple(
                    self.index["state"].select("BS", c("volume").struct.field("min")))
        self.data["max_basin_volume"] = pl_to_dict_with_tuple(
            self.index["state"].select("BS", c("volume").struct.field("max")))
        self.data["powered_volume_enabled"] = {None: self.powered_volume_enabled}

        self.data["discharge_volume"] = pl_to_dict_with_tuple(self.discharge_volume.filter(c("sim_nb") == self.sim_nb)[["TB", "discharge_volume"]])  
        self.data["market_price"] = pl_to_dict(self.market_price.filter(c("sim_nb") == self.sim_nb)[["T", "avg"]])
        if not self.global_price:
            self.data["neg_unpowered_price"] = {
                None: self.market_price.filter(c("sim_nb") == self.sim_nb)["avg"].quantile(0.5 + self.quantile)}
            self.data["pos_unpowered_price"] = {
                None: self.market_price.filter(c("sim_nb") == self.sim_nb)["avg"].quantile(0.5 - self.quantile)}
        
        self.data["powered_volume"] = pl_to_dict(self.powered_volume.filter(c("sim_nb") == self.sim_nb)[["H", "powered_volume"]])
        self.data["volume_buffer"] = pl_to_dict(self.volume_buffer.filter(c("sim_nb") == self.sim_nb)[["H", "volume_buffer"]])

        self.data["min_flow"] = pl_to_dict_with_tuple(hydropower_state[["HQS", "flow"]])  
        self.data["min_power"] = pl_to_dict_with_tuple(hydropower_state[["HQS", "electrical_power"]])  
        self.data["d_flow"] = pl_to_dict_with_tuple(hydropower_state[["HQS", "d_flow"]])  
        self.data["d_power"] = pl_to_dict_with_tuple(hydropower_state[["HQS", "d_electrical_power"]])  
        
        self.model_instance: pyo.Model = self.model.create_instance({None: self.data})

    def finalizes_results_processing(self):
        remaining_volume = pivot_result_table(
            df = self.remaining_volume, on="H", index="sim_nb", 
            values="remaining_volume")

        powered_volume = pivot_result_table(
            df = self.powered_volume, on="H", index="sim_nb", 
            values="powered_volume"
            ).with_columns(
                c("sim_nb").cast(pl.Int32).alias("sim_nb")
            )

        real_powered_volume = pivot_result_table(
            df = self.result_flow
                .group_by("sim_nb", "H")
                .agg((c("flow").sum() * self.real_timestep.total_seconds() * self.volume_factor).alias("real_powered_volume")),
            on="H", index="sim_nb", 
            values="real_powered_volume")
        
                
        start_basin_volume = pivot_result_table(
            df = self.start_basin_volume,
            on="B", index="sim_nb", 
            values="start_basin_volume")
        
        volume = self.result_flow.with_columns((c("flow") * self.real_timestep.total_seconds()).alias("volume"))
        volume = pivot_result_table(
            df = volume, on="H", index=["T", "sim_nb"], 
            values="volume", reindex=True)

        power = pivot_result_table(
            df = self.result_power, on="H", index=["T", "sim_nb"], 
            values="power", reindex=True)
        
        volume_max_mapping: dict[str, float] = pl_to_dict(self.index["water_basin"][["B", "volume_max"]])
        basin_volume = self.result_basin_volume.with_columns(
            (c("basin_volume") / c("B").replace_strict(volume_max_mapping, default=None)).alias("basin_volume")
        )

        basin_volume = pivot_result_table(
            df = basin_volume, on="B", index=["T", "sim_nb"], 
            values="basin_volume", reindex=True)
        
        spilled_volume = pivot_result_table(
            df = self.result_spilled_volume, on="B", index=["T", "sim_nb"], 
            values="spilled_volume", reindex=True)

        market_price = self.market_price.with_row_index(name="real_index")\
            .select(c("real_index"), c("avg").alias("market_price"))

        self.simulation_summary = remaining_volume\
            .join(powered_volume, on = "sim_nb", how="inner")\
            .join(real_powered_volume, on = "sim_nb", how="inner")\
            .join(start_basin_volume, on = "sim_nb", how="inner")
            
        self.simulation_results = self.index["datetime"]\
            .with_row_index(name="real_index")[["real_index", "timestamp"]]\
            .join(basin_volume, on = "real_index", how="inner")\
            .join(volume, on = "real_index", how="inner")\
            .join(power, on = "real_index", how="inner")\
            .join(spilled_volume, on = "real_index", how="inner")\
            .join(market_price, on = "real_index", how="inner")\
            .with_columns(
                (pl.sum_horizontal(cs.starts_with("power")) * c("market_price")).alias("income"),
            )

    def solve_model(self):
        logging.getLogger('pyomo.core').setLevel(logging.ERROR)
        for sim_nb in tqdm.tqdm(
            range(self.sim_tot + 1), 
            desc=f"Solving second stage optimization model number {self.model_nb}",
            position=self.model_nb if self.is_parallel else 0,
            ncols=150,
            leave=True
        ):
            self.sim_nb = sim_nb
            self.generate_state_index()
            self.generate_model_instance()
            solution = self.solver.solve(self.model_instance, tee=self.log_solver_info)
            if solution["Solver"][0]["Status"] == "ok":
                self.extract_result()
            elif solution["Solver"][0]["Status"] == "aborted":
                self.log_mip_gap(solution)
                self.extract_result()
            else:
                solved = self.solve_changing_powered_volume_constraint()
                if not solved:
                    break
                
        logging.getLogger('pyomo.core').setLevel(logging.WARNING)        
        

    def calculated_feasibility(self):
        start_basin_volume = self.start_basin_volume.filter(c("sim_nb") == self.sim_nb)[["B", "start_basin_volume"]]
        remaining_volume = self.remaining_volume.filter(c("sim_nb") == self.sim_nb)[["H", "remaining_volume"]]
        powered_volume = self.powered_volume.filter(c("sim_nb") == self.sim_nb)[["H", "powered_volume"]]
        volume_buffer = self.volume_buffer.filter(c("sim_nb") == self.sim_nb)[["H", "volume_buffer"]]
        basin_power_mapping =pl.from_dicts(self.power_performance_table).drop("power_performance").with_columns(
            pl.all().cast(pl.UInt32)
        )

        print(start_basin_volume\
            .join(basin_power_mapping, on="B", how="inner")\
            .join(remaining_volume, on="H", how="inner")\
            .join(powered_volume, on="H", how="inner")\
            .join(volume_buffer, on="H", how="inner")\
            .join(self.index["water_basin"][["B", "volume_max", "volume_min"]], on="B", how="inner")\
            .with_columns(
                (c("start_basin_volume") - c("powered_volume") + c("volume_buffer") -
                pl.when(c("remaining_volume") < 0).then(c("remaining_volume")).otherwise(0)
                ).alias("pos_boundary"),
                (c("start_basin_volume") - c("powered_volume") - c("volume_buffer") -
                pl.when(c("remaining_volume") > 0).then(c("remaining_volume")).otherwise(0)
                ).alias("neg_boundary"),
            ).to_dicts()[0]
        )
        
    def solve_changing_powered_volume_constraint(self) -> bool:
        if self.powered_volume_enabled == True:
            self.powered_volume_enabled = False
            self.generate_model_instance()   
            solution = self.solver.solve(self.model_instance, tee=self.log_solver_info)
            if solution["Solver"][0]["Status"] == "ok":
                self.extract_result()
            elif solution["Solver"][0]["Status"] == "aborted":
                self.log_mip_gap(solution)
                self.extract_result()
            else:
                self.infeasible_constraints = check_infeasible_constraints(model=self.model_instance)
                self.calculated_feasibility()
                log.warning(f"Second stage optimization problem is infeasible for sim_nb: {self.sim_nb}")
                return False
        else:
            self.infeasible_constraints = check_infeasible_constraints(model=self.model_instance)
            self.calculated_feasibility()
            log.warning(f"Second stage optimization problem is infeasible for sim_nb: {self.sim_nb}")
            return False
        self.powered_volume_enabled = True
        return True

    def log_unbounded(self):
        self.log_book = pl.concat([
            self.log_book,
            pl.DataFrame(
                {
                    "sim_nb": [self.sim_nb],
                    "unbounded": [True]
                }
            )
        ], how="diagonal_relaxed")
        
    def log_mip_gap(self, solution):
        self.log_book = pl.concat([
            self.log_book,
            pl.DataFrame(
                {
                    "sim_nb": [self.sim_nb],
                    "lower_bound": [solution["Problem"][0]["Lower bound"]],
                    "upper_bound": [solution["Problem"][0]["Upper bound"]]
                }
            )
        ], how="diagonal_relaxed")

    def solve_one_instance(self, sim_nb: int):
        self.sim_nb = sim_nb
        if sim_nb == self.sim_tot:
            self.powered_volume_enabled = False
        self.generate_state_index()
        self.generate_model_instance()
        _ = self.solver.solve(self.model_instance)
        self.extract_result()

    

