
import polars as pl
from polars import col as c
from polars import selectors as cs
from datetime import timedelta, datetime, timezone

from data_federation.input_model import SmallflexInputSchema
from typing_extensions import Optional
from utility.pyomo_preprocessing import (
    generate_datetime_index, generate_segments, generate_clean_timeseries, arange_float, filter_data_with_next,
    linear_interpolation_for_bound, arange_float, linear_interpolation_using_cols,generate_state_index_using_errors, 
    filter_by_index, get_min_avg_max_diff, define_state)
from utility.general_function import pl_to_dict, generate_log


log = generate_log(name=__name__)

def generate_baseline_index(
    small_flex_input_schema: SmallflexInputSchema, year: int, 
    real_timestep: timedelta, volume_factor: float,  timestep: Optional[timedelta] = None, 
    hydro_power_mask: Optional[pl.Expr]= None
    ):
    
    min_datetime: datetime = datetime(year, 1, 1, tzinfo=timezone.utc)
    max_datetime: datetime = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    if hydro_power_mask is None:
            hydro_power_mask= pl.lit(True)  
    index: dict[str, pl.DataFrame] = {}
    index["datetime"] = generate_datetime_index(
            min_datetime=min_datetime, max_datetime=max_datetime, model_timestep=timestep, 
            real_timestep=real_timestep
        )
    index["hydro_power_plant"] = small_flex_input_schema.hydro_power_plant\
            .filter(hydro_power_mask).with_row_index(name="H")
    index["water_basin"] = small_flex_input_schema.water_basin\
            .filter(c("power_plant_fk").is_in(index["hydro_power_plant"]["uuid"]))\
            .with_columns(
                c("volume_max", "volume_min", "start_volume")*volume_factor
            ).with_row_index(name="B")
    return index

def generate_water_flow_factor(index: dict[str, pl.DataFrame]) -> pl.DataFrame:

    water_volume_mapping = {
        "upstream_basin_fk" : {"pumped_factor": 1, "turbined_factor": -1},
        "downstream_basin_fk" : {"pumped_factor": -1, "turbined_factor": 1}
    }

    water_flow_factor = index["hydro_power_plant"]\
        .unpivot(on=["upstream_basin_fk", "downstream_basin_fk"], index="H", value_name="uuid", variable_name="situation")\
        .with_columns(
                c("situation").replace_strict(water_volume_mapping, default=None).alias("water_volume")
        ).unnest("water_volume").join(index["water_basin"][["uuid", "B"]], on="uuid", how="left").with_columns(
            pl.concat_list("B", "H").alias("BH")
        )

    return water_flow_factor

def generate_basin_volume_table(
    small_flex_input_schema: SmallflexInputSchema, index: dict[str, pl.DataFrame], volume_factor: float, 
    d_height: float = 1,
    
    )-> dict[int, Optional[pl.DataFrame]]:
    

    volume_table: dict[int, Optional[pl.DataFrame]] = {}

    for water_basin_index in index["water_basin"].to_dicts():

        basin_height_volume_table: pl.DataFrame = small_flex_input_schema.basin_height_volume_table\
                .filter(c("water_basin_fk") == water_basin_index["uuid"])\
                .with_columns(
                    (c("volume") * volume_factor).alias("volume")
                )
        if basin_height_volume_table.is_empty():
            volume_table[water_basin_index["B"]] = None
            continue
        height_min: float= water_basin_index["height_min"] if water_basin_index["height_min"] is not None else basin_height_volume_table["height"].min() # type: ignore
        height_max: float= water_basin_index["height_max"] if water_basin_index["height_max"] is not None else basin_height_volume_table["height"].max() # type: ignore

        volume_table[water_basin_index["B"]] = arange_float(height_max, height_min, d_height)\
            .to_frame(name="height")\
            .join(basin_height_volume_table, on ="height", how="full", coalesce=True)\
            .sort("height")\
            .interpolate()\
            .with_columns(
                linear_interpolation_for_bound(x_col=c("height"), y_col=c("volume")).alias("volume")
            ).drop_nulls("height")[["height", "volume"]]\
            .filter(c("height").ge(height_min).and_(c("height").le(height_max)))\
            
            
    return volume_table

def clean_hydro_power_performance_table(
    small_flex_input_schema: SmallflexInputSchema, index: dict[str, pl.DataFrame], 
    basin_volume_table: dict[int, Optional[pl.DataFrame]]
    ) -> list[dict]:


    power_performance_table: list[dict] = []
    for power_plant_data in index["hydro_power_plant"].to_dicts():
        downstream_basin = index["water_basin"].filter(c("uuid") == power_plant_data["downstream_basin_fk"]).to_dicts()[0]
        upstream_basin = index["water_basin"].filter(c("uuid") == power_plant_data["upstream_basin_fk"]).to_dicts()[0]

        power_plant_state: pl.DataFrame = small_flex_input_schema.power_plant_state\
            .filter(c("power_plant_fk") == power_plant_data["uuid"])

        volume_table = basin_volume_table[upstream_basin["B"]]
        if volume_table is None:
            continue
        
        state_dict = {}
        
        turbine_fk = small_flex_input_schema.resource\
            .filter(c("uuid").is_in(power_plant_data["resource_fk_list"]))\
            .filter(c("type") == "hydro_turbine")["uuid"].to_list()

        state_list = [
                ("turbined", list(map(lambda uuid: uuid in turbine_fk, power_plant_data["resource_fk_list"]))), 
                ("pumped", list(map(lambda uuid: uuid not in turbine_fk, power_plant_data["resource_fk_list"])))
            ]
        for name, state in state_list:
            state_dict[power_plant_state.filter(c("resource_state_list") == state)["uuid"][0]] = name
            
        power_performance: pl.DataFrame = small_flex_input_schema.hydro_power_performance_table\
            .with_columns(
                (c("head") + downstream_basin["height_max"]).alias("height"),
            )

        power_performance = power_performance.with_columns(
                c("power_plant_state_fk").replace_strict(state_dict, default=None).alias("state_name")
            ).drop_nulls("state_name").pivot(
                values=["flow" , "electrical_power"], on="state_name", index="height"
            ).join(volume_table, on ="height", how="full", coalesce=True)\
            .sort("height")
        
        power_performance = linear_interpolation_using_cols(
            df=power_performance, 
            x_col="height", 
            y_col=power_performance.select(pl.all().exclude("height", "volume")).columns
            ).filter(c("height").ge(volume_table["height"].min()).and_(c("height").le(volume_table["height"].max())))

        power_performance = power_performance.with_columns(
            (c("electrical_power_" + state[0]) / c("flow_" + state[0])).alias("alpha_" + state[0]) 
            for state in state_list
        )
            
        power_performance_table.append({
            "H": power_plant_data["H"], "B": upstream_basin["B"], "power_performance": power_performance
            })
            
    return  power_performance_table
            
def generate_hydro_power_state(
    power_performance_table: list[dict], index: dict[str, pl.DataFrame], error_percent: float
    ) -> dict[str, pl.DataFrame]: 
    
    state_index: pl.DataFrame = pl.DataFrame()
    for data in power_performance_table: 
        power_performance: pl.DataFrame = data["power_performance"]
        y_cols: list[str] = power_performance.select(cs.starts_with("alpha")).columns
        
        segments = generate_state_index_using_errors(data=power_performance, column_list=y_cols, error_percent=error_percent)
        if len(segments) > 10:
            log.warning(f"{len(segments)} states found in {data["H"]} hydro power plant")
        state_performance_table = filter_by_index(data=power_performance, index_list=segments)\
        .with_columns(
            c(col).abs().pipe(get_min_avg_max_diff).alias(col) for col in power_performance.columns
        ).slice(offset=0, length=len(segments)-1) 
        
        state_index = pl.concat([
            state_index , 
            state_performance_table.with_columns(
                pl.lit(data["H"]).alias("H"),
                pl.lit(data["B"]).alias("B"))
            ], how="diagonal")
        # Add every downstream basin
    missing_basin: pl.DataFrame = index["water_basin"].filter(~c("B").is_in(state_index["B"])).select(
        c("B"),
        pl.struct(
            c("volume_min").fill_null(0.0).alias("min"), 
            c("volume_max").fill_null(0.0).alias("max"),
            (c("volume_max").fill_null(0.0) + c("volume_min").fill_null(0.0)/2).alias("avg"),
            (c("volume_max").fill_null(0.0) - c("volume_min").fill_null(0.0)).alias("diff"),
        ).alias("volume")
    )
    state_index = pl.concat([state_index, missing_basin], how="diagonal_relaxed")
    index["state"] = state_index.with_row_index(name="S").with_columns(
        pl.concat_list("H", "B", "S", "S").alias("S_BH"),
        pl.concat_list("B", "S").alias("BS"),
        pl.concat_list("H", "S").alias("HS")
    )
        
    return index

def split_timestamps_per_sim(data: pl.DataFrame, divisors: int, col_name: str = "T") -> pl.DataFrame:
    
    offset = data.height%divisors
    # if offset != 0:
    #     offset: int = divisors - data.height%divisors
    return(
        data.with_columns(
            ((c(col_name) + offset)//divisors).alias("sim_nb")
        ).with_columns(
            c(col_name).cum_count().over("sim_nb").alias(col_name)
        )
    )
    
def generate_second_stage_state(
    index: dict[str, pl.DataFrame], power_performance_table: list[dict],
    start_volume_dict: dict, discharge_volume:dict,
    timestep: timedelta, error_threshold: float, volume_factor: float
    ) -> dict[str, pl.DataFrame]:
    
    rated_flow_dict = pl_to_dict(index["hydro_power_plant"][["H", "rated_flow"]])
    state_index: pl.DataFrame = pl.DataFrame()
    start_state: int = 0
    for performance_table in power_performance_table:
        start_volume = start_volume_dict[performance_table["B"]]
        rated_volume = rated_flow_dict[performance_table["H"]] * timestep.total_seconds() * volume_factor
        
        boundaries = (
            start_volume - rated_volume, start_volume + rated_volume + discharge_volume[performance_table["B"]]
        )
        data: pl.DataFrame = filter_data_with_next(
            data=performance_table["power_performance"], col="volume", boundaries=boundaries)
        y_cols = data.select(cs.starts_with(name) for name in ["flow", "electrical"]).columns
        state_name_list = set(map(lambda x : x.split("_")[-1], y_cols))
        
        data = define_state(data=data, x_col="volume", y_cols=y_cols, error_threshold=error_threshold)\
            .with_row_index(offset=start_state, name="S")\
            .with_columns(
                    pl.lit(performance_table["H"]).alias("H"),
                    pl.lit(performance_table["B"]).alias("B")
            ).with_columns(
                pl.struct(cs.ends_with(col_name)).name.map_fields(lambda x: "_".join(x.split("_")[:-1])).alias(col_name)
                for col_name in list(state_name_list)
            ).with_columns(
                pl.struct(
                    pl.lit(0).alias(col_name) 
                    for col_name in ["flow","d_flow", "electrical_power", "d_electrical_power" ]
                ).alias("off")  
            ).unpivot(
                on=list(state_name_list) + ["off"], index= ["volume", "S", "H", "B"], value_name="data", variable_name="state"
            ).unnest("data").drop("state")
            
        state_index = pl.concat([state_index, data], how="diagonal")
        start_state = state_index["S"].max() + 1 # type: ignore
        
    state_index = state_index.with_row_index(name="F")    
    missing_basin: pl.DataFrame = index["water_basin"]\
        .filter(~c("B").is_in(state_index["B"]))\
        .select(
            c("B"),
            pl.struct(
                c("volume_min").fill_null(0.0).alias("min"), 
                c("volume_max").fill_null(0.0).alias("max"),
            ).alias("volume")
        ).with_row_index(offset=start_state, name="S")

    index["state"] = pl.concat([state_index, missing_basin], how="diagonal_relaxed")\
        .with_columns(
        pl.concat_list("H", "B", "S", "S").alias("S_BH"),
        pl.concat_list("B", "S").alias("BS"),
        pl.concat_list("H", "S").alias("HS"),
        pl.concat_list("H", "S", "F").alias("HSF")
    )
    return index