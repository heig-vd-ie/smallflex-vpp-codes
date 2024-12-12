import multiprocessing 
from time import sleep
import polars as pl
import tqdm
import pyomo.environ as pyo

def test_sub_process(df: pl.DataFrame, job_id, queue):
    
    model = pyo.AbstractModel()

    model.T = pyo.Set()
    model.I = pyo.Set()
    model.J = pyo.Set(model.I)
    
    @model.Constraint(model.I) # type: ignore
    def test(model, i):
        return model.z[i] == sum(model.x[i, j] for j in model.J[i])
    
    solver = pyo.SolverFactory('gurobi')
    df_filtered = df.filter(pl.col("a") > job_id)
    for i in tqdm.tqdm(range(0, 10), position=job_id):
        sleep(0.1) 
    queue.put(df_filtered)

def create_dataset():
    return pl.DataFrame({"a": [0, 1, 2, 3, 4], "b": [0, 4, 5, 56, 4]})


def setup():
    # some setup work
    df = create_dataset()
    df.write_parquet("/tmp/test.parquet")


def main():
    test_df = pl.read_parquet("/tmp/test.parquet")
    processes = []
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    for i in range(0, 5):
        proc = ctx.Process(
            target=test_sub_process, args=(test_df, i, queue)
        )
        proc.start()
        processes.append(proc)
    results = []
    
    for p in processes:
        p.join()
    for _ in processes:
        result = queue.get()
        print(result)
        results.append(result)
    print(results)

if __name__ == "__main__":
    setup()
    main()