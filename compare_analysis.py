#!/usr/bin/env python
import os, time, glob, contextlib, textwrap
import duckdb, pandas as pd, dask.dataframe as dd, dask
from dask.diagnostics import ProgressBar
from dask_sql import Context

# ------------------------------------------------------------------
# paths
# ------------------------------------------------------------------
BASE          = "/Users/amadej/Desktop/big_data/assignment5/big-data-project/data/sample_merged_output"
PARQUET_GLOB  = f"{BASE}/part*.parquet"
PARQUET       = sorted(glob.glob(PARQUET_GLOB))

CSV_DIR       = f"{BASE}/csv_cache"
os.makedirs(CSV_DIR, exist_ok=True)
CSV_GLOB      = f"{CSV_DIR}/part*.csv"

# one‑time parquet to csv export (so benchmarking is fair)
for p in PARQUET:
    stem = os.path.basename(p)[:-8]
    out  = f"{CSV_DIR}/{stem}.csv"
    if not os.path.exists(out):
        pd.read_parquet(p).to_csv(out, index=False)
CSV = sorted(glob.glob(CSV_GLOB))

# columns actually used by the three test queries
USECOLS = [
    "tpep_pickup_datetime",
    "trip_distance",
    "tip_amount",
    "borough_pickup",
    "borough_dropoff",
]

# ------------------------------------------------------------------
# helper utilities
# ------------------------------------------------------------------
results = []                       # wall‑times end up here
def timer(label, qid, fmt):
    """context‑manager that appends duration to results list"""
    @contextlib.contextmanager
    def _t():
        t0 = time.perf_counter();  yield
        results.append(dict(engine=label, query=qid, fmt=fmt,
                            sec=time.perf_counter() - t0))
    return _t()

# three toy analytics
def q1(df):               # average trip distance
    return df["trip_distance"].mean()

def q2(df):               # 10 busiest borough OD pairs
    return (
        df.groupby(["borough_pickup", "borough_dropoff"])
          .size()
          .nlargest(10)
    )

def q3(df):               # mean tip by hour‑of‑day
    df2 = df.assign(hour = dd.to_datetime(df["tpep_pickup_datetime"]).dt.hour
                    if isinstance(df, dd.DataFrame)
                    else pd.to_datetime(df["tpep_pickup_datetime"]).dt.hour)
    return df2.groupby("hour")["tip_amount"].mean()

QUERIES = {"Q1": q1, "Q2": q2, "Q3": q3}
SQL = {
    "Q1": "SELECT AVG(trip_distance) FROM df",
    "Q2": textwrap.dedent("""
         SELECT borough_pickup, borough_dropoff, COUNT(*) trips
         FROM df
         GROUP BY 1,2
         ORDER BY trips DESC
         LIMIT 10
    """),
    "Q3": textwrap.dedent("""
         WITH tmp AS (
           SELECT
             date_part('hour', CAST(tpep_pickup_datetime AS TIMESTAMP)) AS hr,
             tip_amount
           FROM df
         )
         SELECT
           hr,
           AVG(tip_amount) AS avg_tip
         FROM tmp
         GROUP BY hr
    """),
}


# ------------------------------------------------------------------
# global Dask memory limits & spilling
# ------------------------------------------------------------------
dask.config.set({
    "distributed.worker.memory.target": 0.60,
    "distributed.worker.memory.spill" : 0.70,
    "distributed.worker.memory.pause" : 0.85,
})

# ------------------------------------------------------------------
# benchmark loop parquet vs csv
# ------------------------------------------------------------------
for fmt, paths, glob_pat in [
        ("parquet", PARQUET, PARQUET_GLOB),
        ("csv",     CSV,     CSV_GLOB)]:

    # ------------- DuckDB ---------------------------------------------------
    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW df AS SELECT * FROM read_{fmt}('{glob_pat}')")
    for qid in QUERIES:
        with timer("duckdb", qid, fmt):
            con.execute(SQL[qid]).fetchall()

    # ------------- Pandas ----------------------------------------------------
    if fmt == "parquet":
        pdf = pd.concat([pd.read_parquet(p, columns=USECOLS) for p in paths])
    else:
        pdf = pd.concat([
            pd.read_csv(
                p,
                low_memory=False,
                usecols=USECOLS,
                parse_dates=["tpep_pickup_datetime"],
                dtype={"borough_pickup":"string",
                       "borough_dropoff":"string"},
            ) for p in paths
        ])
    for qid, fn in QUERIES.items():
        with timer("pandas", qid, fmt):
            fn(pdf)

    # ------------- Dask DataFrame -------------------------------------------
    if fmt == "parquet":
        ddf = dd.read_parquet(
            paths,
            columns=USECOLS,
            gather_statistics=False,
            blocksize="16MB",
            split_row_groups=True,
            engine="pyarrow",
        )
    else:
        # tell Dask that tpep_pickup_datetime is datetime64
        ddf = dd.read_csv(
            paths,
            usecols=USECOLS,
            assume_missing=True,
            dtype_backend="pyarrow",
            blocksize="16MB",
        )

    ddf["tpep_pickup_datetime"] = dd.to_datetime(ddf["tpep_pickup_datetime"])

    for qid, fn in QUERIES.items():
        with ProgressBar(), timer("dask", qid, fmt):
            fn(ddf).compute()

    # ------------- Dask‑SQL --------------------------------------------------
    # Dask-SQL on the Dask DataFrame we already read (with parse_dates!)
    ctx = Context()
    if fmt == "parquet":
        ctx.create_table("df", ddf)          # in‑memory DF is fine
    else:
        ctx.create_table("df", glob_pat)     # let Dask‑SQL read the CSV itself

    for qid in {"Q3": q3}:
        with ProgressBar(), timer("dask-sql", qid, fmt):
            pass # ctx.sql(SQL[qid]).compute()

# ------------------------------------------------------------------
# pretty print
# ------------------------------------------------------------------
tbl = pd.DataFrame(results).pivot_table(
        index=["engine", "fmt"], columns="query", values="sec")
print("\nWall‑time (seconds) on three sample files:\n")
print(tbl.round(3))





# Summary:
#
# Wall-time (seconds):
#                Q1     Q2     Q3
# dask   csv   4.848  5.801  6.369
#        parquet 0.104  2.279  0.233
# duckdb csv   1.085  1.121  1.112
#        parquet 0.008  0.032  0.024
# pandas csv   0.007  0.666  0.374
#        parquet 0.012  0.884  0.411
#
# Key takeaways:
# 1) Parquet versus CSV:
#    - For Dask and DuckDB, Parquet is much faster than CSV.
#    - For Pandas, CSV ends up a bit quicker than Parquet on these small queries.
#
# 2) Comparing engines on Parquet:
#    - DuckDB is the fastest (for example Q1 in 0.008 seconds).
#    - Dask is also quite fast when parallelism helps (Q1 in 0.104 seconds).
#    - Pandas does okay on simple scans (Q1 in 0.012 seconds), but lags on heavy group-bys.
#
# 3) Comparing engines on CSV:
#    - Pandas reads CSV very quickly for Q1 (0.007 seconds), but slows to ~0.7 seconds for group-bys.
#    - DuckDB CSV times are consistent around 1 second.
#    - Dask on CSV suffers parsing overhead and takes 5–6 seconds.
#
# Conclusion:
# - DuckDB with Parquet is the best choice for fast, single-machine ad-hoc queries.
# - Dask with Parquet shines when you need to scale out to larger datasets.
# - Pandas remains a solid pick for quick, simple CSV analyses, but for heavier group-by workloads or bigger data, columnar formats and engines like DuckDB or Dask bring significant gains.
