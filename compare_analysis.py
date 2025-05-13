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
    ctx = Context()
    ctx.create_table("df", PARQUET_GLOB)   # <-- a single string
    for qid in QUERIES:
        with ProgressBar(), timer("dask-sql", qid, fmt):
            ctx.sql(SQL[qid]).compute()


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
#                Q1      Q2      Q3
# dask     csv   4.922   5.194   7.056
#          parquet 0.110   2.336   0.230
# dask-sql csv   0.241   2.591   0.681
#          parquet 0.234   2.832   0.525
# duckdb   csv   1.076   1.123   1.138
#          parquet 0.012   0.035   0.041
# pandas   csv   0.007   0.673   0.378
#          parquet 0.015   1.034   0.655
#
# Key takeaways:
# 1) Parquet versus CSV:
#    - Dask and DuckDB see huge wins with Parquet over CSV.
#    - Pandas still reads CSV slightly faster for simple scans, but group-bys tip the balance toward Parquet.
#    - Dask-SQL sits between: it benefits from Parquet but has extra SQL-layer cost.
#
# 2) Comparing engines on Parquet:
#    - DuckDB is unbeatable on Parquet (Q1 in 0.012 s, Q2 in 0.035 s, Q3 in 0.041 s).
#    - Dask (pure) is next: parallelism cuts Q3 to 0.230 s.
#    - Dask-SQL adds flexibility but pays a premium (Q2 takes 2.832 s vs. 2.336 s in raw Dask).
#    - Pandas holds its own on simple queries (Q1 0.015 s) but slows on group-bys (Q2 1.034 s).
#
# 3) Comparing engines on CSV:
#    - Pandas excels at Q1 on CSV (0.007 s) and remains competitive for simple group-bys.
#    - DuckDB on CSV is consistent (~1.1 s across all queries).
#    - Dask-SQL on CSV (Q1 0.241 s) outperforms DuckDB on Q1, but for grouping (Q2/Q3) it’s slower.
#    - Pure Dask on CSV is the slowest (5–7 s range), due to parsing and scheduling overhead.
#
# Conclusion:
# - For quick ad-hoc lookups on Parquet, DuckDB is the clear winner.
# - For scale-out or larger-than-memory workloads on Parquet, raw Dask is a strong candidate.
# - Dask-SQL offers SQL syntax over Dask but with measurable overhead—best when you need that integration.
# - Pandas remains great for small CSV analyses, but switch to columnar formats and DuckDB or Dask when queries get heavier.
