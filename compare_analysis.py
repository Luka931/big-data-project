#!/usr/bin/env python
import os, time, glob, contextlib, textwrap, duckdb, pandas as pd
import dask.dataframe as dd
from dask_sql import Context

BASE = "/Users/amadej/Desktop/big_data/assignment5/big-data-project/data/sample_merged_output"
PARQUET_GLOB = f"{BASE}/part*.parquet"
PARQUET = sorted(glob.glob(PARQUET_GLOB))

CSV_DIR = f"{BASE}/csv_cache"
os.makedirs(CSV_DIR, exist_ok=True)
CSV_GLOB = f"{CSV_DIR}/part*.csv"

# export once to CSV
for p in PARQUET:
    stem = os.path.basename(p)[:-8]
    out = f"{CSV_DIR}/{stem}.csv"
    if not os.path.exists(out):
        pd.read_parquet(p).to_csv(out, index=False)
CSV = sorted(glob.glob(CSV_GLOB))

# helper timer
results = []
def timer(label):
    @contextlib.contextmanager
    def _t():
        t0 = time.perf_counter(); yield
        results.append(dict(query=qid, engine=label, fmt=fmt,
                            sec=time.perf_counter()-t0))
    return _t()

# three queries
#def q1(df): return df["trip_distance"].mean()
#def q2(df): return df.groupby(["borough_pickup","borough_dropoff"]).size().nlargest(10)
#def q3(df): return df.groupby(df["tpep_pickup_datetime"].dt.hour)["tip_amount"].mean()
# three queries
def q1(df):
    return df["trip_distance"].mean()

def q2(df):
    return df.groupby(["borough_pickup", "borough_dropoff"]).size().nlargest(10)

def q3(df):
    # extract hour into its own column first, so .dt is applied on a Series
    df2 = df.assign(
        hour=dd.to_datetime(df["tpep_pickup_datetime"]).dt.hour
    )
    return df2.groupby("hour")["tip_amount"].mean()

queries = {"Q1": q1, "Q2": q2, "Q3": q3}
sql = {
    "Q1": "SELECT AVG(trip_distance) FROM df",
    "Q2": """SELECT borough_pickup, borough_dropoff, COUNT(*) trips
             FROM df GROUP BY 1,2 ORDER BY trips DESC LIMIT 10""",
    "Q3": """SELECT EXTRACT(hour FROM tpep_pickup_datetime) hr,
                    AVG(tip_amount) avg_tip
             FROM df GROUP BY hr""",
}

# benchmark loop
for fmt, paths, glob_pat in [("parquet", PARQUET, PARQUET_GLOB),
                             ("csv",     CSV,     CSV_GLOB)]:

    # DuckDB (read all via glob)
    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW df AS SELECT * FROM read_{fmt}('{glob_pat}')")
    for qid in queries:
        with timer("duckdb"):
            con.execute(sql[qid]).fetchall()

    # Pandas
    #pdf = pd.concat([pd.read_parquet(p) if fmt=="parquet"
    #                 else pd.read_csv(p) for p in paths])
        # Pandas
    if fmt == "parquet":
        pdf = pd.concat([pd.read_parquet(p) for p in paths])
    else:
        pdf = pd.concat([
            pd.read_csv(
                p,
                low_memory=False,
                dtype={"store_and_fwd_flag": "category"},
                parse_dates=["tpep_pickup_datetime"],
                usecols=[
                    "tpep_pickup_datetime",
                    "trip_distance",
                    "tip_amount",
                    "borough_pickup",
                    "borough_dropoff"
                ],
            )
            for p in paths
        ])

    for qid,f in queries.items():
        with timer("pandas"): f(pdf)

    # Dask DataFrame
    ddf = dd.read_parquet(paths) if fmt=="parquet" else dd.read_csv(paths)
    for qid,f in queries.items():
        with timer("dask"): f(ddf).compute()

    # Dask‑SQL
    ctx = Context(); # ctx.create_table("df", ddf)
    #for qid in queries:
    #    with timer("dask-sql"): ctx.sql(sql[qid]).compute()
    ctx.create_table(
    "df", 
    "data/sample_merged_output/part.*.parquet"   # single string, not a list
)
for qid in queries:
    with timer("dask-sql"):
        ctx.sql(sql[qid]).compute()

# show result table
tbl = pd.DataFrame(results)
print("\nWall‑time (seconds) on three sample files:\n")
print(tbl.pivot_table(index=["engine","fmt"], columns="query",
                      values="sec").round(3))
