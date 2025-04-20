#!/usr/bin/env python
# zones_analysis.py

import os
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import holidays
import dask.dataframe as dd

# --- Debug prints to verify working directory and data layout ---
print("→ os.getcwd():", os.getcwd())
script_path = Path(__file__).resolve()
print("→ Script file:", script_path)
print("→ Script parent:", script_path.parent)
data_dir = script_path.parent / "data"
print("→ Exists ‘data/’? ", data_dir.exists())
if data_dir.exists():
    print("→ data/ contents:", sorted(os.listdir(data_dir)))
else:
    print("→ (no data/ folder here)")
print("-" * 60)

# Ensure we run from project root
BASE_DIR = script_path.parent
os.chdir(BASE_DIR)
print("→ After chdir, cwd is now:", os.getcwd())
print("-" * 60)

# --- Load and prepare static GIS data ---
taxi_zones = gpd.read_file("data/taxi-zones/taxi_zones.shp")
schools = (
    gpd.read_file("data/school-locations/SchoolPoints_APS_2024_08_28.shp")
       .to_crs(taxi_zones.crs)
)
schools_per_zone = (
    gpd.sjoin(schools, taxi_zones, how="inner", predicate="within")
      .groupby("LocationID").size()
      .reset_index(name='number_of_schools')
)

def extract_coords(wkt):
    try:
        coords = wkt.split('(')[1].split(')')[0].split()
        return Point(float(coords[0]), float(coords[1]))
    except:
        return None

poi_df = pd.read_csv("data/points-of-interest.csv")
poi_df["geometry"] = poi_df["the_geom"].apply(extract_coords)
poi_gdf = (
    gpd.GeoDataFrame(poi_df, geometry="geometry", crs="EPSG:4326")
       .to_crs(taxi_zones.crs)
)
pois_per_zone = (
    gpd.sjoin(poi_gdf, taxi_zones, how="inner", predicate="within")
      .groupby("LocationID").size()
      .reset_index(name='number_of_pois')
)

univ_df = pd.read_csv("data/universities.csv")
univ_df["geometry"] = univ_df["the_geom"].apply(extract_coords)
univ_gdf = (
    gpd.GeoDataFrame(univ_df, geometry="geometry", crs="EPSG:4326")
       .to_crs(taxi_zones.crs)
)
unis_per_zone = (
    gpd.sjoin(univ_gdf, taxi_zones, how="inner", predicate="within")
      .groupby("LocationID").size()
      .reset_index(name='number_of_universities')
)

# Merge counts into a DataFrame
merged = taxi_zones[["LocationID", "zone", "borough"]].copy()
for df_count, colname in [
    (schools_per_zone, "number_of_schools"),
    (pois_per_zone,    "number_of_pois"),
    (unis_per_zone,    "number_of_universities")
]:
    merged = merged.merge(df_count, on="LocationID", how="left")
    merged[colname] = merged[colname].fillna(0).astype(int)

zones_final = merged[[
    "LocationID", "zone", "borough",
    "number_of_schools", "number_of_pois", "number_of_universities"
]]
zones_final.to_csv("zones_final.csv", index=False)

# --- Dask-based join with large taxi and weather tables ---
taxi_data    = dd.read_parquet("data/taxi-data/*.parquet")
zones_dd     = dd.read_csv("zones_final.csv")
weather_dd   = dd.read_csv("data/weather-data.csv", skiprows=3)

def is_holiday(ds):
    us_hols = holidays.US()
    def check(d):
        return int(d in us_hols) if pd.notna(d) else 0
    return ds.apply(check, meta=("is_holiday", "int64"))

taxi_data["tpep_pickup_datetime_rounded"] = (
    taxi_data["tpep_pickup_datetime"].dt.round("h")
)
weather_dd["time"] = (
    dd.to_datetime(weather_dd["time"]).dt.round("h")
)
taxi_data["is_holiday"] = is_holiday(taxi_data["tpep_pickup_datetime_rounded"])

df = (
    taxi_data
      .merge(zones_dd, left_on="PULocationID", right_on="LocationID", how="left")
      .merge(zones_dd, left_on="DOLocationID", right_on="LocationID",
             how="left", suffixes=("_pickup","_dropoff"))
      .merge(weather_dd, left_on="tpep_pickup_datetime_rounded",
             right_on="time", how="left")
)

# Drop columns we no longer need
df = df.drop(
    columns=[
        "LocationID_pickup", "LocationID_dropoff",
        "time", "Unnamed: 0_pickup", "Unnamed: 0_dropoff",
        "airport_fee"
    ],
    errors="ignore"
)

# --- Checkpointing to avoid OOM ---
# Repartition into ~200MB chunks on disk
df = df.repartition(partition_size="200MB")
# Write out an on-disk checkpoint
df.to_parquet("checkpoint/zones_taxi_weather", write_index=False)
# Free the original lineage & reload
del df
df = dd.read_parquet("checkpoint/zones_taxi_weather")

# Now safely persist or compute on smaller partitions
zones_ddf = df.persist()
print(zones_ddf.head())

# Final write-out (creates many part-files)
zones_ddf.to_csv("output/zones_taxi_weather_*.csv", single_file=False)
