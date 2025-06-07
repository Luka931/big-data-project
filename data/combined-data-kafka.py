import duckdb


def main():
    yellow_taxi_data_path = "data/trip_record_data_filtered/yellow_taxi/*.parquet"
    high_volume_data_path = "data/trip_record_data_filtered/high_volume/*.parquet"

    con = duckdb.connect("data/ducdb.db")

    high_volume_sql = f"""
    select  pickup_datetime, 
            dropoff_datetime, 
            PULocationID, 
            DOLocationID, 
            trip_miles as trip_distance, 
            base_passenger_fare as fare_amount,
            'High Volume For Hire' as source
        from '{high_volume_data_path}'
        where year == 2021

    """

    yellow_taxi_sql = f"""
    select  tpep_pickup_datetime as pickup_datetime, 
            tpep_dropoff_datetime as dropoff_datetime,
            PULocationID, 
            DOLocationID, 
            trip_distance, 
            fare_amount,
            'Yellow Taxi' as source
        from '{yellow_taxi_data_path}' t
        where t.year == 2021
    """

    con.sql(
        f"create table combinded_data as {high_volume_sql} union all by name {yellow_taxi_sql} order by pickup_datetime"
    )


if __name__ == "__main__":
    main()
