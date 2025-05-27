import faust
import math
import pandas as pd
from datetime import timedelta


BOOTSTRAP_SERVERS = 'localhost:9092'
YELLOW_TAXI_TOPIC = 'yellow-taxi'
WINDOW_SIZE_SECONDS = 300  
WINDOW_EXPIRES_SECONDS = 360

LOCATION_TO_BOROUGH = pd.read_csv('location_to_borough.csv').set_index('LocationID')['Borough'].to_dict()

CHOSEN_LOCATIONS = [
    132,  # JFK Airport
    138,  # LaGuardia Airport
    230,  # Times Square
    161,  # Midtown Center
    237,  # Upper East Side South
    79,   # East Harlem South
    186,  # Penn Station / Madison Sq West
    249,  # West Village
    90,   # Flatiron
    48,   # Clinton East
]

app = faust.App(
    'taxi-stats-consumer',
    broker=f'kafka://{BOOTSTRAP_SERVERS}',
    value_serializer='json',
    store='rocksdb://',  
)


class TaxiRide(faust.Record, serializer='json'):
    pickup_datetime: str
    dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    distance: float
    base_fare: float
    passenger_count: int 
    payment_type: int

class StatsData(faust.Record):
    count: int = 0
    distance_sum: float = 0.0
    distance_sq_sum: float = 0.0
    fare_sum: float = 0.0
    fare_sq_sum: float = 0.0
    passenger_sum: int = 0
    passenger_sq_sum: int = 0


taxi_topic = app.topic(YELLOW_TAXI_TOPIC, value_type=TaxiRide)

# Tumbling windows: Non-overlapping, fixed-size windows.
borough_stats_table = app.Table(
    'borough_stats',
    default=StatsData,
    key_type=str,
    value_type=StatsData,
    partitions=1
).tumbling(
    timedelta(seconds=WINDOW_SIZE_SECONDS),
    expires=timedelta(seconds=WINDOW_EXPIRES_SECONDS),
    key_index=True 
)

location_stats_table = app.Table(
    'location_stats',
    default=StatsData,
    key_type=int,
    value_type=StatsData,
    partitions=1
).tumbling(
    timedelta(seconds=WINDOW_SIZE_SECONDS),
    expires=timedelta(seconds=WINDOW_EXPIRES_SECONDS),
    key_index=True
)

def calculate_and_format_stats(stats, name):
    if stats.count == 0:
        return f"{name}: No data in current window."

    mean_dist = stats.distance_sum / stats.count
    mean_fare = stats.fare_sum / stats.count
    mean_pass = stats.passenger_sum / stats.count

    std_dist = math.sqrt(max(0, (stats.distance_sq_sum / stats.count) - (mean_dist ** 2)))
    std_fare = math.sqrt(max(0, (stats.fare_sq_sum / stats.count) - (mean_fare ** 2)))
    std_pass = math.sqrt(max(0, (stats.passenger_sq_sum / stats.count) - (mean_pass ** 2)))

    return (
        f"--- {name} (Window) ---\n"
        f"  Count: {stats.count}\n"
        f"  Distance: Mean={mean_dist:.2f}, StdDev={std_dist:.2f}\n"
        f"  Fare:     Mean={mean_fare:.2f}, StdDev={std_fare:.2f}\n"
        f"  Passengers: Mean={mean_pass:.2f}, StdDev={std_pass:.2f}\n"
    )


@app.agent(taxi_topic)
async def process_taxi_ride(stream):
    async for ride in stream:
        location_id = int(ride.PULocationID)
        borough = LOCATION_TO_BOROUGH.get(location_id, "Unknown")

        current_borough_stats = borough_stats_table[borough].value()
        current_borough_stats.count += 1
        current_borough_stats.distance_sum += ride.distance
        current_borough_stats.distance_sq_sum += ride.distance ** 2
        current_borough_stats.fare_sum += ride.base_fare
        current_borough_stats.fare_sq_sum += ride.base_fare ** 2
        current_borough_stats.passenger_sum += ride.passenger_count
        current_borough_stats.passenger_sq_sum += ride.passenger_count ** 2
        borough_stats_table[borough] = current_borough_stats

        if location_id in CHOSEN_LOCATIONS:
            current_location_stats = location_stats_table[location_id].value()
            current_location_stats.count += 1
            current_location_stats.distance_sum += ride.distance
            current_location_stats.distance_sq_sum += ride.distance ** 2
            current_location_stats.fare_sum += ride.base_fare
            current_location_stats.fare_sq_sum += ride.base_fare ** 2
            current_location_stats.passenger_sum += ride.passenger_count
            current_location_stats.passenger_sq_sum += ride.passenger_count ** 2
            location_stats_table[location_id] = current_location_stats


@app.timer(interval=30.0)  
async def print_stats():
    print("\n" + "=" * 40)
    print(f"ROLLING STATISTICS (Window: {WINDOW_SIZE_SECONDS}s)")
    print("=" * 40)

    print("\n## Borough Statistics ##")
    known_boroughs = set(LOCATION_TO_BOROUGH.values())
    for borough in known_boroughs:
        stats = borough_stats_table[borough].now() 
        print(f"{stats}")
        if stats.count > 0:
            print(calculate_and_format_stats(stats, f"Borough: {borough}"))

    print("\n## Chosen Location Statistics ##")
    for loc_id in CHOSEN_LOCATIONS:
        stats = location_stats_table[loc_id].now()
        if stats.count > 0:
            print(calculate_and_format_stats(stats, f"Location ID: {loc_id}"))

    print("=" * 40 + "\n")


if __name__ == '__main__':
    app.main()