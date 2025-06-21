from confluent_kafka import Consumer, KafkaException
import json
import collections
from datetime import datetime
import pandas as pd
import threading
import statistics
import os


GROUP_ID = "my_confluent_consumer_group"

BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "combinded_data"

LOCATION_TO_BOROUGH = (
    pd.read_csv("kafka_assignment/location_to_borough.csv")
    .set_index("LocationID")["Borough"]
    .to_dict()
)

CHOSEN_LOCATIONS = [
    132,  # JFK Airport
    138,  # LaGuardia Airport
    230,  # Times Square
    161,  # Midtown Center
    237,  # Upper East Side South
    79,  # East Harlem South
    186,  # Penn Station / Madison Sq West
    249,  # West Village
    90,  # Flatiron
    48,  # Clinton East
]


def create_consumer():
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
    }
    try:
        consumer = Consumer(conf)
        print("Confluent Kafka Consumer initialized successfully.")
        return consumer
    except Exception as e:
        print(f"Error initializing Confluent Kafka Consumer: {e}")
        return None


def get_dict_of_deques(keys, window_size):
    dictionary = {}
    for key in keys:
        dictionary[key] = {
            "trip_time": collections.deque([], window_size),
            "trip_distance": collections.deque([], window_size),
            "fare_amount": collections.deque([], window_size),
        }
    return dictionary


def make_dirs():
    for loc in set(CHOSEN_LOCATIONS):
        os.makedirs(f"T6_kafka/results/{loc}", exist_ok=True)
    for borough in set(LOCATION_TO_BOROUGH.values()):
        os.makedirs(f"T6_kafka/results/{borough}", exist_ok=True)


def save_to_file(name, dict):
    for key, values in dict.items():
        file_path = f"T6_kafka/results/{name}/{key}.csv"
        if len(values) == 0:
            continue
        with open(file_path, "a") as f:
            f.write(f"{statistics.mean(values)},\t{statistics.stdev(values)}\n")


def consume_messages(consumer, window_size=1000):
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Topic: {KAFKA_TOPIC}, Group: {GROUP_ID}")
    make_dirs()

    borough_windows = get_dict_of_deques(set(LOCATION_TO_BOROUGH.values()), window_size)
    location_windows = get_dict_of_deques(set(CHOSEN_LOCATIONS), window_size)

    def print_statistics():
        threading.Timer(10, print_statistics).start()

        for borough, values in borough_windows.items():
            save_to_file(borough, values)

        for location, values in location_windows.items():
            save_to_file(location, values)

    print_statistics()

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            decoded_value = json.loads(msg.value().decode("utf-8"))

            pickup_datetime = datetime.strptime(
                decoded_value["pickup_datetime"], "%Y-%m-%d %H:%M:%S"
            )
            dropoff_datetime = datetime.strptime(
                decoded_value["dropoff_datetime"], "%Y-%m-%d %H:%M:%S"
            )
            trip_distance = decoded_value["trip_distance"]
            fare_amount = decoded_value["fare_amount"]
            pickup_location = decoded_value["PULocationID"]
            pickup_borough = LOCATION_TO_BOROUGH.get(pickup_location)

            borough_windows[pickup_borough]["trip_time"].appendleft(
                (dropoff_datetime - pickup_datetime).total_seconds()
            )
            borough_windows[pickup_borough]["trip_distance"].appendleft(trip_distance)
            borough_windows[pickup_borough]["fare_amount"].appendleft(fare_amount)

            if pickup_location in location_windows:
                location_windows[pickup_location]["trip_time"].appendleft(
                    (dropoff_datetime - pickup_datetime).total_seconds()
                )
                location_windows[pickup_location]["trip_distance"].appendleft(
                    trip_distance
                )
                location_windows[pickup_location]["fare_amount"].appendleft(fare_amount)


if __name__ == "__main__":
    consumer = create_consumer()
    if consumer:
        consume_messages(consumer)
