import json
import duckdb
from confluent_kafka import Producer

con = duckdb.connect("data/ducdb.db")

BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "combinded_data"

def get_data_in_batches(batch_size=100_000):
    offset = 0
    print(1)
    while True:
        df = con.sql(
            f"select * from combinded_data limit {batch_size} offset {offset}"
        ).df()
        df["pickup_datetime"] = df["pickup_datetime"].astype("str")
        df["dropoff_datetime"] = df["dropoff_datetime"].astype("str")

        offset = offset + batch_size

        yield df.to_dict(orient="records")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to topic '{msg.topic()}' "
            f"[{msg.partition()}] @ offset {msg.offset()}"
        )


def create_producer():
    conf = {"bootstrap.servers": BOOTSTRAP_SERVERS, "client.id": "python-producer"}
    try:
        producer = Producer(conf)
        print("Confluent Kafka Producer initialized successfully.")
        return producer
    except Exception as e:
        print(f"Error initializing Confluent Kafka Producer: {e}")
        return None


def send_message(producer, topic_name, message_value):
    if producer is None:
        print("Producer is not initialized. Cannot send message.")
        return
    try:
        producer.produce(
            topic_name,
            value=json.dumps(message_value).encode("utf-8"),
            callback=delivery_report,
        )

        producer.poll(0)
    except Exception as e:
        print(f"Error sending message: {e}")

def main():
    gen = get_data_in_batches()

    producer = create_producer()

    if producer:
        for records in gen:
            if len(records) == 0:
                break

            for rec in records:
                send_message(producer, KAFKA_TOPIC, rec)

            producer.flush()

        print("All messages sent and flushed.")


if __name__ == "__main__":
    main()