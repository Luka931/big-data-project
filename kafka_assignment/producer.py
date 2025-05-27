from confluent_kafka import Producer
import duckdb
import json

BOOTSTRAP_SERVERS = 'localhost:9092'
YELLOW_TAXI_TOPIC = 'yellow-taxi'

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' "
              f"[{msg.partition()}] @ offset {msg.offset()}")

def create_producer():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'python-producer'
    }

    producer = Producer(conf)
    print("Confluent Kafka Producer initialized successfully.")
    return producer


def send_message(producer, topic_name, message_value):
    producer.produce(topic_name,
                        value=json.dumps(message_value).encode('utf-8'),
                        callback=delivery_report)
    
    producer.poll(0)


def get_batch_yellow_taxi(batch_size = 1_000_000):
    yellow_taxi_path = 'data/trip_record_partitioned/yellow-taxi/year=2021/*.parquet'
    offset = 0
    
    while True:
        df = duckdb.sql(f'''
            select 
                tpep_pickup_datetime as pickup_datetime,
                tpep_dropoff_datetime as dropoff_datetime,
                PULocationID,
                DOLocationID,
                trip_distance as distance,
                fare_amount as base_fare,
                passenger_count,
                payment_type
            from '{yellow_taxi_path}'
            order by pickup_datetime
            limit {batch_size}
            offset {offset}
        ''').df()
        df['pickup_datetime'] = df['pickup_datetime'].astype('str')
        df['dropoff_datetime'] = df['dropoff_datetime'].astype('str')


        offset = offset + batch_size
        yield df.to_dict(orient='records')
        
def main():
    gen = get_batch_yellow_taxi()

    producer = create_producer()

    for records in gen:
        if len(records) == 0:
            break

        for i, rec in enumerate(records):
            send_message(producer, YELLOW_TAXI_TOPIC, rec)
            if i % 10_000 == 0:
                producer.flush()
            


if __name__ == "__main__":
    main()
