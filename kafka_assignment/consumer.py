from confluent_kafka import Consumer, KafkaException, KafkaError
import json

GROUP_ID = 'my_confluent_consumer_group'
BOOTSTRAP_SERVERS = 'localhost:9092'
YELLOW_TAXI_TOPIC = 'yellow-taxi'

def create_consumer():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,     
        'auto.commit.interval.ms': 1000 
    }
    try:
        consumer = Consumer(conf)
        print("Confluent Kafka Consumer initialized successfully.")
        return consumer
    except Exception as e:
        print(f"Error initializing Confluent Kafka Consumer: {e}")
        return None

def consume_messages(consumer):
    consumer.subscribe([YELLOW_TAXI_TOPIC])
    print(f"Topic: {YELLOW_TAXI_TOPIC}, Group: {GROUP_ID}")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            decoded_value = json.loads(msg.value().decode('utf-8'))
            print(decoded_value)



def main():
    consumer = create_consumer()
    if consumer:
        consume_messages(consumer)


if __name__ == "__main__":
    main()
