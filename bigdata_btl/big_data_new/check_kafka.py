from kafka import KafkaProducer
import json

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_message(producer, topic, message):
    future = producer.send(topic, message)
    producer.flush()
    return future

# Consumer
from kafka import KafkaConsumer

def create_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def consume_messages(consumer):
    for message in consumer:
        yield message.value

# Usage example
if __name__ == "__main__":
    TOPIC = "web-crawl"
    
    # Producer usage
    producer = create_producer()
    send_message(producer, TOPIC, {"key": "value"})
    print("completed send!")
    
    # Consumer usage
    consumer = create_consumer(TOPIC)
    for msg in consume_messages(consumer):
        print(msg)