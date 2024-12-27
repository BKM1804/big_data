from kafka import KafkaProducer
import json

def send_message(topic, message):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(topic, message)
    producer.flush()
    print(f"Đã gửi tin nhắn: {message}")

if __name__ == "__main__":
    topic = 'test-topic'
    message = {'message': 'Hello Kafka!'}
    send_message(topic, message)
