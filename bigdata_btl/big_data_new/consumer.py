from kafka import KafkaConsumer
import json

def consume_messages(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print(f"Đang lắng nghe trên topic: {topic}")
    for message in consumer:
        print(f"Đã nhận tin nhắn: {message.value}")
        # Dừng consumer sau khi nhận được một tin nhắn
        break

if __name__ == "__main__":
    topic = 'test-topic'
    consume_messages(topic)
