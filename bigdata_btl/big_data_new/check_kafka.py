from kafka import KafkaConsumer

consumer = KafkaConsumer('web-crawl', bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='latest')
import pdb
pdb.set_trace()
for message in consumer:
    print(1)
    print(message.value)