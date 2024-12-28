import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
import requests
from kafka import KafkaProducer
import json
import time

# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def crawl_url(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.text
            # Gửi dữ liệu vào Kafka topic 'web-crawl'
            print("Da crawl du lieu! ")
            # print(data[:20])
            producer.send('web-crawl', {'url': url, 'content': data, 'timestamp': time.time()})
            print(f"Đã gửi dữ liệu từ {url} vào Kafka.")
        else:
            print(f"Lỗi khi truy cập {url}: {response.status_code}")
    except Exception as e:
        print(f"Exception khi crawl {url}: {e}")

if __name__ == "__main__":
    target_url = "https://vnexpress.net/xuan-son-nhan-diem-cao-nhat-tran-thang-singapore-4832659.html"  # Thay thế bằng URL bạn muốn crawl
    while True:
        crawl_url(target_url)
        time.sleep(10)  # Crawl mỗi 10 giây