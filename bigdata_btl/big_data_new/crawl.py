import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
import requests
from kafka import KafkaProducer
import json
import time
import requests
from newspaper import Article
# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
server_url = "http://127.0.0.1:8000/crawl_data"

# Định nghĩa headers
headers = {
    "Content-Type": "application/json"
}
def crawl_url(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        data = article.text
        title = article.title
        keywords = article.meta_keywords
        if data = "":
            # Định nghĩa payload (dữ liệu gửi đi)
            payload = {
                "text": url
            }
            response = requests.post(server_url, headers=headers, data=json.dumps(payload))
            data = json.loads(response.text)['content']
        if data == "":
            print(f"Không thể crawl dữ liệu từ {url}")
            return
        # Gửi dữ liệu vào Kafka topic 'web-crawl'
        print("Da crawl du lieu! ")
        # print(data[:20])
        producer.send('web-crawl', {'url': url, 'content': data, 'timestamp': time.time(), 'title': title, 'keywords': keywords})
        print(f"Đã gửi dữ liệu từ {url} vào Kafka.")
    except Exception as e:
        print(f"Exception khi crawl {url}: {e}")

if __name__ == "__main__":
    target_url = "https://vnexpress.net/xuan-son-nhan-diem-cao-nhat-tran-thang-singapore-4832659.html"  # Thay thế bằng URL bạn muốn crawl
    while True:
        crawl_url(target_url)
        time.sleep(10)  # Crawl mỗi 10 giây