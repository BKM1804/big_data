from elasticsearch import Elasticsearch, helpers
from datetime import datetime

# Khởi tạo kết nối đến Elasticsearch
es = Elasticsearch(
    hosts=["http://localhost:9200"],  # Địa chỉ của Elasticsearch
    # basic_auth=("username", "password"),  # Nếu có yêu cầu xác thực
    # scheme="http",
    # port=9200,
)

# Kiểm tra kết nối
if not es.ping():
    raise ValueError("Không thể kết nối đến Elasticsearch")

# Định nghĩa chỉ số (index) và loại tài liệu (document type) nếu cần
index_name = "web-crawl"

# Định nghĩa truy vấn
query = {
    "query": {
        "range": {
            "word_count": {
                "lt": 100000000000000000  # Less than 100,000
            }
        }
    },
    "sort": [
        {"word_count": {"order": "asc"}}  # Sắp xếp tăng dần theo word_count
    ],
    "size": 10  # Số lượng kết quả trả về (có thể điều chỉnh)
}

# Thực hiện truy vấn
response = es.search(index=index_name, body=query)

# Xử lý kết quả
for hit in response['hits']['hits']:
    print(f"ID: {hit['_id']}")
    print(f"Source: {hit['_source']['url']}")
    print("-" * 40)
