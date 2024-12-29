from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, length, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from elasticsearch import Elasticsearch, helpers
import json
from typing import Optional, List, Union
from fastapi import FastAPI, HTTPException
import uvicorn
from pydantic import BaseModel
import logging
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import hashlib
# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ElasticsearchAPI")

app = FastAPI()

# Khởi tạo client Elasticsearch
es = Elasticsearch(
    hosts=["http://localhost:9200"],
    # basic_auth=("username", "password"),  # Nếu Elasticsearch yêu cầu xác thực, bỏ comment và điền thông tin
    # scheme="http",
    # port=9200,
)

# Kiểm tra kết nối Elasticsearch khi khởi động ứng dụng
@app.on_event("startup")
def startup_event():
    if not es.ping():
        logger.error("Không thể kết nối đến Elasticsearch")
        raise RuntimeError("Không thể kết nối đến Elasticsearch")
    else:
        logger.info("Kết nối đến Elasticsearch thành công")
class UrlRequest(BaseModel):
    url: str

# Schema cho response
class ElasticsearchResponse(BaseModel):
    id: str
    url: str
    content: Optional[str] = None
    timestamp: Optional[float] = None
    word_count: Optional[int] = None
    file_name: Optional[str] = None  # Thêm các trường khác nếu cần

# Schema cho response API có thể là list hoặc int
class APIResponse(BaseModel):
    data: Union[List[ElasticsearchResponse], int]



class FileRequest(BaseModel):
    file_name: str

# 3. Định Nghĩa Schema Response
class ContentResponse(BaseModel):
    content: Optional[str]  # Trả về None nếu không tìm thấy
    embedding: Optional[list]

# 4. Khởi Tạo FastAPI
app = FastAPI()

# 5. Khởi Tạo SparkSession Khi Khởi Động Ứng Dụng
@app.on_event("startup")
def start_spark():
    global spark
    spark = SparkSession.builder \
        .appName("HDFStoElasticsearchAPI") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession đã được khởi tạo thành công.")

# 6. Đóng SparkSession Khi Dừng Ứng Dụng
@app.on_event("shutdown")
def stop_spark():
    spark.stop()
    logger.info("SparkSession đã được dừng.")

# 7. Định Nghĩa Endpoint để Lấy Content từ File HDFS
@app.post("/get_content_from_hdfs", response_model=ContentResponse)
def get_content_from_hdfs(request: FileRequest):
    file_name = request.file_name
    hdfs_path = f"hdfs://localhost:9000/user/spark/web_crawl_data/{file_name}"
    
    logger.info(f"Đang đọc file từ HDFS: {hdfs_path}")
    
    try:
        # Đọc file Parquet từ HDFS
        df = spark.read.parquet(hdfs_path)
        
        # Lấy giá trị của cột 'content' từ bản ghi đầu tiên
        content_row = df.select("content").first()
        embedding_row = df.select('embedding').first()
        
        if content_row and content_row["content"]:
            content = content_row["content"]
            embedding = embedding_row["embedding"]
            logger.info(f"Đã lấy nội dung từ file {file_name}")
            return ContentResponse(content=content , embedding = embedding)
        else:
            logger.warning(f"Không tìm thấy nội dung trong file {file_name}")
            return ContentResponse(content=None)
    
    except Exception as e:
        logger.error(f"Lỗi khi đọc file {file_name}: {e}")
        raise HTTPException(status_code=500, detail="Lỗi khi đọc file từ HDFS")
@app.post("/get_file_from_hdfs")
def get_file_from_hdfs(request: UrlRequest):
    url = request.url
    index_name = "web-crawl_1"  # Thay đổi nếu cần
    # Định nghĩa truy vấn Elasticsearch để tìm các tài liệu có 'url' trùng khớp
    query = {
        "query": {
            "term": {
                "_id": hashlib.sha256(url.encode()).hexdigest()  # Sử dụng 'url.keyword' nếu 'url' được định nghĩa là 'keyword' trong mapping
            }
        },
        "sort": [
            {"word_count": {"order": "asc"}}  # Sắp xếp tăng dần theo 'word_count'
        ],
        "size": 10  # Số lượng kết quả trả về (có thể điều chỉnh)
    }
    try:
        # Thực hiện truy vấn
        response = es.search(index=index_name, body=query)

        hits = response.get('hits', {}).get('hits', [])

        if not hits:
            return APIResponse(data=0)  # Trả về 0 nếu không tìm thấy tài liệu

        # Xử lý kết quả và tạo danh sách các tài liệu
        results = []
        for hit in hits:
            source = hit.get('_source', {})
            result = ElasticsearchResponse(
                id=hit.get('_id'),
                url=source.get('url', ''),
                content=source.get('content'),
                timestamp=source.get('timestamp'),
                word_count=source.get('word_count'),
                file_name=source.get('file_name')  # Nếu có trường 'file_name'
                # Thêm các trường khác nếu cần
            )
            results.append(result)

        return APIResponse(data=results)

    except exceptions.NotFoundError:
        logger.error(f"Chỉ số '{index_name}' không tồn tại trong Elasticsearch.")
        raise HTTPException(status_code=404, detail="Chỉ số không tồn tại")
    except exceptions.ElasticsearchException as e:
        logger.error(f"Lỗi Elasticsearch: {e}")
        raise HTTPException(status_code=500, detail="Lỗi Elasticsearch")
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")
        raise HTTPException(status_code=500, detail="Lỗi không mong muốn")
if __name__ == "__main__":
    # Khởi chạy server FastAPI sử dụng Uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7728)