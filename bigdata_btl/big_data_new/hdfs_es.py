from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, length, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from elasticsearch import Elasticsearch, helpers

import json
import logging

# 1. Cấu Hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HDFStoElasticsearch")

# 2. Định Nghĩa Chỉ Mục Elasticsearch
es_index = "web-crawl"

# 3. Khởi Tạo SparkSession với Elasticsearch Connector
spark = SparkSession.builder \
    .appName("HDFStoElasticsearch") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.17.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# 4. Đọc Dữ Liệu Từ HDFS
df = spark.read.parquet("hdfs://localhost:9000/user/spark/web_crawl_data/")

# 5. Xử Lý Dữ Liệu (Tính toán word_count nếu chưa có)
# (Nếu đã có word_count trong dữ liệu, bạn có thể bỏ qua bước này)
# processed_df = df.withColumn("word_count", 
#     (length(col("content")) - length(regexp_replace(col("content"), "\\w+", ""))) / 1
# )

# 6. Ghi Dữ Liệu Lên Elasticsearch
def write_to_elasticsearch(partition):
    es = Elasticsearch(["http://localhost:9200"])
    actions = []
    for row in partition:
        doc = row.asDict()
        action = {
            "_index": es_index,
            "_id": doc.get("url"),  # Sử dụng 'url' làm ID duy nhất
            "_source": doc
        }
        actions.append(action)
    if actions:
        helpers.bulk(es, actions)
        logger.info(f"Indexed {len(actions)} records to Elasticsearch.")

# Sử dụng `foreachPartition` để ghi dữ liệu lên Elasticsearch
df.foreachPartition(write_to_elasticsearch)

logger.info("Đã ghi dữ liệu lên Elasticsearch thành công.")

# Dừng SparkSession
spark.stop()
