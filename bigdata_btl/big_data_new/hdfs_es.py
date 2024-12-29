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
def read_data_from_hdfs():
    df = spark.read.parquet("hdfs://localhost:9000/user/spark/web_crawl_data/")
    file_path = "hdfs://localhost:9000/user/spark/web_crawl_data/web_crawl_data_batch_74.parquet"
    df = spark.read.parquet(file_path)
    import pdb
    pdb.set_trace()
    return df.take(1)[0]['content']
if __name__ == "__main__":
    x = read_data_from_hdfs()
    print(x)
