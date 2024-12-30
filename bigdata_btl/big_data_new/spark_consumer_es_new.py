from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, length, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from elasticsearch import Elasticsearch, helpers
import json
import logging
import signal
import sys
import hashlib
import requests
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaSparkElasticsearchIntegration")

# Define the Elasticsearch index
es_index = "web-crawl_1"

# Graceful shutdown handler
def signal_handler(sig, frame):
    logger.info("Shutting down gracefully...")
    # Stop all streaming queries
    es_query.stop()
    console_query.stop()
    # Stop Spark session
    spark.stop()
    sys.exit(0)
def get_embedding(text):
    url = "http://127.0.0.1:8000/get_docs_embedding"

    # Định nghĩa payload (dữ liệu gửi đi)
    payload = {
        "text": text
    }
    # Định nghĩa headers
    headers = {
        "Content-Type": "application/json"
    }
    try:
        # Gửi yêu cầu POST
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        # Kiểm tra mã trạng thái HTTP
        if response.status_code == 200:
            # Giả sử API trả về JSON chứa embedding
            embedding = response.json().get("embedding")
            return embedding
        else:
            print(f"Yêu cầu thất bại với mã trạng thái: {response.status_code}")
            print("Nội dung phản hồi:", response.text)
            return 0

    except requests.exceptions.RequestException as e:
        print("Đã xảy ra lỗi khi gửi yêu cầu:", e)


# Register the signal handlers for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Initialize Spark Session with necessary connectors
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.17.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Define schema for incoming JSON data
schema = StructType([
    StructField("url", StringType(), True),
    StructField("content", StringType(), True),
    StructField("timestamp", FloatType(), True)
    StructField("title", StringType(), True),
    StructField("keywords", StringType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-crawl") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Process the data (e.g., compute word count)
processed_df = json_df.withColumn("word_count", 
    (length(col("content")) - length(regexp_replace(col("content"), "\\w+", ""))) / 1
)



# Write processed data to console for debugging
console_query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Function to write each micro-batch to Elasticsearch and HDFS
def process_batch(batch_df, batch_id):
    try:
        logger.info(f"Processing batch {batch_id}...")

        # Tạo tên file HDFS dựa trên batch_id
        file_name = f"web_crawl_data_batch_{batch_id}.parquet"
        hdfs_path = f"hdfs://localhost:9000/user/spark/web_crawl_data/{file_name}"

        records = batch_df.toJSON().map(lambda j: json.loads(j)).collect()
        logger.info(f"Batch {batch_id} has {len(records)} records.")

        # Thêm trường 'embedding' vào mỗi bản ghi
        for record in records:
            content = record.get("content", "")
            embedding = get_embedding(content)
            record["embedding"] = embedding  # Thêm trường 'embedding'

        # Chuyển đổi danh sách các bản ghi trở lại thành DataFrame Spark
        # Sử dụng parallelize để tạo RDD từ danh sách các bản ghi
        new_batch_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
        new_batch_df = spark.read.json(new_batch_rdd)

        # Ghi DataFrame vào HDFS với tên file cụ thể
        # Sử dụng coalesce(1) để đảm bảo ghi thành một file duy nhất
        new_batch_df.coalesce(1).write.mode("append").parquet(hdfs_path)
        logger.info(f"Batch {batch_id} written to HDFS at {hdfs_path}.")

        # Thêm trường 'file_name' vào DataFrame
        batch_df_with_file = batch_df.withColumn("file_name", lit(file_name)).drop("content")

        # Khởi tạo client Elasticsearch với basic_auth
        es = Elasticsearch(
            hosts=["http://localhost:9200"],
        )

        # Kiểm tra kết nối đến Elasticsearch
        if not es.ping():
            logger.error("Elasticsearch cluster is down!")
            return

        # Thu thập các bản ghi từ DataFrame (cẩn thận với bộ nhớ nếu dữ liệu lớn)
        records = batch_df_with_file.toJSON().map(lambda j: json.loads(j)).collect()
        logger.info(f"Batch {batch_id} has {len(records)} records.")

        # Chuẩn bị các hành động bulk cho Elasticsearch
        actions = [
            {
                "_index": es_index,
                "_source": record,
                "_id": hashlib.sha256(record["url"].encode()).hexdigest()
            }
            for record in records
        ]
        # Ghi dữ liệu vào Elasticsearch
        if actions:
            helpers.bulk(es, actions)
            logger.info(f"Batch {batch_id} indexed successfully with file name {file_name}.")
        else:
            logger.info(f"Batch {batch_id} is empty.")
                
    except Exception as e:
        logger.error(f"Error in process_batch: {e}")

# Start the streaming query to write to Elasticsearch and HDFS
es_query = (
    processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/web-crawl-es-hdfs") \
        .start()
)

# Await termination once after starting all streaming queries
spark.streams.awaitAnyTermination()

logger.info("*************************************************")
