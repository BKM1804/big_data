from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, length
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from elasticsearch import Elasticsearch, helpers
import json
import logging
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaSparkElasticsearchIntegration")

# Define the Elasticsearch index
es_index = "web-crawl"

# Graceful shutdown handler
def signal_handler(sig, frame):
    logger.info("Shutting down gracefully...")
    # Stop all streaming queries
    es_query.stop()
    hdfs_query.stop()
    console_query.stop()
    # Stop Spark session
    spark.stop()
    sys.exit(0)

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

# Function to write each micro-batch to Elasticsearch
def write_to_elasticsearch(batch_df, batch_id):
    try:
        # Initialize Elasticsearch client
        es = Elasticsearch(["http://localhost:9200"])

        # Check if Elasticsearch is up
        if not es.ping():
            logger.error("Elasticsearch cluster is down!")
            return

        # Collect records (ensure data is small to avoid driver memory issues)
        records = batch_df.toJSON().map(lambda j: json.loads(j)).collect()
        logger.info(f"Batch {batch_id} has {len(records)} records.")

        # Prepare bulk actions
        actions = [
            {
                "_index": es_index,
                "_source": record
            }
            for record in records
        ]

        # Bulk index into Elasticsearch
        if actions:
            helpers.bulk(es, actions)
            logger.info(f"Batch {batch_id} indexed successfully.")
        else:
            logger.info(f"Batch {batch_id} is empty.")
                
    except Exception as e:
        logger.error(f"Error in write_to_elasticsearch: {e}")

# Start the streaming query to write to Elasticsearch
es_query = (
    processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_elasticsearch) \
        .option("checkpointLocation", "/tmp/checkpoints/web-crawl-es") \
        .start()
)

# Ghi dữ liệu đã xử lý lên HDFS theo batch
hdfs_query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/spark/web_crawl_data") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_hdfs") \
    .start()

# Await termination once after starting all streaming queries
spark.streams.awaitAnyTermination()

logger.info("*************************************************")
