from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, udf, split, size , length
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, DoubleType
# import tensorflow as tf
# import numpy as np

# Khởi tạo Spark Session với các package cần thiết
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu JSON
schema = StructType([
    StructField("url", StringType(), True),
    StructField("content", StringType(), True),
    StructField("timestamp", FloatType(), True)
])

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-crawl") \
    .option("startingOffsets", "latest") \
    .load()

# Chuyển đổi dữ liệu từ Kafka (giữ dưới dạng JSON)
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

processed_df = json_df.withColumn("word_count", 
    (length(col("content")) - length(regexp_replace(col("content"), "\\w+", ""))) / 1
)

# Ghi dữ liệu đã xử lý lên HDFS theo batch
hdfs_query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/spark/web_crawl_data") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_hdfs") \
    .start()

# Ghi dữ liệu đã xử lý lên Elasticsearch
es_query = processed_df.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "hehe") \
    .option("es.port", "9200") \
    .option("es.resource", "web_crawl_data/doc") \
    .option("es.input.json", "false") \
    .option("es.nodes.wan.only", "false") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_es") \
    .start()
print("*************************************************")
spark.streams.awaitTermination()
# es_query.a
