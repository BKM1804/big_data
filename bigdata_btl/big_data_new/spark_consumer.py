# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col , lenght , udf
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, length, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, DoubleType
# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "elasticsearch-hadoop-7.17.5.jar") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-crawl") \
    .option("startingOffsets", "latest") \
    .load()

# Định nghĩa schema cho dữ liệu JSON
schema = StructType([
    StructField("url", StringType(), True),
    StructField("content", StringType(), True),
    StructField("timestamp", FloatType(), True)
])

# Chuyển đổi dữ liệu từ Kafka (giữ dưới dạng JSON)
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Xử lý dữ liệu (ví dụ: đếm số từ trong nội dung)
processed_df = json_df.withColumn("word_count", 
    (length(col("content")) - length(regexp_replace(col("content"), "\\w+", ""))) / 1
)

# Ghi dữ liệu đã xử lý lên HDFS theo batch
query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/spark/web_crawl_data") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()
es_query = processed_df.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", "web_crawl_data/doc") \
    .option("es.input.json", "false") \
    .option("es.nodes.wan.only", "false") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_es") \
    .start()
# query.awaitTermination()
print("*************************************************")
spark.streams.awaitAnyTermination()
