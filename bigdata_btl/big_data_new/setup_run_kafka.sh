docker exec -it kafka bash
# Tạo topic có tên là 'web-crawl'
kafka-topics --create --topic web-crawl --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
# Kiểm tra các topic đã tạo
kafka-topics --list --bootstrap-server localhost:9092

#chạy ở ngoài
python3 crawl.py
# spark-submit spark_consumer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 spark_consumer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.0 spark_consumer_es.py
#check data đã lên hadoop chưa
docker exec -it namenode bash
hdfs dfs -ls /user/spark/web_crawl_data