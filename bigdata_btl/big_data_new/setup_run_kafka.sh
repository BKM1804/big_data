docker exec -it kafka bash
# Tạo topic có tên là 'web-crawl'
kafka-topics --create --topic web-crawl --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
# Kiểm tra các topic đã tạo
kafka-topics --list --bootstrap-server localhost:9092

#chạy ở ngoài
python3 crawl.py
spark-submit spark_consumer.py
