from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ShelterEye-Kafka-To-Bronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Baca data real-time streaming dari Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic-transjakarta-ws") \
    .load()

# Ambil value data biner Kafka dan convert ke String
df_bronze = df_kafka.selectExpr("CAST(value AS STRING) as json_data")

# Sink langsung ke Hadoop HDFS Bronze Layer
query = df_bronze.writeStream \
    .format("text") \
    .option("path", "hdfs://namenode:9000/lakehouse_data/bronze/transjakarta_raw") \
    .option("checkpointLocation", "hdfs://namenode:9000/lakehouse_data/checkpoint_bronze") \
    .start()

query.awaitTermination()