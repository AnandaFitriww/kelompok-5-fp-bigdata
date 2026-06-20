from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# 1. Inisialisasi Spark dengan ekstensi Kafka & Delta Lake
spark = SparkSession.builder \
    .appName("ShelterEye-Bronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Karena Spark jalan di dalam Docker, kita pakai nama service dari docker-compose
KAFKA_BROKER = "kafka:29092" 
HDFS_PATH = "hdfs://namenode:9000/lakehouse/bronze"

print("🔥 Menjalankan Spark Streaming: Menyedot Kafka ke HDFS (Bronze Layer)...")

def create_bronze_stream(topic_name, folder_name):
    # Baca real-time dari Kafka
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()
        
    # Ubah format byte Kafka jadi String (JSON Mentah) & tambahkan waktu masuk (ingested_at)
    df_bronze = df_stream.selectExpr("CAST(value AS STRING) as raw_json") \
        .withColumn("_ingested_at", current_timestamp())
        
    # Tulis (Dump) ke HDFS pakai format Delta
    query = df_bronze.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{HDFS_PATH}/checkpoints/{folder_name}") \
        .start(f"{HDFS_PATH}/data/{folder_name}")
        
    return query

# 2. Jalankan dua penyedot secara paralel!
query_gps = create_bronze_stream("topic-gps", "gps")
query_penumpang = create_bronze_stream("topic-penumpang", "penumpang")

# 3. Tahan proses agar jalan terus (listening)
spark.streams.awaitAnyTermination()