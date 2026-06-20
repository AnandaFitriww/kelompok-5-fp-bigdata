from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# 1. Inisialisasi Spark
spark = SparkSession.builder \
    .appName("ShelterEye-Silver") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_BRONZE = "hdfs://namenode:9000/lakehouse/bronze/data"
HDFS_SILVER = "hdfs://namenode:9000/lakehouse/silver/data"
CHECKPOINT_DIR = "hdfs://namenode:9000/lakehouse/silver/checkpoints"

print("🧹 Menjalankan Silver Layer: Membersihkan dan memecah (explode) data array...")

# 2. Definisikan Skema Array Kendaraan (Sesuai dengan temuan awal kita)
vehicle_schema = StructType([
    StructField("bus_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("current_stop", StringType())
])

batch_schema = StructType([
    StructField("type", StringType()),
    StructField("streaming_timestamp", StringType()),
    StructField("vehicles", ArrayType(vehicle_schema))
])

# 3. Baca stream dari tabel Delta Bronze (GPS)
df_bronze_gps = spark.readStream \
    .format("delta") \
    .load(f"{HDFS_BRONZE}/gps")

# 4. Transformasi Krusial: Parse JSON & Explode Array
df_silver_gps = df_bronze_gps \
    .select(from_json(col("raw_json"), batch_schema).alias("data"), col("_ingested_at")) \
    .select(
        col("data.streaming_timestamp").alias("event_time"),
        col("_ingested_at"),
        explode(col("data.vehicles")).alias("vehicle") # INI KUNCINYA! 1 Array dipecah jadi banyak baris
    ) \
    .select(
        col("event_time"),
        col("vehicle.bus_id").alias("bus_id"),
        col("vehicle.lat").alias("lat"),
        col("vehicle.lon").alias("lon"),
        col("vehicle.speed_kmh").alias("speed_kmh"),
        col("vehicle.current_stop").alias("current_stop"),
        col("_ingested_at")
    ) \
    .filter(col("lat").isNotNull() & col("lon").isNotNull()) # Buang data yang gak punya koordinat

# 5. Simpan (Dump) ke HDFS Silver Layer
query_gps = df_silver_gps.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/gps_bersih") \
    .start(f"{HDFS_SILVER}/gps_bersih")

query_gps.awaitTermination()