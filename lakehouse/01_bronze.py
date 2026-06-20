from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType
)
from pyspark.sql.functions import (
    from_json, col, current_timestamp, to_date, hour
)

# ============================================================
# 1. Inisialisasi Spark + Delta Lake + Kafka
# ============================================================
spark = SparkSession.builder \
    .appName("ShelterEye-Bronze") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 2. Konfigurasi
# ============================================================
KAFKA_BROKER = "kafka:29092"
HDFS_BASE = "hdfs://namenode:9000/lakehouse/bronze"

# ============================================================
# 3. Schema eksplisit untuk kedua topic
# ============================================================

# Schema untuk topic: bus-telemetry-raw
telemetry_schema = StructType([
    StructField("type", StringType()),
    StructField("bus_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("route_name", StringType()),
    StructField("direction", IntegerType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("is_moving", BooleanType()),
    StructField("current_stop", StringType()),
    StructField("next_stop", StringType()),
    StructField("progress", DoubleType()),
    StructField("distance_km", DoubleType()),
    StructField("sim_time", StringType()),
    StructField("streaming_timestamp", StringType()),
])

# Schema untuk topic: transaction-raw
transaction_schema = StructType([
    StructField("transID", StringType()),
    StructField("payCardID", StringType()),
    StructField("payCardBank", StringType()),
    StructField("payCardName", StringType()),
    StructField("payCardSex", StringType()),
    StructField("payCardBirthDate", StringType()),
    StructField("corridorID", StringType()),
    StructField("corridorName", StringType()),
    StructField("direction", StringType()),
    StructField("tapInStops", StringType()),
    StructField("tapInStopsName", StringType()),
    StructField("tapInStopsLat", StringType()),
    StructField("tapInStopsLon", StringType()),
    StructField("stopStartSeq", StringType()),
    StructField("tapInTime", StringType()),
    StructField("tapOutStops", StringType()),
    StructField("tapOutStopsName", StringType()),
    StructField("tapOutStopsLat", StringType()),
    StructField("tapOutStopsLon", StringType()),
    StructField("stopEndSeq", StringType()),
    StructField("tapOutTime", StringType()),
    StructField("payAmount", StringType()),
    StructField("streaming_timestamp", StringType()),
])

# ============================================================
# 4. Fungsi generik: Kafka → Delta Bronze
# ============================================================
def create_bronze_stream(topic_name, schema, output_path):
    """
    Baca dari Kafka, parse JSON pakai schema eksplisit,
    tambah metadata kolom, tulis ke Delta table di HDFS.
    
    Prinsip Bronze: landing apa adanya, TANPA filtering/dedup/business logic.
    """

    # --- READ: Kafka stream ---
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # --- PARSE: JSON + metadata Kafka ---
    df_parsed = df_raw.select(
        # Parse JSON value pakai schema eksplisit
        from_json(
            col("value").cast("string"), schema
        ).alias("data"),
        # Metadata dari Kafka
        col("timestamp").alias("kafka_ingest_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )

    # Flatten: expand struct "data.*" ke kolom-kolom individual
    df_flat = df_parsed.select(
        "data.*",
        "kafka_ingest_timestamp",
        "kafka_partition",
        "kafka_offset",
    )

    # Tambah kolom partisi turunan
    df_bronze = df_flat \
        .withColumn("ingest_date", to_date("kafka_ingest_timestamp")) \
        .withColumn("ingest_hour", hour("kafka_ingest_timestamp"))

    # --- WRITE: Delta table ke HDFS ---
    table_path = f"{HDFS_BASE}/{output_path}"
    checkpoint_path = f"{HDFS_BASE}/checkpoints/{output_path}"

    query = df_bronze.writeStream \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("ingest_date", "ingest_hour") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", checkpoint_path) \
        .start(table_path)

    return query


# ============================================================
# 5. Jalankan 2 stream Bronze secara paralel
# ============================================================
print("🔥 Bronze Layer: Kafka → Delta (HDFS)")
print(f"   Output: {HDFS_BASE}/bus_telemetry/")
print(f"   Output: {HDFS_BASE}/transactions/")

query_telemetry = create_bronze_stream(
    topic_name="bus-telemetry-raw",
    schema=telemetry_schema,
    output_path="bus_telemetry",
)

query_transaction = create_bronze_stream(
    topic_name="transaction-raw",
    schema=transaction_schema,
    output_path="transactions",
)

# Tahan proses agar streaming terus jalan
spark.streams.awaitAnyTermination()