from pyspark.sql import SparkSession

# 1. Inisialisasi Spark
spark = SparkSession.builder \
    .appName("ShelterEye-IntipData") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # Set ke ERROR biar log warning gak nutupin tabel

print("\n" + "="*70)
print("🚌 👀 MENGINTIP DATA SILVER LAYER (GPS BUS BERSIH) 👀 🚌")
print("="*70)

HDFS_SILVER_GPS = "hdfs://namenode:9000/lakehouse/silver/data/gps_bersih"

try:
    # Karena kita cuma mau ngintip, kita pakai spark.read (Batch), BUKAN readStream
    df_silver_gps = spark.read.format("delta").load(HDFS_SILVER_GPS)
    
    # Hitung total data yang sudah masuk
    total_data = df_silver_gps.count()
    print(f"Total baris data GPS di Silver Layer saat ini: {total_data} baris\n")
    
    # Tampilkan 10 data teratas secara rapi
    df_silver_gps.show(10, truncate=False)

except Exception as e:
    print(f"Ups! Tabel belum siap atau ada error: {e}")

print("="*70 + "\n")