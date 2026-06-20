from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable
import time

# ============================================================
# 1. SPARK SESSION + DELTA LAKE
# ============================================================
spark = SparkSession.builder \
    .appName("ShelterEye-Silver") \
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
# 2. PATH CONFIGURATION
# ============================================================
HDFS = "hdfs://namenode:9000"

# Bronze (input)
BRONZE_TELEMETRY   = f"{HDFS}/lakehouse/bronze/bus_telemetry"
BRONZE_TRANSACTION = f"{HDFS}/lakehouse/bronze/transactions"

# Silver (output)
SILVER_TXN_ENRICHED  = f"{HDFS}/lakehouse/silver/transactions_enriched"
SILVER_TXN_QUARANTINE = f"{HDFS}/lakehouse/silver/transactions_quarantine"
SILVER_BUS_OCCUPANCY = f"{HDFS}/lakehouse/silver/bus_occupancy"
SILVER_STOP_DEMAND   = f"{HDFS}/lakehouse/silver/stop_demand"
SILVER_CORRIDOR_MEDIAN = f"{HDFS}/lakehouse/silver/corridor_median_duration"

# Jakarta bounding box (untuk validasi koordinat)
JAK_LAT_MIN, JAK_LAT_MAX = -6.45, -6.05
JAK_LON_MIN, JAK_LON_MAX = 106.65, 107.05


# ============================================================
# 3. HELPER: MERGE atau CREATE Delta table
# ============================================================
def merge_or_create(df, path, merge_condition, partition_cols=None):
    """
    Jika Delta table sudah ada di path → MERGE (upsert).
    Jika belum ada → CREATE (initial write).
    """
    try:
        is_delta = DeltaTable.isDeltaTable(spark, path)
    except Exception:
        is_delta = False

    if is_delta:
        delta_table = DeltaTable.forPath(spark, path)
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        print(f"    ✅ MERGE → {path}")
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        print(f"    ✅ CREATE → {path}")


# ============================================================
# 4. MAIN SILVER BATCH JOB
# ============================================================
def run_silver_batch():
    print("=" * 65)
    print("🧹 SILVER LAYER — Batch Processing Start")
    print("=" * 65)

    # ----------------------------------------------------------
    # STEP 1: Baca & Bersihkan Bus Telemetry
    # ----------------------------------------------------------
    print("\n📡 Step 1/7: Cleaning Bus Telemetry...")

    try:
        df_tel_raw = spark.read.format("delta").load(BRONZE_TELEMETRY)
    except Exception as e:
        print(f"  ⚠️ Bronze telemetry belum tersedia, skip batch: {e}")
        return

    df_telemetry_clean = df_tel_raw \
        .dropDuplicates(["bus_id", "streaming_timestamp"]) \
        .withColumn("sim_time_ts",
                    F.to_timestamp("sim_time")) \
        .withColumn("streaming_ts",
                    F.to_timestamp("streaming_timestamp")) \
        .filter(
            (F.col("lat").between(JAK_LAT_MIN, JAK_LAT_MAX)) &
            (F.col("lon").between(JAK_LON_MIN, JAK_LON_MAX))
        )

    # Cache karena dipakai di banyak step (trip matching, synthetic tap-out, occupancy)
    df_telemetry_clean.cache()
    tel_count = df_telemetry_clean.count()
    print(f"    Telemetry bersih: {tel_count:,} records")

    # ----------------------------------------------------------
    # STEP 2: Baca & Bersihkan Transaksi
    # ----------------------------------------------------------
    print("\n🎫 Step 2/7: Cleaning Transactions...")

    try:
        df_txn_raw = spark.read.format("delta").load(BRONZE_TRANSACTION)
    except Exception as e:
        print(f"  ⚠️ Bronze transactions belum tersedia, skip batch: {e}")
        df_telemetry_clean.unpersist()
        return

    df_txn_dedup = df_txn_raw.dropDuplicates(["transID"])

    # --- Karantina: transaksi tanpa corridorID ---
    df_quarantine = df_txn_dedup.filter(F.col("corridorID").isNull())
    quarantine_count = df_quarantine.count()
    if quarantine_count > 0:
        merge_or_create(df_quarantine, SILVER_TXN_QUARANTINE,
                        "target.transID = source.transID")
        print(f"    ⚠️ Karantina: {quarantine_count:,} transaksi tanpa corridorID")

    # --- Transaksi valid: parse & cast ---
    df_txn_clean = df_txn_dedup \
        .filter(F.col("corridorID").isNotNull()) \
        .withColumn("tapInTime_ts",  F.to_timestamp("tapInTime")) \
        .withColumn("tapOutTime_ts", F.to_timestamp("tapOutTime")) \
        .withColumn("direction_int",
                    F.col("direction").cast("double").cast("int")) \
        .withColumn("tapInLat",
                    F.col("tapInStopsLat").cast("double")) \
        .withColumn("tapInLon",
                    F.col("tapInStopsLon").cast("double")) \
        .withColumn("tapOutLat",
                    F.col("tapOutStopsLat").cast("double")) \
        .withColumn("tapOutLon",
                    F.col("tapOutStopsLon").cast("double")) \
        .withColumn("stopStartSeq_int",
                    F.col("stopStartSeq").cast("int")) \
        .withColumn("stopEndSeq_int",
                    F.col("stopEndSeq").cast("double").cast("int")) \
        .withColumn("payAmount_dbl",
                    F.col("payAmount").cast("double"))

    df_txn_clean.cache()
    txn_count = df_txn_clean.count()
    print(f"    Transaksi bersih: {txn_count:,} records")

    # ----------------------------------------------------------
    # STEP 3: Hitung Median Durasi per Corridor (tabel fallback)
    # ----------------------------------------------------------
    print("\n⏱️  Step 3/7: Computing Corridor Median Durations...")

    df_median = df_txn_clean \
        .filter(
            F.col("tapOutTime_ts").isNotNull() &
            F.col("tapInTime_ts").isNotNull()
        ) \
        .withColumn("trip_dur_sec",
                    F.col("tapOutTime_ts").cast("long")
                    - F.col("tapInTime_ts").cast("long")) \
        .filter(F.col("trip_dur_sec") > 0) \
        .groupBy("corridorID") \
        .agg(F.percentile_approx("trip_dur_sec", 0.5)
              .alias("median_duration_sec"))

    merge_or_create(df_median, SILVER_CORRIDOR_MEDIAN,
                    "target.corridorID = source.corridorID")

    # Baca ulang dari Delta (supaya konsisten dengan data yang sudah di-merge)
    df_median_ref = spark.read.format("delta").load(SILVER_CORRIDOR_MEDIAN)
    print(f"    Median durations: {df_median_ref.count()} corridors")

    # ----------------------------------------------------------
    # STEP 4: Trip Matching (Transaksi ↔ Bus)
    # ----------------------------------------------------------
    print("\n🔗 Step 4/7: Trip Matching...")

    # Join conditions:
    #   corridorID == route_id
    #   direction_int == direction (telemetry INT)
    #   (current_stop == tapInStopsName OR next_stop == tapInStopsName)
    #   sim_time_ts antara tapInTime_ts dan tapInTime_ts + 10 menit
    df_candidates = df_txn_clean.alias("txn").join(
        df_telemetry_clean.alias("tel"),
        (F.col("txn.corridorID") == F.col("tel.route_id")) &
        (F.col("txn.direction_int") == F.col("tel.direction")) &
        (
            (F.col("tel.current_stop") == F.col("txn.tapInStopsName")) |
            (F.col("tel.next_stop")    == F.col("txn.tapInStopsName"))
        ) &
        (F.col("tel.sim_time_ts") >= F.col("txn.tapInTime_ts")) &
        (F.col("tel.sim_time_ts") <=
         F.col("txn.tapInTime_ts") + F.expr("INTERVAL 10 MINUTES")),
        "inner"
    ).select(
        # Kolom dari transaksi
        F.col("txn.transID"),
        F.col("txn.payCardID"),
        F.col("txn.payCardBank"),
        F.col("txn.payCardName"),
        F.col("txn.payCardSex"),
        F.col("txn.payCardBirthDate"),
        F.col("txn.corridorID"),
        F.col("txn.corridorName"),
        F.col("txn.direction_int").alias("direction"),
        F.col("txn.tapInStopsName"),
        F.col("txn.tapInLat"),
        F.col("txn.tapInLon"),
        F.col("txn.stopStartSeq_int").alias("stopStartSeq"),
        F.col("txn.tapInTime_ts"),
        F.col("txn.tapOutStopsName"),
        F.col("txn.tapOutLat"),
        F.col("txn.tapOutLon"),
        F.col("txn.stopEndSeq_int").alias("stopEndSeq"),
        F.col("txn.payAmount_dbl").alias("payAmount"),
        # Kolom matching dari telemetry
        F.col("tel.bus_id").alias("matched_bus_id"),
        F.col("tel.sim_time_ts").alias("matched_sim_time"),
    )

    # Pilih kandidat terdekat per transaksi (ROW_NUMBER, ORDER BY sim_time ASC)
    w_match = Window.partitionBy("transID") \
                    .orderBy(F.col("matched_sim_time").asc())

    df_matched = df_candidates \
        .withColumn("_rn", F.row_number().over(w_match)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    matched_count = df_matched.count()
    print(f"    Matched: {matched_count:,} / {txn_count:,} transaksi")

    # ----------------------------------------------------------
    # STEP 5: Synthetic Tap-Out
    # ----------------------------------------------------------
    print("\n🚏 Step 5/7: Computing Synthetic Tap-Out...")

    # Untuk tiap matched transaction, cari record telemetry PERTAMA dimana
    # bus tersebut tiba di halte tujuan (current_stop == tapOutStopsName)
    # setelah waktu matching, dalam batas 90 menit.
    df_with_arrival = df_matched.alias("m").join(
        df_telemetry_clean.alias("arr"),
        (F.col("m.matched_bus_id") == F.col("arr.bus_id")) &
        (F.col("arr.current_stop") == F.col("m.tapOutStopsName")) &
        (F.col("arr.sim_time_ts") > F.col("m.matched_sim_time")) &
        (F.col("arr.sim_time_ts") <=
         F.col("m.matched_sim_time") + F.expr("INTERVAL 90 MINUTES")),
        "left"
    ).select(
        F.col("m.*"),
        F.col("arr.sim_time_ts").alias("arrival_at_dest"),
    )

    # Pilih kedatangan paling awal per transaksi
    w_arrival = Window.partitionBy("transID") \
                      .orderBy(F.col("arrival_at_dest").asc_nulls_last())

    df_first_arrival = df_with_arrival \
        .withColumn("_rn", F.row_number().over(w_arrival)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    # Join dengan median corridor duration (fallback)
    df_with_fallback = df_first_arrival.alias("fa").join(
        df_median_ref.alias("med"),
        F.col("fa.corridorID") == F.col("med.corridorID"),
        "left"
    ).select(
        F.col("fa.*"),
        F.col("med.median_duration_sec"),
    )

    # Hitung synthetic_tapOutTime:
    #   1. Bus terlihat di halte tujuan → arrival_at_dest + 2 menit
    #   2. Fallback median → tapInTime_ts + median_duration_sec
    #   3. Ultimate fallback → tapInTime_ts + 30 menit
    df_enriched = df_with_fallback.withColumn(
        "synthetic_tapOutTime",
        F.when(
            F.col("arrival_at_dest").isNotNull(),
            F.col("arrival_at_dest") + F.expr("INTERVAL 2 MINUTES")
        ).otherwise(
            F.when(
                F.col("median_duration_sec").isNotNull(),
                # tapInTime (epoch seconds) + median_duration → cast back to timestamp
                (F.col("tapInTime_ts").cast("long")
                 + F.col("median_duration_sec")).cast("timestamp")
            ).otherwise(
                F.col("tapInTime_ts") + F.expr("INTERVAL 30 MINUTES")
            )
        )
    ).withColumn(
        "trip_duration_minutes",
        (F.col("synthetic_tapOutTime").cast("long")
         - F.col("tapInTime_ts").cast("long")) / 60.0
    ).drop("arrival_at_dest", "median_duration_sec")

    # --- MERGE → transactions_enriched ---
    merge_or_create(df_enriched, SILVER_TXN_ENRICHED,
                    "target.transID = source.transID")
    enriched_count = df_enriched.count()
    print(f"    Enriched transactions: {enriched_count:,} records")

    # ----------------------------------------------------------
    # STEP 6: Occupancy Timeline
    # ----------------------------------------------------------
    print("\n📊 Step 6/7: Computing Occupancy Timeline...")

    # Buat event +1 (tap-in) dan -1 (tap-out) untuk setiap transaksi yang matched
    events_in = df_enriched.select(
        F.col("matched_bus_id").alias("bus_id"),
        F.col("tapInTime_ts").alias("event_time"),
        F.lit(1).alias("pax_change"),
    )
    events_out = df_enriched.select(
        F.col("matched_bus_id").alias("bus_id"),
        F.col("synthetic_tapOutTime").alias("event_time"),
        F.lit(-1).alias("pax_change"),
    )

    # Tambahkan null columns agar schema cocok untuk union dengan telemetry
    _null_loc = [
        F.lit(None).cast("double").alias("lat"),
        F.lit(None).cast("double").alias("lon"),
        F.lit(None).cast("string").alias("route_id"),
        F.lit(None).cast("string").alias("route_name"),
        F.lit(None).cast("string").alias("current_stop"),
        F.lit(None).cast("string").alias("next_stop"),
        F.lit(None).cast("double").alias("speed_kmh"),
    ]

    pax_events = events_in.select("bus_id", "event_time", "pax_change", *_null_loc) \
        .union(events_out.select("bus_id", "event_time", "pax_change", *_null_loc))

    # Telemetry sebagai 0-change anchor (membawa info lokasi)
    tel_anchors = df_telemetry_clean.select(
        F.col("bus_id"),
        F.col("sim_time_ts").alias("event_time"),
        F.lit(0).alias("pax_change"),
        F.col("lat"),
        F.col("lon"),
        F.col("route_id"),
        F.col("route_name"),
        F.col("current_stop"),
        F.col("next_stop"),
        F.col("speed_kmh"),
    )

    # Union semua event, hitung running sum per bus
    all_events = pax_events.union(tel_anchors)

    # Window: partisi per bus, urut waktu, tap-in (+1) didahulukan atas anchor (0) atas tap-out (-1)
    w_occ = Window.partitionBy("bus_id") \
        .orderBy(F.col("event_time"), F.col("pax_change").desc()) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_occ_all = all_events.withColumn(
        "occupancy", F.sum("pax_change").over(w_occ)
    )

    # Ambil hanya baris telemetry (punya lat/lon) sebagai output
    df_bus_occupancy = df_occ_all \
        .filter(F.col("lat").isNotNull()) \
        .select(
            "bus_id",
            F.col("event_time").alias("sim_time"),
            "lat", "lon",
            "route_id", "route_name",
            "current_stop", "next_stop",
            "speed_kmh", "occupancy",
        )

    # --- MERGE → bus_occupancy ---
    merge_or_create(
        df_bus_occupancy, SILVER_BUS_OCCUPANCY,
        "target.bus_id = source.bus_id AND target.sim_time = source.sim_time"
    )
    occ_count = df_bus_occupancy.count()
    print(f"    Bus occupancy: {occ_count:,} records")

    # ----------------------------------------------------------
    # STEP 7: Demand per Halte (5-minute windows)
    # ----------------------------------------------------------
    print("\n🏗️  Step 7/7: Computing Stop Demand...")

    # Hitung jumlah tap-in per halte per 5 menit
    # Memakai SEMUA transaksi bersih (matched maupun tidak)
    df_stop_demand = df_txn_clean \
        .filter(F.col("tapInTime_ts").isNotNull()) \
        .withColumn("time_window",
                    F.window(F.col("tapInTime_ts"), "5 minutes")) \
        .groupBy(
            F.col("tapInStopsName").alias("stop_name"),
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
        ).agg(
            F.count("*").alias("demand_count"),
            F.first("tapInLat").alias("stop_lat"),
            F.first("tapInLon").alias("stop_lon"),
            F.first("corridorID").alias("corridor_id"),
            F.first("corridorName").alias("corridor_name"),
        )

    # --- MERGE → stop_demand ---
    merge_or_create(
        df_stop_demand, SILVER_STOP_DEMAND,
        "target.stop_name = source.stop_name AND target.window_start = source.window_start"
    )
    demand_count = df_stop_demand.count()
    print(f"    Stop demand: {demand_count:,} records")

    # ----------------------------------------------------------
    # Cleanup cache
    # ----------------------------------------------------------
    df_telemetry_clean.unpersist()
    df_txn_clean.unpersist()

    print("\n" + "=" * 65)
    print("✅ SILVER LAYER — Batch selesai!")
    print("=" * 65)


# ============================================================
# 5. ENTRY POINT: Loop batch setiap 5 menit
# ============================================================
if __name__ == "__main__":
    BATCH_INTERVAL = 300  # 5 menit

    print("🧹 Silver Layer — Periodic Batch Job")
    print(f"   Interval   : {BATCH_INTERVAL}s")
    print(f"   Input (tel): {BRONZE_TELEMETRY}")
    print(f"   Input (txn): {BRONZE_TRANSACTION}")
    print(f"   Output     : {SILVER_TXN_ENRICHED}")
    print(f"                {SILVER_BUS_OCCUPANCY}")
    print(f"                {SILVER_STOP_DEMAND}")

    while True:
        try:
            run_silver_batch()
        except Exception as e:
            print(f"\n❌ Error di Silver batch: {e}")
            import traceback
            traceback.print_exc()

        print(f"\n⏳ Menunggu {BATCH_INTERVAL}s sebelum batch berikutnya...\n")
        time.sleep(BATCH_INTERVAL)