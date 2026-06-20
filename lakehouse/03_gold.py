"""
Gold Layer: 3 ML Models + Metrics untuk Transjakarta Prediction
1. Stop Overcapacity Risk Prediction  (GBTRegressor — 15 min forecast)
2. Shelter Congestion Index            (metric calculation)
3. Bus Arrival Occupancy Prediction    (GBTRegressor)

Output: Gold Delta tables + JSON exports untuk dashboard API.
"""

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from delta.tables import DeltaTable
import json
import os
import time
from datetime import datetime, date

# ============================================================
# 1. SPARK SESSION + DELTA LAKE
# ============================================================
spark = SparkSession.builder \
    .appName("ShelterEye-Gold") \
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
# 2. KONFIGURASI
# ============================================================
BUS_CAPACITY = 60  # Semua bus dianggap kapasitas sama

HDFS = "hdfs://namenode:9000"

# Silver (input)
SILVER_BUS_OCCUPANCY  = f"{HDFS}/lakehouse/silver/bus_occupancy"
SILVER_STOP_DEMAND    = f"{HDFS}/lakehouse/silver/stop_demand"
SILVER_TXN_ENRICHED   = f"{HDFS}/lakehouse/silver/transactions_enriched"

# Gold (output — Delta tables)
GOLD_STOP_OVERCAPACITY = f"{HDFS}/lakehouse/gold/stop_overcapacity_predictions"
GOLD_CONGESTION_INDEX  = f"{HDFS}/lakehouse/gold/shelter_congestion_index"
GOLD_BUS_ARRIVAL_OCC   = f"{HDFS}/lakehouse/gold/bus_arrival_occupancy_predictions"

# JSON exports (untuk Flask API)
EXPORT_DIR = "/opt/spark/work-dir/gold_exports"

# ML config
MIN_ROWS_FOR_ML = 50  # minimum rows untuk training GBTRegressor


# ============================================================
# 3. HELPERS
# ============================================================
def merge_or_create(df, path, merge_condition):
    """MERGE jika Delta table sudah ada, CREATE jika belum."""
    try:
        is_delta = DeltaTable.isDeltaTable(spark, path)
    except Exception:
        is_delta = False

    if is_delta:
        DeltaTable.forPath(spark, path).alias("target").merge(
            df.alias("source"), merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        df.write.format("delta").mode("overwrite").save(path)


def serialize_row(row):
    """Convert Spark Row ke dict yang JSON-serializable."""
    d = row.asDict()
    for k, v in d.items():
        if isinstance(v, (datetime, date)):
            d[k] = v.isoformat()
        elif isinstance(v, float) and (v != v):  # NaN
            d[k] = None
    return d


def export_json(df, filename, limit=500):
    """Export DataFrame ke JSON file untuk API consumption."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    rows = df.limit(limit).collect()
    data = [serialize_row(r) for r in rows]
    payload = {
        "data": data,
        "count": len(data),
        "updated_at": datetime.now().isoformat(),
        "bus_capacity": BUS_CAPACITY,
    }
    filepath = os.path.join(EXPORT_DIR, filename)
    with open(filepath, "w") as f:
        json.dump(payload, f, default=str)
    print(f"    📄 Exported {len(data)} rows → {filepath}")


# ============================================================
# 4. MODEL 1: STOP OVERCAPACITY RISK PREDICTION
# ============================================================
def model_stop_overcapacity(df_stop_demand):
    """
    Prediksi halte dengan risiko overkapasitas dalam 15 menit ke depan.
    
    Features: demand lag (5/10/15 menit lalu), hour, rush_hour flag
    Label: demand 15 menit ke depan (3 window x 5 menit)
    Model: GBTRegressor
    """
    print("\n🔮 Model 1: Stop Overcapacity Risk Prediction...")

    if df_stop_demand.count() == 0:
        print("    ⚠️ Tidak ada data stop_demand, skip model 1")
        return

    # --- Feature Engineering ---
    w = Window.partitionBy("stop_name").orderBy("window_start")

    df_feat = df_stop_demand \
        .withColumn("demand_lag_1", F.lag("demand_count", 1).over(w)) \
        .withColumn("demand_lag_2", F.lag("demand_count", 2).over(w)) \
        .withColumn("demand_lag_3", F.lag("demand_count", 3).over(w)) \
        .withColumn("demand_future_3", F.lead("demand_count", 3).over(w)) \
        .withColumn("hour_of_day", F.hour("window_start")) \
        .withColumn("is_rush_hour",
                    F.when(
                        (F.col("hour_of_day").between(6, 9)) |
                        (F.col("hour_of_day").between(16, 19)), 1
                    ).otherwise(0)) \
        .withColumn("demand_trend",
                    F.col("demand_count") - F.coalesce(F.col("demand_lag_1"), F.col("demand_count")))

    # Rows yang lengkap untuk training (punya lag + future label)
    df_train_ready = df_feat.filter(
        F.col("demand_lag_1").isNotNull() &
        F.col("demand_lag_2").isNotNull() &
        F.col("demand_lag_3").isNotNull() &
        F.col("demand_future_3").isNotNull()
    )

    train_count = df_train_ready.count()
    print(f"    Training rows: {train_count}")

    # --- Feature columns ---
    feature_cols = [
        "demand_count", "demand_lag_1", "demand_lag_2", "demand_lag_3",
        "hour_of_day", "is_rush_hour", "demand_trend"
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="skip"
    )

    if train_count >= MIN_ROWS_FOR_ML:
        # --- Train GBTRegressor ---
        gbt = GBTRegressor(
            featuresCol="features",
            labelCol="demand_future_3",
            maxIter=50,
            maxDepth=5,
            seed=42,
        )
        pipeline = Pipeline(stages=[assembler, gbt])

        train_df, test_df = df_train_ready.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_df)

        # Evaluate
        test_pred = model.transform(test_df)
        if test_pred.count() > 0:
            evaluator = RegressionEvaluator(
                labelCol="demand_future_3", metricName="rmse"
            )
            rmse = evaluator.evaluate(test_pred)
            print(f"    📊 GBT RMSE: {rmse:.2f}")

        # Score ALL current data (termasuk yang tidak punya future label)
        df_scoreable = df_feat.filter(
            F.col("demand_lag_1").isNotNull() &
            F.col("demand_lag_2").isNotNull() &
            F.col("demand_lag_3").isNotNull()
        )
        df_scored = model.transform(df_scoreable)
        predicted_col = "prediction"
    else:
        # --- Fallback: heuristic (moving average + trend) ---
        print(f"    ⚠️ Data < {MIN_ROWS_FOR_ML}, pakai heuristic fallback")
        df_scoreable = df_feat.filter(F.col("demand_lag_1").isNotNull())
        df_scored = df_scoreable.withColumn(
            "prediction",
            (F.col("demand_count") + F.coalesce(F.col("demand_lag_1"), F.lit(0))) / 2
            + F.col("demand_trend") * 3  # extrapolate trend 15 min
        )
        predicted_col = "prediction"

    # --- Compute risk score & level ---
    df_result = df_scored.withColumn(
        "predicted_demand_15min",
        F.greatest(F.round(F.col(predicted_col), 0).cast("int"), F.lit(0))
    ).withColumn(
        "overcapacity_risk_score",
        F.round(F.col("predicted_demand_15min") / F.lit(BUS_CAPACITY), 3)
    ).withColumn(
        "risk_level",
        F.when(F.col("overcapacity_risk_score") >= 0.85, "CRITICAL")
         .when(F.col("overcapacity_risk_score") >= 0.6, "HIGH")
         .when(F.col("overcapacity_risk_score") >= 0.3, "MODERATE")
         .otherwise("LOW")
    )

    # Ambil window terbaru per halte
    w_latest = Window.partitionBy("stop_name").orderBy(F.col("window_start").desc())
    df_latest = df_result \
        .withColumn("_rn", F.row_number().over(w_latest)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    df_output = df_latest.select(
        "stop_name", "stop_lat", "stop_lon",
        "corridor_id", "corridor_name",
        "demand_count", "predicted_demand_15min",
        "overcapacity_risk_score", "risk_level",
        "hour_of_day", "is_rush_hour",
        "window_start", "window_end",
    )

    # MERGE to Gold + Export JSON
    merge_or_create(df_output, GOLD_STOP_OVERCAPACITY,
                    "target.stop_name = source.stop_name")
    export_json(df_output, "stop_overcapacity.json")
    print(f"    ✅ Stop overcapacity predictions: {df_output.count()} stops")


# ============================================================
# 5. MODEL 2: SHELTER CONGESTION INDEX
# ============================================================
def model_shelter_congestion(df_stop_demand, df_bus_occ):
    """
    Hitung Shelter Congestion Index per halte.
    
    congestion_index = current_demand / effective_supply
    effective_supply = (avg buses/hour at stop) * BUS_CAPACITY / 12
    
    12 = jumlah window 5 menit per jam
    """
    print("\n📊 Model 2: Shelter Congestion Index...")

    if df_stop_demand.count() == 0:
        print("    ⚠️ Tidak ada data stop_demand, skip model 2")
        return

    # --- Hitung rata-rata bus per jam per halte ---
    # Dari bus_occupancy, hitung unique bus_id per current_stop per jam
    if df_bus_occ.count() > 0:
        df_bus_per_stop = df_bus_occ \
            .withColumn("hour", F.hour("sim_time")) \
            .groupBy("current_stop", "hour") \
            .agg(F.countDistinct("bus_id").alias("buses_this_hour")) \
            .groupBy("current_stop") \
            .agg(F.avg("buses_this_hour").alias("avg_buses_per_hour"))
    else:
        # Fallback jika belum ada data occupancy
        df_bus_per_stop = df_stop_demand.select(
            F.col("stop_name").alias("current_stop")
        ).distinct().withColumn("avg_buses_per_hour", F.lit(2.0))

    # --- Ambil demand terbaru per halte ---
    w_latest = Window.partitionBy("stop_name").orderBy(F.col("window_start").desc())
    df_demand_latest = df_stop_demand \
        .withColumn("_rn", F.row_number().over(w_latest)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    # --- Join & compute congestion index ---
    df_congestion = df_demand_latest.alias("d").join(
        df_bus_per_stop.alias("b"),
        F.col("d.stop_name") == F.col("b.current_stop"),
        "left"
    ).withColumn(
        "avg_buses_per_hour",
        F.coalesce(F.col("b.avg_buses_per_hour"), F.lit(2.0))
    ).withColumn(
        # effective_supply = buses per 5-min window * capacity
        "effective_supply",
        (F.col("avg_buses_per_hour") / 12.0) * F.lit(BUS_CAPACITY)
    ).withColumn(
        "congestion_index",
        F.round(
            F.when(F.col("effective_supply") > 0,
                   F.col("demand_count") / F.col("effective_supply"))
             .otherwise(F.lit(0.0)),
            3
        )
    ).withColumn(
        "congestion_level",
        F.when(F.col("congestion_index") >= 0.85, "CRITICAL")
         .when(F.col("congestion_index") >= 0.6, "HIGH")
         .when(F.col("congestion_index") >= 0.3, "MODERATE")
         .otherwise("LOW")
    )

    df_output = df_congestion.select(
        F.col("d.stop_name").alias("stop_name"),
        F.col("d.stop_lat").alias("stop_lat"),
        F.col("d.stop_lon").alias("stop_lon"),
        F.col("d.corridor_id"),
        F.col("d.corridor_name"),
        "demand_count",
        "avg_buses_per_hour",
        "effective_supply",
        "congestion_index",
        "congestion_level",
        F.col("d.window_start").alias("window_start"),
        F.col("d.window_end").alias("window_end"),
    )

    # MERGE to Gold + Export JSON
    merge_or_create(df_output, GOLD_CONGESTION_INDEX,
                    "target.stop_name = source.stop_name")
    export_json(df_output, "shelter_congestion.json")
    print(f"    ✅ Congestion index: {df_output.count()} stops")


# ============================================================
# 6. MODEL 3: BUS ARRIVAL OCCUPANCY PREDICTION
# ============================================================
def model_bus_arrival_occupancy(df_bus_occ, df_stop_demand):
    """
    Prediksi tingkat keterisian bus saat tiba di halte berikutnya.
    
    Features: current_occupancy, occupancy_ratio, speed, hour, rush_hour, next_stop_demand
    Label: occupancy saat bus tiba di next_stop (dari data historis)
    Model: GBTRegressor
    """
    print("\n🚌 Model 3: Bus Arrival Occupancy Prediction...")

    if df_bus_occ.count() == 0:
        print("    ⚠️ Tidak ada data bus_occupancy, skip model 3")
        return

    # --- Feature Engineering ---
    w_bus = Window.partitionBy("bus_id").orderBy("sim_time")

    # Detect stop changes: saat bus pindah ke halte berikutnya
    df_enriched = df_bus_occ \
        .withColumn("prev_stop", F.lag("current_stop", 1).over(w_bus)) \
        .withColumn("next_record_occupancy", F.lead("occupancy", 1).over(w_bus)) \
        .withColumn("next_record_stop", F.lead("current_stop", 1).over(w_bus)) \
        .withColumn("hour_of_day", F.hour("sim_time")) \
        .withColumn("is_rush_hour",
                    F.when(
                        (F.col("hour_of_day").between(6, 9)) |
                        (F.col("hour_of_day").between(16, 19)), 1
                    ).otherwise(0)) \
        .withColumn("occupancy_ratio",
                    F.round(F.col("occupancy") / F.lit(BUS_CAPACITY), 3))

    # Hitung rata-rata occupancy per route
    df_route_avg = df_bus_occ.groupBy("route_id") \
        .agg(F.avg("occupancy").alias("route_avg_occupancy"))

    df_enriched = df_enriched.join(
        df_route_avg, on="route_id", how="left"
    ).withColumn(
        "route_avg_occupancy",
        F.coalesce(F.col("route_avg_occupancy"), F.lit(0.0))
    )

    # Join demand di next_stop
    w_demand = Window.partitionBy("stop_name").orderBy(F.col("window_start").desc())
    df_demand_latest = df_stop_demand \
        .withColumn("_rn", F.row_number().over(w_demand)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn") \
        .select(
            F.col("stop_name"),
            F.col("demand_count").alias("next_stop_demand")
        )

    df_enriched = df_enriched.join(
        df_demand_latest,
        df_enriched["next_stop"] == df_demand_latest["stop_name"],
        "left"
    ).drop("stop_name").withColumn(
        "next_stop_demand",
        F.coalesce(F.col("next_stop_demand"), F.lit(0))
    )

    # Training data: rows dimana bus sudah pindah ke stop baru
    # Label = occupancy saat bus sampai di next_stop (next_record_occupancy)
    df_train_ready = df_enriched.filter(
        F.col("next_record_occupancy").isNotNull() &
        F.col("occupancy_ratio").isNotNull()
    )

    train_count = df_train_ready.count()
    print(f"    Training rows: {train_count}")

    feature_cols = [
        "occupancy", "occupancy_ratio", "speed_kmh",
        "hour_of_day", "is_rush_hour",
        "next_stop_demand", "route_avg_occupancy"
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="skip"
    )

    if train_count >= MIN_ROWS_FOR_ML:
        # --- Train GBTRegressor ---
        gbt = GBTRegressor(
            featuresCol="features",
            labelCol="next_record_occupancy",
            maxIter=50,
            maxDepth=5,
            seed=42,
        )
        pipeline = Pipeline(stages=[assembler, gbt])

        train_df, test_df = df_train_ready.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_df)

        # Evaluate
        test_pred = model.transform(test_df)
        if test_pred.count() > 0:
            evaluator = RegressionEvaluator(
                labelCol="next_record_occupancy", metricName="rmse"
            )
            rmse = evaluator.evaluate(test_pred)
            print(f"    📊 GBT RMSE: {rmse:.2f}")

        # Score latest per bus
        df_scored = model.transform(df_enriched.filter(
            F.col("occupancy_ratio").isNotNull()
        ))
        pred_col = "prediction"
    else:
        # Fallback: simple heuristic
        print(f"    ⚠️ Data < {MIN_ROWS_FOR_ML}, pakai heuristic fallback")
        df_scored = df_enriched.withColumn(
            "prediction",
            F.col("occupancy") + (F.col("next_stop_demand") * 0.3)
            - (F.col("occupancy") * 0.1)  # some passengers alight
        )
        pred_col = "prediction"

    # Ambil record terbaru per bus
    w_latest = Window.partitionBy("bus_id").orderBy(F.col("sim_time").desc())
    df_latest = df_scored \
        .withColumn("_rn", F.row_number().over(w_latest)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    df_output = df_latest.select(
        "bus_id", "route_id", "route_name",
        "current_stop", "next_stop",
        "occupancy",
        F.greatest(F.round(F.col(pred_col), 0).cast("int"), F.lit(0))
         .alias("predicted_occupancy"),
        F.round(
            F.greatest(F.col(pred_col), F.lit(0)) / F.lit(BUS_CAPACITY) * 100, 1
        ).alias("predicted_occupancy_pct"),
        "speed_kmh",
        F.col("sim_time").alias("prediction_time"),
    )

    # MERGE to Gold + Export JSON
    merge_or_create(df_output, GOLD_BUS_ARRIVAL_OCC,
                    "target.bus_id = source.bus_id")
    export_json(df_output, "bus_arrival_occupancy.json")
    print(f"    ✅ Bus arrival predictions: {df_output.count()} buses")


# ============================================================
# 7. EXPORT CURRENT BUS OCCUPANCY SNAPSHOT
# ============================================================
def export_bus_occupancy_snapshot(df_bus_occ):
    """Export occupancy terbaru per bus sebagai JSON untuk dashboard."""
    print("\n📸 Exporting current bus occupancy snapshot...")

    if df_bus_occ.count() == 0:
        print("    ⚠️ Tidak ada data, skip snapshot")
        export_json(spark.createDataFrame([], df_bus_occ.schema),
                    "bus_occupancy_current.json", limit=1)
        return

    w = Window.partitionBy("bus_id").orderBy(F.col("sim_time").desc())
    df_latest = df_bus_occ \
        .withColumn("_rn", F.row_number().over(w)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn") \
        .withColumn("occupancy_pct",
                    F.round(F.col("occupancy") / F.lit(BUS_CAPACITY) * 100, 1)) \
        .withColumn("occupancy_level",
                    F.when(F.col("occupancy_pct") >= 85, "FULL")
                     .when(F.col("occupancy_pct") >= 60, "HIGH")
                     .when(F.col("occupancy_pct") >= 30, "MODERATE")
                     .otherwise("LOW"))

    df_output = df_latest.select(
        "bus_id", "route_id", "route_name",
        "current_stop", "next_stop",
        "occupancy", "occupancy_pct", "occupancy_level",
        "speed_kmh", "lat", "lon", "sim_time",
    )

    export_json(df_output, "bus_occupancy_current.json", limit=1000)
    print(f"    ✅ Snapshot: {df_output.count()} buses")


# ============================================================
# 8. MAIN GOLD BATCH JOB
# ============================================================
def run_gold_batch():
    print("=" * 65)
    print("🏅 GOLD LAYER — ML Predictions & Metrics")
    print(f"   BUS_CAPACITY = {BUS_CAPACITY}")
    print("=" * 65)

    # --- Read Silver tables ---
    try:
        df_bus_occ = spark.read.format("delta").load(SILVER_BUS_OCCUPANCY)
        print(f"\n📖 bus_occupancy: {df_bus_occ.count():,} rows")
    except Exception as e:
        print(f"\n⚠️ Silver bus_occupancy belum tersedia: {e}")
        df_bus_occ = spark.createDataFrame([], "bus_id STRING, sim_time TIMESTAMP, "
                     "lat DOUBLE, lon DOUBLE, route_id STRING, route_name STRING, "
                     "current_stop STRING, next_stop STRING, speed_kmh DOUBLE, "
                     "occupancy INT")

    try:
        df_stop_demand = spark.read.format("delta").load(SILVER_STOP_DEMAND)
        print(f"📖 stop_demand: {df_stop_demand.count():,} rows")
    except Exception as e:
        print(f"⚠️ Silver stop_demand belum tersedia: {e}")
        df_stop_demand = spark.createDataFrame([], "stop_name STRING, "
                          "window_start TIMESTAMP, window_end TIMESTAMP, "
                          "demand_count INT, stop_lat DOUBLE, stop_lon DOUBLE, "
                          "corridor_id STRING, corridor_name STRING")

    # Cache for reuse
    df_bus_occ.cache()
    df_stop_demand.cache()

    # --- Run 3 Models ---
    model_stop_overcapacity(df_stop_demand)
    model_shelter_congestion(df_stop_demand, df_bus_occ)
    model_bus_arrival_occupancy(df_bus_occ, df_stop_demand)
    export_bus_occupancy_snapshot(df_bus_occ)

    # Cleanup
    df_bus_occ.unpersist()
    df_stop_demand.unpersist()

    print("\n" + "=" * 65)
    print("✅ GOLD LAYER — Batch selesai!")
    print("=" * 65)


# ============================================================
# 9. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    BATCH_INTERVAL = 300  # 5 menit

    print("🏅 Gold Layer — ML Prediction Batch Job")
    print(f"   Interval     : {BATCH_INTERVAL}s")
    print(f"   BUS_CAPACITY : {BUS_CAPACITY}")
    print(f"   Input        : {SILVER_BUS_OCCUPANCY}")
    print(f"                  {SILVER_STOP_DEMAND}")
    print(f"   JSON export  : {EXPORT_DIR}")

    while True:
        try:
            run_gold_batch()
        except Exception as e:
            print(f"\n❌ Error di Gold batch: {e}")
            import traceback
            traceback.print_exc()

        print(f"\n⏳ Menunggu {BATCH_INTERVAL}s sebelum batch berikutnya...\n")
        time.sleep(BATCH_INTERVAL)
