# spark/streaming_job.py
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from session import create_spark_session
from schemas import SENSOR_EVENT_SCHEMA, ALERT_SCHEMA
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_URL   = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
POSTGRES_PROPS = {
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver":   "org.postgresql.Driver",
}
S3_BUCKET      = os.getenv("S3_BUCKET", "mining-analytics-raw")
CHECKPOINT_DIR = "/tmp/spark-checkpoints"


# ── 1. Read from Kafka ────────────────────────────────────────────────────────

def read_sensor_stream(spark: SparkSession) -> DataFrame:
    """Read raw sensor events from Kafka and parse JSON."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",  KAFKA_SERVERS)
        .option("subscribe",                "sensor-events")
        .option("startingOffsets",          "latest")
        .option("failOnDataLoss",           "false")
        .option("maxOffsetsPerTrigger",     10000)       # backpressure control
        .load()
    )

    # Kafka gives us bytes — parse to our schema
    parsed = (
        raw
        .select(F.from_json(
            F.col("value").cast(StringType()),
            SENSOR_EVENT_SCHEMA
        ).alias("data"), "timestamp")
        .select("data.*", F.col("timestamp").alias("kafka_timestamp"))
        .withWatermark("timestamp", "10 minutes")        # late data tolerance
    )

    return parsed


# ── 2. Anomaly detection ──────────────────────────────────────────────────────

def detect_anomalies(df: DataFrame) -> DataFrame:
    """
    Flag readings that breach operational thresholds.
    Returns original df enriched with anomaly columns.
    """
    return df.withColumn(
        "anomaly_flags",
        F.array(
            F.when(F.col("engine_temp_c")    > 105,  F.lit("HIGH_ENGINE_TEMP")).otherwise(F.lit(None)),
            F.when(F.col("engine_temp_c")    > 100,  F.lit("WARN_ENGINE_TEMP")).otherwise(F.lit(None)),
            F.when(F.col("vibration_mm_s")   > 15,   F.lit("HIGH_VIBRATION")).otherwise(F.lit(None)),
            F.when(F.col("vibration_mm_s")   > 10,   F.lit("WARN_VIBRATION")).otherwise(F.lit(None)),
            F.when(F.col("oil_pressure_kpa") < 150,  F.lit("CRITICAL_LOW_OIL")).otherwise(F.lit(None)),
            F.when(F.col("oil_pressure_kpa") < 200,  F.lit("WARN_LOW_OIL")).otherwise(F.lit(None)),
            F.when(F.col("fuel_level_pct")   < 5,    F.lit("CRITICAL_LOW_FUEL")).otherwise(F.lit(None)),
            F.when(F.col("fuel_level_pct")   < 15,   F.lit("WARN_LOW_FUEL")).otherwise(F.lit(None)),
            F.when(F.col("hours_since_service") > 250, F.lit("SERVICE_DUE")).otherwise(F.lit(None)),
        )
    ).withColumn(
        "has_anomaly",
        F.exists("anomaly_flags", lambda x: x.isNotNull())
    ).withColumn(
        "is_critical",
        F.exists("anomaly_flags", lambda x: x.startswith("CRITICAL") | x.startswith("HIGH"))
    )


# ── 3. 5-minute windowed aggregations ────────────────────────────────────────

def build_window_aggregations(df: DataFrame) -> DataFrame:
    """
    Rolling 5-min window stats per equipment.
    Powers the real-time trend charts in Grafana.
    """
    return (
        df
        .groupBy(
            F.window("timestamp", "5 minutes", "1 minute"),   # sliding window
            "equipment_id",
            "site_id",
            "equipment_type",
        )
        .agg(
            F.avg("engine_temp_c")       .alias("avg_engine_temp"),
            F.max("engine_temp_c")       .alias("max_engine_temp"),
            F.avg("vibration_mm_s")      .alias("avg_vibration"),
            F.max("vibration_mm_s")      .alias("max_vibration"),
            F.avg("fuel_level_pct")      .alias("avg_fuel_level"),
            F.min("fuel_level_pct")      .alias("min_fuel_level"),
            F.avg("oil_pressure_kpa")    .alias("avg_oil_pressure"),
            F.avg("engine_rpm")          .alias("avg_rpm"),
            F.avg("speed_kmh")           .alias("avg_speed"),
            F.sum("fuel_burn_rate")      .alias("total_fuel_burn"),
            F.count("*")                 .alias("reading_count"),
            F.sum(F.col("has_anomaly").cast("int")).alias("anomaly_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )


# ── 4. Write to PostgreSQL (foreachBatch) ─────────────────────────────────────

def write_to_postgres(batch_df: DataFrame, batch_id: int):
    """
    foreachBatch sink — runs on each micro-batch.
    Uses JDBC batch writes for efficiency.
    """
    count = batch_df.count()
    if count == 0:
        return

    logger.info(f"Batch {batch_id}: writing {count} rows to PostgreSQL")

    (
        batch_df
        .select(
            "equipment_id", "timestamp",
            "engine_temp_c", "engine_rpm", "oil_pressure_kpa", "vibration_mm_s",
            "fuel_level_pct", "fuel_burn_rate", "payload_tonnes", "drill_rpm",
            "dig_force_kn", "speed_kmh", "latitude", "longitude",
            "status", "hours_since_service", "fault_code",
        )
        .write
        .jdbc(
            url=POSTGRES_URL,
            table="sensor_readings",
            mode="append",
            properties={**POSTGRES_PROPS, "batchsize": "500"},
        )
    )


def write_alerts_to_postgres(batch_df: DataFrame, batch_id: int):
    """Write detected anomalies as alerts to PostgreSQL."""
    anomalies = batch_df.filter(F.col("has_anomaly") == True)
    count = anomalies.count()
    if count == 0:
        return

    logger.info(f"Batch {batch_id}: writing {count} alerts")

    # Explode anomaly_flags array — one row per alert type
    alerts = (
        anomalies
        .withColumn("alert_type", F.explode(
            F.filter("anomaly_flags", lambda x: x.isNotNull())
        ))
        .select(
            F.expr("uuid()").alias("alert_id"),
            "equipment_id", "site_id",
            F.col("timestamp").alias("triggered_at"),
            "alert_type",
            F.when(F.col("alert_type").startswith("CRITICAL") | F.col("alert_type").startswith("HIGH"),
                   F.lit("CRITICAL")).otherwise(F.lit("WARNING")).alias("severity"),
            F.lit("sensor").alias("metric"),
            F.lit(0.0).alias("value"),
            F.lit(0.0).alias("threshold"),
            F.concat(F.col("equipment_id"), F.lit(": "), F.col("alert_type")).alias("message"),
        )
    )

    alerts.write.jdbc(
        url=POSTGRES_URL,
        table="alerts",
        mode="append",
        properties=POSTGRES_PROPS,
    )


# ── 5. Write to S3 as Parquet ─────────────────────────────────────────────────

def write_to_s3(batch_df: DataFrame, batch_id: int):
    """
    Archive raw events to S3 as Parquet, partitioned by site/date.
    Cheap long-term storage, queryable by Athena later.
    """
    if batch_df.count() == 0:
        return

    (
        batch_df
        .withColumn("year",  F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("day",   F.dayofmonth("timestamp"))
        .write
        .mode("append")
        .partitionBy("site_id", "year", "month", "day")
        .parquet(f"s3a://{S3_BUCKET}/sensor-events/")
    )


# ── 6. Main entrypoint ────────────────────────────────────────────────────────

def main():
    spark = create_spark_session("MiningAnalytics-Streaming")
    logger.info("Spark session started")

    # Read stream
    sensor_stream = read_sensor_stream(spark)

    # Enrich with anomaly detection
    enriched = detect_anomalies(sensor_stream)

    # ── Query 1: Write raw readings to Postgres ─────────────────────────────
    q1 = (
        enriched.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/postgres-readings")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Query 2: Write alerts to Postgres ──────────────────────────────────
    q2 = (
        enriched.writeStream
        .foreachBatch(write_alerts_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/postgres-alerts")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Query 3: Archive to S3 ──────────────────────────────────────────────
    q3 = (
        enriched.writeStream
        .foreachBatch(write_to_s3)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/s3-archive")
        .trigger(processingTime="60 seconds")   # less frequent — cost saving
        .start()
    )

    # ── Query 4: Window aggregations to console (dev mode) ──────────────────
    window_aggs = build_window_aggregations(sensor_stream)
    q4 = (
        window_aggs.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/window-aggs")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("All streaming queries running — waiting for termination")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
