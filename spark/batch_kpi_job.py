# spark/batch_kpi_job.py
"""
Runs daily via Airflow DAG.
Reads yesterday's sensor_readings and writes aggregated KPIs to daily_kpi table.
"""
import os
from datetime import date, timedelta
from pyspark.sql import functions as F
from session import create_spark_session
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL   = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
POSTGRES_PROPS = {
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver":   "org.postgresql.Driver",
}

def run_daily_kpi(target_date: date = None):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    spark = create_spark_session("MiningAnalytics-BatchKPI")

    # Read yesterday's partition from Postgres
    readings = (
        spark.read.jdbc(
            url=POSTGRES_URL,
            table="sensor_readings",
            properties=POSTGRES_PROPS,
        )
        .filter(F.to_date("recorded_at") == str(target_date))
    )

    # Read equipment reference data
    equipment = spark.read.jdbc(
        url=POSTGRES_URL,
        table="equipment",
        properties=POSTGRES_PROPS,
    )

    # Read alerts for this date
    alerts = (
        spark.read.jdbc(url=POSTGRES_URL, table="alerts", properties=POSTGRES_PROPS)
        .filter(F.to_date("triggered_at") == str(target_date))
    )

    # Aggregate KPIs per equipment per day
    kpis = (
        readings
        .groupBy("equipment_id")
        .agg(
            F.avg("engine_temp_c")         .alias("avg_engine_temp_c"),
            F.max("engine_temp_c")         .alias("max_engine_temp_c"),
            F.avg("vibration_mm_s")        .alias("avg_vibration_mm_s"),
            F.max("vibration_mm_s")        .alias("max_vibration_mm_s"),
            F.sum("fuel_burn_rate")        .alias("total_fuel_litres"),
            # Operating hours = count of readings in 'operating' status * interval
            (F.sum(F.when(F.col("status") == "operating", 1).otherwise(0)) / 360).alias("operating_hours"),
            (F.sum(F.when(F.col("status") == "idle",      1).otherwise(0)) / 360).alias("idle_hours"),
            (F.sum(F.when(F.col("status") == "fault",     1).otherwise(0)) / 360).alias("fault_hours"),
            F.sum("payload_tonnes")        .alias("total_payload_tonnes"),
            F.avg("drill_rpm")             .alias("avg_drill_rpm"),
        )
        .withColumn("kpi_date",   F.lit(str(target_date)))
        .withColumn("uptime_pct",
            F.round(F.col("operating_hours") / 24 * 100, 2)
        )
    )

    # Join alert counts
    alert_counts = (
        alerts
        .groupBy("equipment_id")
        .agg(
            F.count("*")                                           .alias("alert_count"),
            F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).alias("critical_alert_count"),
        )
    )

    final = (
        kpis
        .join(equipment.select("equipment_id", "site_id"), "equipment_id", "left")
        .join(alert_counts, "equipment_id", "left")
        .fillna(0, subset=["alert_count", "critical_alert_count"])
    )

    # Write to daily_kpi (upsert pattern)
    final.write.jdbc(
        url=POSTGRES_URL,
        table="daily_kpi",
        mode="append",
        properties={**POSTGRES_PROPS, "batchsize": "100"},
    )

    print(f"KPI batch complete for {target_date} — {final.count()} equipment rows written")
    spark.stop()

if __name__ == "__main__":
    run_daily_kpi()
