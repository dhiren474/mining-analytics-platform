# kafka/simple_consumer.py
"""
Lightweight Kafka → Postgres consumer.
Runs automatically as a Docker service.
Replaces Spark for local dev — same logic, no RAM overhead.
"""
import json, os, time
import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

def insert_readings(cur, readings: list):
    if not readings:
        return
    sql = """
        INSERT INTO sensor_readings (
            equipment_id, recorded_at,
            engine_temp_c, engine_rpm, oil_pressure_kpa, vibration_mm_s,
            fuel_level_pct, fuel_burn_rate, payload_tonnes, drill_rpm,
            dig_force_kn, speed_kmh, latitude, longitude,
            status, hours_since_service, fault_code
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """
    values = [(
        r.get("equipment_id"), r.get("timestamp"),
        r.get("engine_temp_c"), r.get("engine_rpm"),
        r.get("oil_pressure_kpa"), r.get("vibration_mm_s"),
        r.get("fuel_level_pct"), r.get("fuel_burn_rate"),
        r.get("payload_tonnes"), r.get("drill_rpm"),
        r.get("dig_force_kn"), r.get("speed_kmh"),
        r.get("latitude"), r.get("longitude"),
        r.get("status"), r.get("hours_since_service"),
        r.get("fault_code"),
    ) for r in readings]
    psycopg2.extras.execute_values(cur, sql, values, page_size=100)

def insert_alerts(cur, alerts: list):
    if not alerts:
        return
    sql = """
        INSERT INTO alerts (
            equipment_id, site_id, triggered_at,
            alert_type, severity, metric,
            value, threshold, message
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """
    values = [(
        a.get("equipment_id"), a.get("site_id"), a.get("timestamp"),
        a.get("alert_type"), a.get("severity"), a.get("metric"),
        a.get("value"), a.get("threshold"), a.get("message"),
    ) for a in alerts]
    psycopg2.extras.execute_values(cur, sql, values, page_size=100)

def run():
    # Wait for Postgres to be ready
    logger.info("Waiting for Postgres...")
    for i in range(10):
        try:
            conn = get_db_connection()
            conn.close()
            logger.info("Postgres ready")
            break
        except Exception:
            logger.info(f"Postgres not ready, retry {i+1}/10")
            time.sleep(3)

    logger.info("Connecting to Kafka...")
    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="simple-pg-consumer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=-1,
    )
    consumer.subscribe(["sensor-events", "equipment-alerts"])
    logger.info("Consumer started — listening to sensor-events + equipment-alerts")

    conn = get_db_connection()
    conn.autocommit = False

    readings_batch = []
    alerts_batch   = []
    batch_size     = 50
    total_readings = 0
    total_alerts   = 0

    for message in consumer:
        try:
            data  = message.value
            topic = message.topic

            if topic == "sensor-events":
                readings_batch.append(data)
            elif topic == "equipment-alerts":
                alerts_batch.append(data)

            # Flush every 50 messages
            if len(readings_batch) >= batch_size or len(alerts_batch) >= batch_size:
                with conn.cursor() as cur:
                    insert_readings(cur, readings_batch)
                    insert_alerts(cur, alerts_batch)
                conn.commit()

                total_readings += len(readings_batch)
                total_alerts   += len(alerts_batch)

                if total_readings % 500 == 0 or total_readings < 100:
                    logger.info(
                        f"✅ Written — readings: {total_readings} | "
                        f"alerts: {total_alerts}"
                    )

                readings_batch = []
                alerts_batch   = []

        except psycopg2.Error as e:
            logger.error(f"DB error: {e}")
            conn.rollback()
            # Reconnect
            try:
                conn = get_db_connection()
                conn.autocommit = False
            except Exception as re:
                logger.error(f"Reconnect failed: {re}")
                time.sleep(5)

        except Exception as e:
            logger.error(f"Consumer error: {e}")

if __name__ == "__main__":
    run()
