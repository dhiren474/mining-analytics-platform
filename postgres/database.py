# postgres/database.py
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Optional
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     os.getenv("POSTGRES_PORT", 5432),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

@contextmanager
def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def insert_sensor_reading(reading: dict):
    sql = """
        INSERT INTO sensor_readings (
            equipment_id, recorded_at,
            engine_temp_c, engine_rpm, oil_pressure_kpa, vibration_mm_s,
            fuel_level_pct, fuel_burn_rate, payload_tonnes, drill_rpm,
            dig_force_kn, speed_kmh, latitude, longitude,
            status, hours_since_service, fault_code
        ) VALUES (
            %(equipment_id)s, %(timestamp)s,
            %(engine_temp_c)s, %(engine_rpm)s, %(oil_pressure_kpa)s, %(vibration_mm_s)s,
            %(fuel_level_pct)s, %(fuel_burn_rate)s, %(payload_tonnes)s, %(drill_rpm)s,
            %(dig_force_kn)s, %(speed_kmh)s, %(latitude)s, %(longitude)s,
            %(status)s, %(hours_since_service)s, %(fault_code)s
        )
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, reading)

def insert_alert(alert: dict):
    sql = """
        INSERT INTO alerts (
            equipment_id, site_id, triggered_at,
            alert_type, severity, metric, value, threshold, message
        ) VALUES (
            %(equipment_id)s, %(site_id)s, %(timestamp)s,
            %(alert_type)s, %(severity)s, %(metric)s,
            %(value)s, %(threshold)s, %(message)s
        )
        ON CONFLICT DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, alert)

def bulk_insert_readings(readings: list[dict]):
    """Batch insert for Spark consumer — much faster than one-by-one."""
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
    with get_connection() as conn:
        with conn.cursor() as cur:
            values = [(
                r["equipment_id"], r["timestamp"],
                r.get("engine_temp_c"), r.get("engine_rpm"),
                r.get("oil_pressure_kpa"), r.get("vibration_mm_s"),
                r.get("fuel_level_pct"), r.get("fuel_burn_rate"),
                r.get("payload_tonnes"), r.get("drill_rpm"),
                r.get("dig_force_kn"), r.get("speed_kmh"),
                r.get("latitude"), r.get("longitude"),
                r.get("status"), r.get("hours_since_service"),
                r.get("fault_code"),
            ) for r in readings]
            psycopg2.extras.execute_values(cur, sql, values, page_size=500)
            logger.info(f"Bulk inserted {len(readings)} readings")
