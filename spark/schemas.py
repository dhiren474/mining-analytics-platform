# spark/schemas.py
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, BooleanType
)

# Must match exactly what the Kafka producer sends
SENSOR_EVENT_SCHEMA = StructType([
    StructField("event_id",              StringType(),    False),
    StructField("equipment_id",          StringType(),    False),
    StructField("equipment_type",        StringType(),    False),
    StructField("site_id",               StringType(),    False),
    StructField("timestamp",             TimestampType(), False),
    StructField("engine_temp_c",         DoubleType(),    True),
    StructField("engine_rpm",            DoubleType(),    True),
    StructField("oil_pressure_kpa",      DoubleType(),    True),
    StructField("vibration_mm_s",        DoubleType(),    True),
    StructField("fuel_level_pct",        DoubleType(),    True),
    StructField("fuel_burn_rate",        DoubleType(),    True),
    StructField("payload_tonnes",        DoubleType(),    True),
    StructField("drill_rpm",             DoubleType(),    True),
    StructField("dig_force_kn",          DoubleType(),    True),
    StructField("latitude",              DoubleType(),    True),
    StructField("longitude",             DoubleType(),    True),
    StructField("speed_kmh",             DoubleType(),    True),
    StructField("status",                StringType(),    True),
    StructField("hours_since_service",   DoubleType(),    True),
    StructField("fault_code",            StringType(),    True),
])

ALERT_SCHEMA = StructType([
    StructField("alert_id",     StringType(),    False),
    StructField("equipment_id", StringType(),    False),
    StructField("site_id",      StringType(),    False),
    StructField("timestamp",    TimestampType(), False),
    StructField("alert_type",   StringType(),    False),
    StructField("severity",     StringType(),    False),
    StructField("metric",       StringType(),    False),
    StructField("value",        DoubleType(),    False),
    StructField("threshold",    DoubleType(),    False),
    StructField("message",      StringType(),    False),
])
