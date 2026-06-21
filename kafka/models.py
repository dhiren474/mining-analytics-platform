# kafka/models.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from enum import Enum

class EquipmentType(str, Enum):
    HAUL_TRUCK  = "haul_truck"
    DRILL_RIG   = "drill_rig"
    EXCAVATOR   = "excavator"

class EquipmentStatus(str, Enum):
    OPERATING   = "operating"
    IDLE        = "idle"
    MAINTENANCE = "maintenance"
    FAULT       = "fault"

class SensorEvent(BaseModel):
    event_id:         str
    equipment_id:     str
    equipment_type:   EquipmentType
    site_id:          str               # e.g. "PILBARA-01", "HUNTER-VALLEY-02"
    timestamp:        datetime

    # Engine & mechanical
    engine_temp_c:    float             # normal: 80–95°C, alert: >105°C
    engine_rpm:       float             # haul truck normal: 1200–1800
    oil_pressure_kpa: float             # normal: 280–450 kPa
    vibration_mm_s:   float             # normal: <7 mm/s, fault: >15 mm/s

    # Operational
    fuel_level_pct:   float             # 0–100%
    fuel_burn_rate:   float             # L/hr
    payload_tonnes:   Optional[float]   # haul truck only
    drill_rpm:        Optional[float]   # drill rig only
    dig_force_kn:     Optional[float]   # excavator only

    # Location
    latitude:         float
    longitude:        float
    speed_kmh:        float

    # Health
    status:           EquipmentStatus
    hours_since_service: float
    fault_code:       Optional[str] = None

class AlertEvent(BaseModel):
    alert_id:       str
    equipment_id:   str
    site_id:        str
    timestamp:      datetime
    alert_type:     str                 # "HIGH_TEMP", "LOW_OIL", "HIGH_VIBRATION"
    severity:       str                 # "WARNING", "CRITICAL"
    metric:         str
    value:          float
    threshold:      float
    message:        str
