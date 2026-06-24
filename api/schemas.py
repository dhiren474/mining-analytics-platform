# api/schemas.py
from pydantic import BaseModel
from datetime import datetime
from typing import Optional
from uuid import UUID

class EquipmentLatest(BaseModel):
    equipment_id:        str
    equipment_type:      str
    model:               Optional[str]
    site_id:             str
    recorded_at:         datetime
    engine_temp_c:       Optional[float]
    engine_rpm:          Optional[float]
    oil_pressure_kpa:    Optional[float]
    vibration_mm_s:      Optional[float]
    fuel_level_pct:      Optional[float]
    speed_kmh:           Optional[float]
    status:              Optional[str]
    fault_code:          Optional[str]
    hours_since_service: Optional[float]

    model_config = {"from_attributes": True}

class AlertSummary(BaseModel):
    alert_id:     UUID          # ← was str, now UUID
    equipment_id: str
    site_id:      str
    site_name:    str
    triggered_at: datetime
    alert_type:   str
    severity:     str
    metric:       str
    value:        float
    threshold:    float
    message:      str
    hours_open:   Optional[float]

    model_config = {"from_attributes": True}

class DailyKPI(BaseModel):
    equipment_id:         str
    site_id:              str
    kpi_date:             str
    avg_engine_temp_c:    Optional[float]
    max_engine_temp_c:    Optional[float]
    avg_vibration_mm_s:   Optional[float]
    total_fuel_litres:    Optional[float]
    operating_hours:      Optional[float]
    uptime_pct:           Optional[float]
    alert_count:          Optional[int]
    critical_alert_count: Optional[int]

    model_config = {"from_attributes": True}

class FleetHealth(BaseModel):
    site_id:         str
    site_name:       str
    equipment_type:  str
    total_machines:  int
    operating:       Optional[int]
    in_fault:        Optional[int]
    in_maintenance:  Optional[int]
    avg_fuel_pct:    Optional[float]
    avg_engine_temp: Optional[float]
    avg_vibration:   Optional[float]

    model_config = {"from_attributes": True}

class SensorHistory(BaseModel):
    recorded_at:      datetime
    engine_temp_c:    Optional[float]
    vibration_mm_s:   Optional[float]
    fuel_level_pct:   Optional[float]
    oil_pressure_kpa: Optional[float]
    engine_rpm:       Optional[float]
    status:           Optional[str]

    model_config = {"from_attributes": True}

class PipelineStats(BaseModel):
    total_readings:      int
    readings_last_hour:  int
    readings_last_5min:  int
    open_alerts:         int
    critical_alerts:     int
    equipment_in_fault:  int
    last_reading_at:     Optional[datetime]

    model_config = {"from_attributes": True}
