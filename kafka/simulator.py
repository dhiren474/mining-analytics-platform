# kafka/simulator.py
import uuid, time, random, math
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from loguru import logger
from models import SensorEvent, AlertEvent, EquipmentType, EquipmentStatus

load_dotenv()

# Australian mine site coordinates (Pilbara WA, Hunter Valley NSW, Bowen Basin QLD)
MINE_SITES = {
    "PILBARA-01":       {"lat": -22.3, "lon": 118.5, "equipment_count": 5},
    "HUNTER-VALLEY-01": {"lat": -32.8, "lon": 151.3, "equipment_count": 3},
    "BOWEN-BASIN-01":   {"lat": -22.5, "lon": 148.0, "equipment_count": 4},
}

# Alert thresholds
THRESHOLDS = {
    "engine_temp_c":    {"warning": 100, "critical": 110},
    "vibration_mm_s":   {"warning": 10,  "critical": 15},
    "oil_pressure_kpa": {"warning": 200, "critical": 150},  # low is bad
    "fuel_level_pct":   {"warning": 15,  "critical": 5},
}


class MiningEquipment:
    """Simulates a single piece of mining equipment with realistic sensor drift."""

    def __init__(self, equipment_id: str, equipment_type: EquipmentType, site_id: str):
        self.equipment_id   = equipment_id
        self.equipment_type = equipment_type
        self.site_id        = site_id
        self.site           = MINE_SITES[site_id]
        self.status         = EquipmentStatus.OPERATING
        self.hours_since_service = random.uniform(0, 250)

        # State that drifts over time (makes data realistic)
        self._engine_temp   = random.uniform(80, 92)
        self._vibration     = random.uniform(1, 5)
        self._fuel_level    = random.uniform(40, 95)
        self._oil_pressure  = random.uniform(300, 420)
        self._fault_injected = False

    def _drift(self, value: float, center: float, noise: float, rate: float = 0.05) -> float:
        """Gradually drifts a value toward center with random noise."""
        drift = (center - value) * rate + random.gauss(0, noise)
        return value + drift

    def _maybe_inject_fault(self):
        """1% chance per reading of injecting a fault condition."""
        if not self._fault_injected and random.random() < 0.01:
            self._fault_injected = True
            self._engine_temp += random.uniform(15, 30)   # spike temp
            self._vibration   += random.uniform(8, 15)    # spike vibration
            logger.warning(f"🔴 Fault injected on {self.equipment_id}")

        # Fault recovers after temp drops
        if self._fault_injected and self._engine_temp < 98:
            self._fault_injected = False

    def generate_reading(self) -> SensorEvent:
        self._maybe_inject_fault()

        # Drift all sensors
        self._engine_temp  = self._drift(self._engine_temp, 87, 0.8)
        self._vibration    = self._drift(self._vibration, 3.5, 0.3)
        self._fuel_level   = max(0, self._fuel_level - random.uniform(0.01, 0.05))
        self._oil_pressure = self._drift(self._oil_pressure, 360, 5)
        self.hours_since_service += 1 / 360  # ~10 readings/min

        # Equipment-specific fields
        payload_tonnes = None
        drill_rpm      = None
        dig_force_kn   = None

        if self.equipment_type == EquipmentType.HAUL_TRUCK:
            payload_tonnes = random.uniform(180, 320) if self.status == EquipmentStatus.OPERATING else 0
            engine_rpm     = random.uniform(1300, 1750)
            speed_kmh      = random.uniform(0, 45)
            fuel_burn      = random.uniform(85, 130)

        elif self.equipment_type == EquipmentType.DRILL_RIG:
            drill_rpm  = random.uniform(60, 120)
            engine_rpm = random.uniform(1800, 2200)
            speed_kmh  = 0  # drills don't move while operating
            fuel_burn  = random.uniform(40, 65)

        elif self.equipment_type == EquipmentType.EXCAVATOR:
            dig_force_kn = random.uniform(400, 800)
            engine_rpm   = random.uniform(1500, 1900)
            speed_kmh    = random.uniform(0, 5)
            fuel_burn     = random.uniform(60, 95)

        # Determine status
        if self._fault_injected:
            status     = EquipmentStatus.FAULT
            fault_code = f"F{random.randint(100,999)}"
        elif self.hours_since_service > 250:
            status     = EquipmentStatus.MAINTENANCE
            fault_code = "MAINT-DUE"
        else:
            status     = EquipmentStatus.OPERATING
            fault_code = None

        # Add GPS jitter within site area
        lat = self.site["lat"] + random.uniform(-0.05, 0.05)
        lon = self.site["lon"] + random.uniform(-0.05, 0.05)

        return SensorEvent(
            event_id         = str(uuid.uuid4()),
            equipment_id     = self.equipment_id,
            equipment_type   = self.equipment_type,
            site_id          = self.site_id,
            timestamp        = datetime.now(timezone.utc),
            engine_temp_c    = round(self._engine_temp, 2),
            engine_rpm       = round(engine_rpm, 1),
            oil_pressure_kpa = round(self._oil_pressure, 1),
            vibration_mm_s   = round(self._vibration, 3),
            fuel_level_pct   = round(self._fuel_level, 2),
            fuel_burn_rate   = round(fuel_burn, 2),
            payload_tonnes   = round(payload_tonnes, 1) if payload_tonnes else None,
            drill_rpm        = round(drill_rpm, 1) if drill_rpm else None,
            dig_force_kn     = round(dig_force_kn, 1) if dig_force_kn else None,
            latitude         = round(lat, 6),
            longitude        = round(lon, 6),
            speed_kmh        = round(speed_kmh, 1),
            status           = status,
            hours_since_service = round(self.hours_since_service, 2),
            fault_code       = fault_code,
        )

    def check_alerts(self, reading: SensorEvent) -> list[AlertEvent]:
        alerts = []

        checks = [
            ("engine_temp_c",    reading.engine_temp_c,    "HIGH_ENGINE_TEMP"),
            ("vibration_mm_s",   reading.vibration_mm_s,   "HIGH_VIBRATION"),
            ("fuel_level_pct",   reading.fuel_level_pct,   "LOW_FUEL"),
            ("oil_pressure_kpa", reading.oil_pressure_kpa, "LOW_OIL_PRESSURE"),
        ]

        for metric, value, alert_type in checks:
            thresholds = THRESHOLDS[metric]
            is_low = "low" in alert_type.lower()

            critical = value < thresholds["critical"] if is_low else value > thresholds["critical"]
            warning  = value < thresholds["warning"]  if is_low else value > thresholds["warning"]

            if critical or warning:
                severity  = "CRITICAL" if critical else "WARNING"
                threshold = thresholds["critical"] if critical else thresholds["warning"]
                alerts.append(AlertEvent(
                    alert_id     = str(uuid.uuid4()),
                    equipment_id = self.equipment_id,
                    site_id      = self.site_id,
                    timestamp    = reading.timestamp,
                    alert_type   = alert_type,
                    severity     = severity,
                    metric       = metric,
                    value        = value,
                    threshold    = threshold,
                    message      = f"{severity}: {self.equipment_id} {metric}={value} (threshold={threshold})",
                ))

        return alerts
