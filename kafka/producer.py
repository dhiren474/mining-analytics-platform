# kafka/producer.py
import json, time, os
from kafka import KafkaProducer
from dotenv import load_dotenv
from loguru import logger
from simulator import MiningEquipment
from models import EquipmentType
from create_topics import create_topics

load_dotenv()

def json_serialiser(obj):
    """Handle datetime serialisation."""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "value"):       # enums
        return obj.value
    raise TypeError(f"Type {type(obj)} not serialisable")

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        value_serializer=lambda v: json.dumps(v, default=json_serialiser).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",                     # wait for all replicas
        retries=5,
        max_in_flight_requests_per_connection=1,
    )

def build_fleet() -> list[MiningEquipment]:
    """Create a realistic mixed fleet across Australian mine sites."""
    fleet = []
    configs = [
        # (site,              type,           count)
        ("PILBARA-01",       "haul_truck",    3),
        ("PILBARA-01",       "excavator",     2),
        ("HUNTER-VALLEY-01", "haul_truck",    2),
        ("HUNTER-VALLEY-01", "drill_rig",     1),
        ("BOWEN-BASIN-01",   "haul_truck",    2),
        ("BOWEN-BASIN-01",   "drill_rig",     2),
    ]
    for site_id, eq_type, count in configs:
        for i in range(count):
            prefix = {"haul_truck": "HT", "drill_rig": "DR", "excavator": "EX"}[eq_type]
            eq_id  = f"{prefix}-{site_id[:3].upper()}-{i+1:03d}"
            fleet.append(MiningEquipment(eq_id, EquipmentType(eq_type), site_id))

    logger.info(f"Fleet initialised: {len(fleet)} machines across 3 sites")
    return fleet

def run_producer(interval_seconds: float = 0.5):
    create_topics()
    producer = build_producer()
    fleet    = build_fleet()

    logger.info(f"Producer running — emitting every {interval_seconds}s per machine")
    stats = {"sent": 0, "alerts": 0, "errors": 0}

    while True:
        for equipment in fleet:
            try:
                reading = equipment.generate_reading()
                data    = reading.model_dump()

                # Send to sensor-events (keyed by equipment_id for ordering)
                producer.send(
                    topic="sensor-events",
                    key=equipment.equipment_id,
                    value=data,
                )
                stats["sent"] += 1

                # Check and emit alerts
                for alert in equipment.check_alerts(reading):
                    producer.send(
                        topic="equipment-alerts",
                        key=equipment.equipment_id,
                        value=alert.model_dump(),
                    )
                    stats["alerts"] += 1
                    logger.warning(f"⚠️  Alert: {alert.message}")

                # Status heartbeat every 10 readings
                if stats["sent"] % 10 == 0:
                    producer.send(
                        topic="equipment-status",
                        key=equipment.equipment_id,
                        value={
                            "equipment_id": equipment.equipment_id,
                            "site_id":      equipment.site_id,
                            "status":       reading.status.value,
                            "timestamp":    reading.timestamp.isoformat(),
                        },
                    )

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Producer error for {equipment.equipment_id}: {e}")

        producer.flush()

        if stats["sent"] % 100 == 0:
            logger.info(f"📊 Stats — sent: {stats['sent']} | alerts: {stats['alerts']} | errors: {stats['errors']}")

        time.sleep(interval_seconds)

if __name__ == "__main__":
    run_producer(interval_seconds=0.5)
