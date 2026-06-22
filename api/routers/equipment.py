# api/routers/equipment.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
from database import get_db
from schemas import EquipmentLatest, SensorHistory, FleetHealth
from cache import cache
from loguru import logger

router = APIRouter(prefix="/equipment", tags=["Equipment"])

@router.get("/latest", response_model=List[EquipmentLatest])
async def get_all_equipment_latest(
    site_id: Optional[str] = Query(None, description="Filter by site"),
    status:  Optional[str] = Query(None, description="Filter by status"),
    db: AsyncSession = Depends(get_db),
):
    """Latest sensor reading for every machine — powers the fleet overview panel."""
    where_clauses = []
    if site_id: where_clauses.append(f"site_id = '{site_id}'")
    if status:  where_clauses.append(f"status = '{status}'")
    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    result = await db.execute(text(f"""
        SELECT * FROM vw_equipment_latest {where}
        ORDER BY site_id, equipment_id
    """))
    rows = result.mappings().all()
    return [dict(r) for r in rows]

@router.get("/{equipment_id}/history", response_model=List[SensorHistory])
async def get_equipment_history(
    equipment_id: str,
    hours: int = Query(24, ge=1, le=168, description="Hours of history"),
    db: AsyncSession = Depends(get_db),
):
    """Time-series sensor history — powers the trend charts in Grafana."""
    result = await db.execute(text(f"""
        SELECT
            recorded_at, engine_temp_c, vibration_mm_s,
            fuel_level_pct, oil_pressure_kpa, engine_rpm, status
        FROM sensor_readings
        WHERE equipment_id = :equipment_id
        AND recorded_at >= NOW() - INTERVAL '{hours} hours'
        ORDER BY recorded_at ASC
    """), {"equipment_id": equipment_id})

    rows = result.mappings().all()
    if not rows:
        raise HTTPException(404, f"No data found for equipment {equipment_id}")
    return [dict(r) for r in rows]

@router.get("/fleet/health", response_model=List[FleetHealth])
async def get_fleet_health(db: AsyncSession = Depends(get_db)):
    """Fleet health summary per site — powers the site overview cards."""
    result = await db.execute(text("SELECT * FROM vw_site_fleet_health"))
    return [dict(r) for r in result.mappings().all()]
