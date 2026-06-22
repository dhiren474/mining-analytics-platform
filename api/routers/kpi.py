# api/routers/kpi.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
from database import get_db
from schemas import DailyKPI, PipelineStats

router = APIRouter(prefix="/kpi", tags=["KPIs"])

@router.get("/daily", response_model=List[DailyKPI])
async def get_daily_kpi(
    days:         int          = Query(7,    ge=1, le=90),
    equipment_id: Optional[str] = Query(None),
    site_id:      Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Daily KPI history — powers trend charts and tables."""
    where_clauses = [f"kpi_date >= CURRENT_DATE - INTERVAL '{days} days'"]
    if equipment_id: where_clauses.append(f"equipment_id = '{equipment_id}'")
    if site_id:      where_clauses.append(f"site_id = '{site_id}'")
    where = "WHERE " + " AND ".join(where_clauses)

    result = await db.execute(text(f"""
        SELECT * FROM daily_kpi {where}
        ORDER BY kpi_date DESC, equipment_id
    """))
    return [dict(r) for r in result.mappings().all()]

@router.get("/pipeline/stats", response_model=PipelineStats)
async def get_pipeline_stats(db: AsyncSession = Depends(get_db)):
    """Real-time pipeline health stats — powers the header stat bar."""
    result = await db.execute(text("""
        SELECT
            (SELECT COUNT(*)   FROM sensor_readings)                                     AS total_readings,
            (SELECT COUNT(*)   FROM sensor_readings WHERE recorded_at >= NOW() - INTERVAL '1 hour')  AS readings_last_hour,
            (SELECT COUNT(*)   FROM sensor_readings WHERE recorded_at >= NOW() - INTERVAL '5 minutes') AS readings_last_5min,
            (SELECT COUNT(*)   FROM alerts          WHERE resolved_at IS NULL)           AS open_alerts,
            (SELECT COUNT(*)   FROM alerts          WHERE resolved_at IS NULL AND severity = 'CRITICAL') AS critical_alerts,
            (SELECT COUNT(DISTINCT equipment_id) FROM vw_equipment_latest WHERE status = 'fault') AS equipment_in_fault,
            (SELECT MAX(recorded_at) FROM sensor_readings)                               AS last_reading_at
    """))
    return dict(result.mappings().first())
