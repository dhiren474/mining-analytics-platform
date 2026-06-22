# api/routers/alerts.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
from database import get_db
from schemas import AlertSummary

router = APIRouter(prefix="/alerts", tags=["Alerts"])

@router.get("/open", response_model=List[AlertSummary])
async def get_open_alerts(
    severity: Optional[str] = Query(None, description="CRITICAL or WARNING"),
    site_id:  Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """All unresolved alerts — powers the alert panel in Grafana."""
    where_clauses = []
    if severity: where_clauses.append(f"severity = '{severity}'")
    if site_id:  where_clauses.append(f"site_id = '{site_id}'")
    where = "AND " + " AND ".join(where_clauses) if where_clauses else ""

    result = await db.execute(text(f"""
        SELECT * FROM vw_open_alerts {where} LIMIT 100
    """))
    return [dict(r) for r in result.mappings().all()]

@router.patch("/{alert_id}/resolve")
async def resolve_alert(alert_id: str, db: AsyncSession = Depends(get_db)):
    """Mark an alert as resolved."""
    await db.execute(text("""
        UPDATE alerts
        SET resolved_at = NOW()
        WHERE alert_id = :alert_id
        AND resolved_at IS NULL
    """), {"alert_id": alert_id})
    await db.commit()
    return {"status": "resolved", "alert_id": alert_id}

@router.get("/stats")
async def get_alert_stats(db: AsyncSession = Depends(get_db)):
    """Alert counts by severity and site — Grafana stat panels."""
    result = await db.execute(text("""
        SELECT
            site_id,
            severity,
            COUNT(*) as count
        FROM alerts
        WHERE triggered_at >= NOW() - INTERVAL '24 hours'
        GROUP BY site_id, severity
        ORDER BY site_id, severity
    """))
    return [dict(r) for r in result.mappings().all()]
