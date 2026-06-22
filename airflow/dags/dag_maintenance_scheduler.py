# airflow/dags/dag_maintenance_scheduler.py
"""
Runs daily at 6am AEST.
Checks which equipment is approaching or past service hours threshold
and creates maintenance log records + alerts.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import POSTGRES_CONN_ID, SCHEDULE_MAINT
from config import SERVICE_HOURS_WARNING, SERVICE_HOURS_CRITICAL

default_args = {
    "owner":   "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def query_service_hours(**ctx):
    """Find all equipment approaching or past service interval."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    rows = hook.get_records("""
        SELECT
            e.equipment_id,
            e.site_id,
            e.equipment_type,
            e.model,
            e.service_interval_hrs,
            l.hours_since_service,
            s.site_name,
            CASE
                WHEN l.hours_since_service >= e.service_interval_hrs
                    THEN 'OVERDUE'
                WHEN l.hours_since_service >= e.service_interval_hrs * 0.92
                    THEN 'DUE_SOON'
                ELSE 'OK'
            END as service_status
        FROM equipment e
        JOIN sites s USING (site_id)
        LEFT JOIN (
            SELECT DISTINCT ON (equipment_id)
                equipment_id, hours_since_service
            FROM sensor_readings
            ORDER BY equipment_id, recorded_at DESC
        ) l USING (equipment_id)
        WHERE e.active = TRUE
        ORDER BY l.hours_since_service DESC NULLS LAST
    """)

    equipment_list = [
        {
            "equipment_id":       r[0],
            "site_id":            r[1],
            "equipment_type":     r[2],
            "model":              r[3],
            "service_interval":   float(r[4]),
            "hours_since_service": float(r[5] or 0),
            "site_name":          r[6],
            "service_status":     r[7],
        }
        for r in rows
    ]

    due_soon    = [e for e in equipment_list if e["service_status"] in ("DUE_SOON", "OVERDUE")]
    overdue     = [e for e in equipment_list if e["service_status"] == "OVERDUE"]

    print(f"Total equipment: {len(equipment_list)} | Due soon: {len(due_soon)} | Overdue: {len(overdue)}")

    ctx["ti"].xcom_push(key="equipment_list", value=equipment_list)
    ctx["ti"].xcom_push(key="due_count",      value=len(due_soon))
    ctx["ti"].xcom_push(key="overdue_count",  value=len(overdue))

def flag_service_due_equipment(**ctx):
    """Insert alert records for equipment needing service."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    equipment_list = ctx["ti"].xcom_pull(key="equipment_list")

    flagged = [e for e in equipment_list if e["service_status"] in ("DUE_SOON", "OVERDUE")]

    if not flagged:
        print("No equipment requires service flagging today")
        return

    for equip in flagged:
        severity = "CRITICAL" if equip["service_status"] == "OVERDUE" else "WARNING"
        hook.run("""
            INSERT INTO alerts (
                equipment_id, site_id, triggered_at,
                alert_type, severity, metric,
                value, threshold, message
            ) VALUES (
                %(equipment_id)s, %(site_id)s, NOW(),
                'SERVICE_DUE', %(severity)s, 'hours_since_service',
                %(hours)s, %(threshold)s, %(message)s
            )
            ON CONFLICT DO NOTHING
        """, parameters={
            "equipment_id": equip["equipment_id"],
            "site_id":      equip["site_id"],
            "severity":     severity,
            "hours":        equip["hours_since_service"],
            "threshold":    equip["service_interval"],
            "message":      (
                f"{severity}: {equip['equipment_id']} ({equip['model']}) at "
                f"{equip['site_name']} — {equip['hours_since_service']:.0f}h since last service "
                f"(interval: {equip['service_interval']:.0f}h)"
            ),
        })

    print(f"Flagged {len(flagged)} equipment records")

def insert_maintenance_log_records(**ctx):
    """
    Auto-create maintenance log entries for overdue equipment.
    In production this would trigger a work order in the CMMS system.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    equipment_list = ctx["ti"].xcom_pull(key="equipment_list")

    overdue = [e for e in equipment_list if e["service_status"] == "OVERDUE"]

    for equip in overdue:
        hook.run("""
            INSERT INTO maintenance_logs (
                equipment_id, service_date, service_type,
                hours_at_service, next_service_due, notes
            )
            SELECT
                %(equipment_id)s,
                CURRENT_DATE,
                'preventive',
                %(hours)s,
                %(hours)s + %(interval)s,
                'Auto-generated by Airflow maintenance_scheduler DAG'
            WHERE NOT EXISTS (
                SELECT 1 FROM maintenance_logs
                WHERE equipment_id = %(equipment_id)s
                AND service_date = CURRENT_DATE
            )
        """, parameters={
            "equipment_id": equip["equipment_id"],
            "hours":        equip["hours_since_service"],
            "interval":     equip["service_interval"],
        })

    print(f"Created maintenance log records for {len(overdue)} overdue machines")

def send_maintenance_summary(**ctx):
    """Print maintenance summary — wire to Slack/email in production."""
    due_count     = ctx["ti"].xcom_pull(key="due_count")
    overdue_count = ctx["ti"].xcom_pull(key="overdue_count")
    equipment_list = ctx["ti"].xcom_pull(key="equipment_list")

    flagged = [e for e in equipment_list if e["service_status"] in ("DUE_SOON", "OVERDUE")]

    summary_lines = "\n".join([
        f"  [{e['service_status']}] {e['equipment_id']} — "
        f"{e['hours_since_service']:.0f}h / {e['service_interval']:.0f}h — {e['site_name']}"
        for e in flagged
    ])

    print(f"""
    🔧 Daily Maintenance Report — {ctx['ds']}
    ─────────────────────────────────────────
    Equipment due soon:  {due_count}
    Equipment overdue:   {overdue_count}

    Details:
    {summary_lines or '  All equipment within service intervals ✅'}
    """)

with DAG(
    dag_id="maintenance_scheduler",
    default_args=default_args,
    description="Daily maintenance scheduling and alerting for mining equipment",
    schedule_interval=SCHEDULE_MAINT,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "alerts", "daily"],
) as dag:

    query_hours = PythonOperator(
        task_id="query_equipment_service_hours",
        python_callable=query_service_hours,
    )

    flag_due = PythonOperator(
        task_id="flag_service_due_equipment",
        python_callable=flag_service_due_equipment,
    )

    insert_logs = PythonOperator(
        task_id="insert_maintenance_log_records",
        python_callable=insert_maintenance_log_records,
    )

    send_summary = PythonOperator(
        task_id="send_maintenance_summary",
        python_callable=send_maintenance_summary,
    )

    query_hours >> flag_due >> insert_logs >> send_summary
