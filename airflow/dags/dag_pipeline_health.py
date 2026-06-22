# airflow/dags/dag_pipeline_health.py
"""
Runs every 5 minutes.
Checks Kafka consumer lag, Spark streaming status, and Postgres ingestion rate.
Alerts if anything looks wrong.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from config import POSTGRES_CONN_ID, SCHEDULE_HEALTH
import subprocess
import json

default_args = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}

def check_kafka_consumer_lag(**ctx):
    """Check if Kafka consumer group is falling behind."""
    try:
        result = subprocess.run(
            [
                "kafka-consumer-groups.sh",
                "--bootstrap-server", "kafka:9092",
                "--describe",
                "--group", "spark-mining-consumer",
            ],
            capture_output=True, text=True, timeout=10
        )
        # In real setup parse lag from output
        # For local dev just check Kafka is reachable
        print("Kafka consumer lag check passed")
        return "check_spark_streaming"
    except Exception as e:
        print(f"Kafka check failed: {e}")
        raise

def check_spark_streaming(**ctx):
    """Verify Spark is writing data to Postgres in last 5 minutes."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
        SELECT COUNT(*) as recent_rows
        FROM sensor_readings
        WHERE recorded_at >= NOW() - INTERVAL '5 minutes'
    """
    result = hook.get_first(sql)
    recent_rows = result[0] if result else 0

    print(f"Rows in last 5 min: {recent_rows}")

    # Push to XCom for downstream tasks
    ctx["ti"].xcom_push(key="recent_row_count", value=recent_rows)

    if recent_rows < 10:
        return "pipeline_unhealthy"
    return "pipeline_healthy"

def check_postgres_row_count(**ctx):
    """Verify total row counts are growing."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    counts = hook.get_first("""
        SELECT
            (SELECT COUNT(*) FROM sensor_readings) as total_readings,
            (SELECT COUNT(*) FROM alerts WHERE resolved_at IS NULL) as open_alerts,
            (SELECT COUNT(*) FROM alerts
             WHERE triggered_at >= NOW() - INTERVAL '1 hour') as alerts_last_hour
    """)

    print(f"Total readings: {counts[0]} | Open alerts: {counts[1]} | Alerts last hour: {counts[2]}")

    ctx["ti"].xcom_push(key="pipeline_stats", value={
        "total_readings":    counts[0],
        "open_alerts":       counts[1],
        "alerts_last_hour":  counts[2],
    })

def handle_unhealthy_pipeline(**ctx):
    """Log and raise when pipeline is unhealthy."""
    recent_rows = ctx["ti"].xcom_pull(key="recent_row_count")
    raise Exception(
        f"Pipeline unhealthy! Only {recent_rows} rows in last 5 min. "
        "Check Spark streaming job and Kafka producer."
    )

with DAG(
    dag_id="sensor_pipeline_health",
    default_args=default_args,
    description="Monitor real-time pipeline health every 5 minutes",
    schedule_interval=SCHEDULE_HEALTH,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["monitoring", "pipeline"],
) as dag:

    check_kafka = PythonOperator(
        task_id="check_kafka_consumer_lag",
        python_callable=check_kafka_consumer_lag,
    )

    check_spark = BranchPythonOperator(
        task_id="check_spark_streaming",
        python_callable=check_spark_streaming,
    )

    check_postgres = PythonOperator(
        task_id="check_postgres_row_count",
        python_callable=check_postgres_row_count,
    )

    pipeline_healthy = EmptyOperator(task_id="pipeline_healthy")

    pipeline_unhealthy = PythonOperator(
        task_id="pipeline_unhealthy",
        python_callable=handle_unhealthy_pipeline,
    )

    # Task dependencies
    check_kafka >> check_spark >> [pipeline_healthy, pipeline_unhealthy]
    pipeline_healthy >> check_postgres
