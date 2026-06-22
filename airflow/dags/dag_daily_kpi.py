# airflow/dags/dag_daily_kpi.py
"""
Runs daily at 2am AEST.
Triggers Spark batch job to aggregate yesterday's sensor data into daily_kpi table.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import POSTGRES_CONN_ID, SPARK_CONN_ID, SCHEDULE_KPI

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=1),
}

def validate_source_data(**ctx):
    """
    Check yesterday has enough data before running expensive Spark job.
    Fails fast if data is missing — saves compute cost.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    execution_date = ctx["ds"]   # Airflow passes YYYY-MM-DD

    result = hook.get_first(f"""
        SELECT COUNT(*), COUNT(DISTINCT equipment_id)
        FROM sensor_readings
        WHERE DATE(recorded_at) = '{execution_date}'::date - INTERVAL '1 day'
    """)

    row_count, equipment_count = result
    print(f"Source data for {execution_date}: {row_count} rows, {equipment_count} equipment")

    if row_count < 100:
        raise ValueError(
            f"Insufficient data for {execution_date}: only {row_count} rows. "
            "Minimum 100 required. Check Kafka producer."
        )

    ctx["ti"].xcom_push(key="source_row_count", value=row_count)
    ctx["ti"].xcom_push(key="equipment_count",  value=equipment_count)

def validate_kpi_output(**ctx):
    """Verify Spark wrote KPI rows correctly."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    execution_date = ctx["ds"]

    result = hook.get_first(f"""
        SELECT COUNT(*), AVG(uptime_pct), AVG(alert_count)
        FROM daily_kpi
        WHERE kpi_date = '{execution_date}'::date - INTERVAL '1 day'
    """)

    kpi_count, avg_uptime, avg_alerts = result
    print(f"KPI output: {kpi_count} rows | avg uptime: {avg_uptime:.1f}% | avg alerts: {avg_alerts:.1f}")

    if kpi_count == 0:
        raise ValueError("Spark KPI job produced no output rows!")

    # Store for Slack notification
    ctx["ti"].xcom_push(key="kpi_summary", value={
        "kpi_rows":   kpi_count,
        "avg_uptime": round(float(avg_uptime or 0), 1),
        "avg_alerts": round(float(avg_alerts or 0), 1),
    })

def notify_success(**ctx):
    """Log success summary — extend with Slack/email in production."""
    summary = ctx["ti"].xcom_pull(key="kpi_summary")
    source_rows = ctx["ti"].xcom_pull(key="source_row_count")
    execution_date = ctx["ds"]

    print(f"""
    ✅ Daily KPI Aggregation Complete
    ─────────────────────────────────
    Date:          {execution_date}
    Source rows:   {source_rows:,}
    KPI rows:      {summary['kpi_rows']}
    Avg uptime:    {summary['avg_uptime']}%
    Avg alerts:    {summary['avg_alerts']}
    """)

with DAG(
    dag_id="daily_kpi_aggregation",
    default_args=default_args,
    description="Aggregate daily KPIs from sensor readings via Spark batch",
    schedule_interval=SCHEDULE_KPI,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kpi", "spark", "daily"],
) as dag:

    validate_source = PythonOperator(
        task_id="validate_source_data",
        python_callable=validate_source_data,
    )

    run_spark_kpi = SparkSubmitOperator(
        task_id="run_spark_batch_kpi",
        conn_id=SPARK_CONN_ID,
        application="/opt/spark-apps/batch_kpi_job.py",
        name="mining-daily-kpi-{{ ds }}",
        packages="org.postgresql:postgresql:42.7.1",
        conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.executor.memory":        "1g",
        },
        env_vars={
            "POSTGRES_HOST":     "postgres",
            "POSTGRES_PORT":     "5432",
            "POSTGRES_DB":       "{{ var.value.postgres_db }}",
            "POSTGRES_USER":     "{{ var.value.postgres_user }}",
            "POSTGRES_PASSWORD": "{{ var.value.postgres_password }}",
        },
        execution_timeout=timedelta(minutes=45),
    )

    validate_output = PythonOperator(
        task_id="validate_kpi_output",
        python_callable=validate_kpi_output,
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    validate_source >> run_spark_kpi >> validate_output >> notify
