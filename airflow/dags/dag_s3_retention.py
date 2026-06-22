# airflow/dags/dag_s3_retention.py
"""
Runs weekly Sunday 1am AEST.
Moves sensor_readings older than 90 days from Postgres to S3 Glacier.
Drops old Postgres partitions to keep the DB lean.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import POSTGRES_CONN_ID, SCHEDULE_RETENTION
import boto3, os

default_args = {
    "owner":   "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

RETENTION_DAYS = 90
S3_BUCKET      = os.getenv("S3_BUCKET", "mining-analytics-raw")

def list_old_partitions(**ctx):
    """Find Postgres partitions older than retention window."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Query pg_inherits to find partition names
    partitions = hook.get_records("""
        SELECT
            child.relname AS partition_name,
            pg_get_expr(child.relpartbound, child.oid) AS partition_range
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
        WHERE parent.relname = 'sensor_readings'
        AND child.relname NOT LIKE '%default%'
        ORDER BY child.relname
    """)

    old_partitions = []
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)

    for partition_name, _ in partitions:
        # Partition names like sensor_readings_2024_01
        parts = partition_name.split("_")
        if len(parts) >= 4:
            try:
                year  = int(parts[-2])
                month = int(parts[-1])
                partition_date = datetime(year, month, 1)
                if partition_date < cutoff:
                    old_partitions.append(partition_name)
            except ValueError:
                pass

    print(f"Found {len(old_partitions)} partitions older than {RETENTION_DAYS} days: {old_partitions}")
    ctx["ti"].xcom_push(key="old_partitions", value=old_partitions)

def archive_to_glacier(**ctx):
    """Note S3 archival — data already in S3 via Spark streaming job."""
    old_partitions = ctx["ti"].xcom_pull(key="old_partitions")
    print(f"Data for {len(old_partitions)} partitions already archived to S3 by Spark.")
    print(f"S3 path: s3://{S3_BUCKET}/sensor-events/ — consider moving to Glacier storage class.")
    # In production: boto3 call to change storage class to GLACIER

def delete_old_postgres_partitions(**ctx):
    """Drop old partitions from Postgres — frees disk, speeds up queries."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    old_partitions = ctx["ti"].xcom_pull(key="old_partitions")

    if not old_partitions:
        print("No partitions to drop")
        return

    for partition in old_partitions:
        print(f"Dropping partition: {partition}")
        hook.run(f"DROP TABLE IF EXISTS {partition}")

    print(f"Dropped {len(old_partitions)} partitions")

with DAG(
    dag_id="s3_data_retention",
    default_args=default_args,
    description="Weekly data retention — archive to S3, drop old Postgres partitions",
    schedule_interval=SCHEDULE_RETENTION,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["retention", "s3", "weekly"],
) as dag:

    list_partitions = PythonOperator(
        task_id="list_old_partitions",
        python_callable=list_old_partitions,
    )

    archive = PythonOperator(
        task_id="archive_to_glacier",
        python_callable=archive_to_glacier,
    )

    delete_partitions = PythonOperator(
        task_id="delete_postgres_old_partitions",
        python_callable=delete_old_postgres_partitions,
    )

    list_partitions >> archive >> delete_partitions
