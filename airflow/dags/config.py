# airflow/dags/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Postgres connection ID registered in Airflow UI
POSTGRES_CONN_ID  = "mining_postgres"
SPARK_CONN_ID     = "mining_spark"
SLACK_CONN_ID     = "mining_slack"
AWS_CONN_ID       = "mining_aws"

# AEST schedules (UTC offset +10)
SCHEDULE_HEALTH   = "*/5 * * * *"        # every 5 min
SCHEDULE_KPI      = "0 16 * * *"         # daily 2am AEST = 4pm UTC prev day
SCHEDULE_MAINT    = "0 20 * * *"         # daily 6am AEST = 8pm UTC prev day
SCHEDULE_RETENTION = "0 15 * * 0"        # Sunday 1am AEST = 3pm UTC Saturday

# Thresholds
SERVICE_HOURS_WARNING  = 230
SERVICE_HOURS_CRITICAL = 250
