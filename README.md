# Real-Time Mining Equipment Analytics Platform

A production-grade streaming data pipeline that ingests telemetry from mining equipment across Australian mine sites, detects anomalies in real time, and serves live KPI dashboards.

## Architecture

```
Sensors → Kafka → Spark Streaming → PostgreSQL → Grafana
                       ↓
                   Airflow DAGs
                       ↓
                    AWS (ECS, MSK, RDS, S3)
```

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka 7.4 (Confluent) |
| Stream processing | Apache Spark Structured Streaming |
| Batch orchestration | Apache Airflow 2.8 |
| Storage | PostgreSQL 15, AWS S3 |
| Caching | Redis 7 |
| API | FastAPI |
| Dashboards | Grafana |
| Containerisation | Docker, AWS ECS |
| Infrastructure | AWS EC2, MSK, RDS, S3 |

## Mine sites simulated

- **Pilbara WA** — 5 machines (haul trucks + excavators)
- **Hunter Valley NSW** — 3 machines (haul trucks + drill rigs)
- **Bowen Basin QLD** — 4 machines (haul trucks + drill rigs)

## Equipment types

- Haul trucks (CAT 793, Komatsu 930E) — payload, fuel, engine metrics
- Drill rigs (Sandvik, Atlas Copco) — RPM, vibration, depth
- Excavators (Hitachi EX3600) — dig force, cycle time, fuel burn

## Sensors monitored

- Engine temperature (°C)
- Vibration (mm/s)
- Oil pressure (kPa)
- Fuel level & burn rate
- GPS location
- Engine RPM
- Equipment-specific metrics

## Quick start

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/mining-analytics-platform.git
cd mining-analytics-platform

# 2. Configure environment
cp .env.example .env
# Edit .env with your values

# 3. Start all services
docker-compose up -d

# 4. Create Kafka topics + start producer
cd kafka
python create_topics.py
python producer.py
```

## Services

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Airflow | http://localhost:8081 |
| Grafana | http://localhost:3000 |
| FastAPI docs | http://localhost:8000/docs |

## Project structure

```
mining-analytics-platform/
├── kafka/              # Producer, simulator, models
├── spark/              # Streaming + batch jobs
├── airflow/dags/       # Orchestration DAGs
├── postgres/           # Schema + migrations
├── api/                # FastAPI app
├── dashboard/          # Grafana provisioning
├── scripts/            # Utilities
├── tests/              # Unit + integration tests
└── docker-compose.yml
```

## Key features

- Real-time anomaly detection with configurable thresholds
- Fault injection simulation (1% failure rate, gradual recovery)
- Keyed Kafka messages for per-equipment ordering guarantees
- Production Kafka settings (`acks=all`, retries, idempotent producer)
- Sensor drift modelling using Gaussian noise
- Alerting pipeline with WARNING / CRITICAL severity levels
