-- postgres/init.sql

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";      -- UUID generation
CREATE EXTENSION IF NOT EXISTS "pg_trgm";        -- fuzzy text search
CREATE EXTENSION IF NOT EXISTS "btree_gist";     -- GiST indexes for time ranges

-- ── Enums ─────────────────────────────────────────────────────────────────────
CREATE TYPE equipment_type AS ENUM (
    'haul_truck', 'drill_rig', 'excavator'
);

CREATE TYPE equipment_status AS ENUM (
    'operating', 'idle', 'maintenance', 'fault'
);

CREATE TYPE alert_severity AS ENUM (
    'WARNING', 'CRITICAL'
);

CREATE TYPE service_type AS ENUM (
    'routine', 'corrective', 'preventive', 'emergency'
);

-- ── sites ─────────────────────────────────────────────────────────────────────
CREATE TABLE sites (
    site_id         VARCHAR(30)  PRIMARY KEY,
    site_name       VARCHAR(100) NOT NULL,
    state           VARCHAR(3)   NOT NULL,    -- WA, NSW, QLD
    latitude        NUMERIC(9,6) NOT NULL,
    longitude       NUMERIC(9,6) NOT NULL,
    timezone        VARCHAR(50)  NOT NULL DEFAULT 'Australia/Perth',
    active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO sites VALUES
    ('PILBARA-01',       'Pilbara Iron Ore Mine',       'WA',  -22.3,  118.5, 'Australia/Perth',    TRUE, NOW()),
    ('HUNTER-VALLEY-01', 'Hunter Valley Coal Mine',     'NSW', -32.8,  151.3, 'Australia/Sydney',   TRUE, NOW()),
    ('BOWEN-BASIN-01',   'Bowen Basin Thermal Coal',    'QLD', -22.5,  148.0, 'Australia/Brisbane', TRUE, NOW());

-- ── equipment ─────────────────────────────────────────────────────────────────
CREATE TABLE equipment (
    equipment_id         VARCHAR(20)    PRIMARY KEY,
    site_id              VARCHAR(30)    NOT NULL REFERENCES sites(site_id),
    equipment_type       equipment_type NOT NULL,
    model                VARCHAR(100),
    manufacturer         VARCHAR(100),
    serial_number        VARCHAR(50)    UNIQUE,
    commissioned_date    DATE,
    service_interval_hrs NUMERIC(8,2)   NOT NULL DEFAULT 250,
    active               BOOLEAN        NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

INSERT INTO equipment (equipment_id, site_id, equipment_type, model, manufacturer, service_interval_hrs) VALUES
    ('HT-PIL-001', 'PILBARA-01',       'haul_truck', 'CAT 793F',     'Caterpillar', 250),
    ('HT-PIL-002', 'PILBARA-01',       'haul_truck', 'Komatsu 930E', 'Komatsu',     250),
    ('HT-PIL-003', 'PILBARA-01',       'haul_truck', 'CAT 793F',     'Caterpillar', 250),
    ('EX-PIL-001', 'PILBARA-01',       'excavator',  'EX3600-7',     'Hitachi',     500),
    ('EX-PIL-002', 'PILBARA-01',       'excavator',  'PC8000',       'Komatsu',     500),
    ('HT-HUN-001', 'HUNTER-VALLEY-01', 'haul_truck', 'CAT 789D',     'Caterpillar', 250),
    ('HT-HUN-002', 'HUNTER-VALLEY-01', 'haul_truck', 'CAT 789D',     'Caterpillar', 250),
    ('DR-HUN-001', 'HUNTER-VALLEY-01', 'drill_rig',  'D65',          'Sandvik',     500),
    ('HT-BOW-001', 'BOWEN-BASIN-01',   'haul_truck', 'Komatsu 830E', 'Komatsu',     250),
    ('HT-BOW-002', 'BOWEN-BASIN-01',   'haul_truck', 'Komatsu 830E', 'Komatsu',     250),
    ('DR-BOW-001', 'BOWEN-BASIN-01',   'drill_rig',  'PV271',        'Atlas Copco', 500),
    ('DR-BOW-002', 'BOWEN-BASIN-01',   'drill_rig',  'PV271',        'Atlas Copco', 500);

-- ── sensor_readings (partitioned by month) ────────────────────────────────────
-- Partitioning is KEY for time-series performance — mention this in interviews
CREATE TABLE sensor_readings (
    reading_id          UUID         NOT NULL DEFAULT uuid_generate_v4(),
    equipment_id        VARCHAR(20)  NOT NULL,
    recorded_at         TIMESTAMPTZ  NOT NULL,
    engine_temp_c       NUMERIC(6,2),
    engine_rpm          NUMERIC(8,1),
    oil_pressure_kpa    NUMERIC(8,1),
    vibration_mm_s      NUMERIC(8,3),
    fuel_level_pct      NUMERIC(5,2),
    fuel_burn_rate      NUMERIC(8,2),
    payload_tonnes      NUMERIC(8,1),  -- haul trucks only
    drill_rpm           NUMERIC(8,1),  -- drill rigs only
    dig_force_kn        NUMERIC(8,1),  -- excavators only
    speed_kmh           NUMERIC(6,1),
    latitude            NUMERIC(9,6),
    longitude           NUMERIC(9,6),
    status              equipment_status,
    hours_since_service NUMERIC(10,2),
    fault_code          VARCHAR(20),
    PRIMARY KEY (reading_id, recorded_at)
) PARTITION BY RANGE (recorded_at);

-- Create monthly partitions (2024 + 2025)
CREATE TABLE sensor_readings_2024_01 PARTITION OF sensor_readings
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE sensor_readings_2024_12 PARTITION OF sensor_readings
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');
CREATE TABLE sensor_readings_2025_01 PARTITION OF sensor_readings
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE sensor_readings_2025_06 PARTITION OF sensor_readings
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE sensor_readings_2025_07 PARTITION OF sensor_readings
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

-- Default partition catches anything outside defined ranges
CREATE TABLE sensor_readings_default PARTITION OF sensor_readings DEFAULT;

-- ── alerts ────────────────────────────────────────────────────────────────────
CREATE TABLE alerts (
    alert_id        UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
    equipment_id    VARCHAR(20)   NOT NULL REFERENCES equipment(equipment_id),
    site_id         VARCHAR(30)   NOT NULL REFERENCES sites(site_id),
    triggered_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    alert_type      VARCHAR(50)   NOT NULL,
    severity        alert_severity NOT NULL,
    metric          VARCHAR(50)   NOT NULL,
    value           NUMERIC(12,3) NOT NULL,
    threshold       NUMERIC(12,3) NOT NULL,
    message         TEXT          NOT NULL,
    resolved_at     TIMESTAMPTZ,
    acknowledged    BOOLEAN       NOT NULL DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    notes           TEXT
);

-- ── maintenance_logs ──────────────────────────────────────────────────────────
CREATE TABLE maintenance_logs (
    log_id              UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    equipment_id        VARCHAR(20)  NOT NULL REFERENCES equipment(equipment_id),
    service_date        DATE         NOT NULL,
    service_type        service_type NOT NULL DEFAULT 'routine',
    technician          VARCHAR(100),
    cost_aud            NUMERIC(10,2),
    hours_at_service    NUMERIC(10,2),
    next_service_due    NUMERIC(10,2),   -- hours
    parts_replaced      TEXT[],          -- array of part names
    notes               TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── daily_kpi (pre-aggregated by Airflow DAG) ─────────────────────────────────
CREATE TABLE daily_kpi (
    kpi_id              UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    equipment_id        VARCHAR(20)  NOT NULL REFERENCES equipment(equipment_id),
    site_id             VARCHAR(30)  NOT NULL REFERENCES sites(site_id),
    kpi_date            DATE         NOT NULL,
    avg_engine_temp_c   NUMERIC(6,2),
    max_engine_temp_c   NUMERIC(6,2),
    avg_vibration_mm_s  NUMERIC(8,3),
    max_vibration_mm_s  NUMERIC(8,3),
    total_fuel_litres   NUMERIC(10,2),
    operating_hours     NUMERIC(6,2),
    idle_hours          NUMERIC(6,2),
    fault_hours         NUMERIC(6,2),
    uptime_pct          NUMERIC(5,2),
    alert_count         INTEGER      DEFAULT 0,
    critical_alert_count INTEGER     DEFAULT 0,
    total_payload_tonnes NUMERIC(12,2),  -- haul trucks
    avg_drill_rpm        NUMERIC(8,1),   -- drill rigs
    UNIQUE (equipment_id, kpi_date)
);
