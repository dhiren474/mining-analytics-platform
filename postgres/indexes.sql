-- postgres/indexes.sql

-- sensor_readings — most critical, queried constantly by Grafana
CREATE INDEX idx_sensor_readings_equipment_time
    ON sensor_readings (equipment_id, recorded_at DESC);

CREATE INDEX idx_sensor_readings_time
    ON sensor_readings (recorded_at DESC);

CREATE INDEX idx_sensor_readings_status
    ON sensor_readings (status, recorded_at DESC)
    WHERE status IN ('fault', 'maintenance');   -- partial index — only problem rows

-- alerts
CREATE INDEX idx_alerts_equipment_time
    ON alerts (equipment_id, triggered_at DESC);

CREATE INDEX idx_alerts_unresolved
    ON alerts (triggered_at DESC)
    WHERE resolved_at IS NULL;                  -- partial index — only open alerts

CREATE INDEX idx_alerts_severity
    ON alerts (severity, triggered_at DESC);

-- daily_kpi
CREATE INDEX idx_daily_kpi_equipment_date
    ON daily_kpi (equipment_id, kpi_date DESC);

CREATE INDEX idx_daily_kpi_site_date
    ON daily_kpi (site_id, kpi_date DESC);

-- maintenance_logs
CREATE INDEX idx_maintenance_equipment
    ON maintenance_logs (equipment_id, service_date DESC);
