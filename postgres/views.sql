-- postgres/views.sql

-- Latest reading per machine — powers the live dashboard
CREATE OR REPLACE VIEW vw_equipment_latest AS
SELECT DISTINCT ON (equipment_id)
    sr.equipment_id,
    e.equipment_type,
    e.model,
    e.site_id,
    sr.recorded_at,
    sr.engine_temp_c,
    sr.engine_rpm,
    sr.oil_pressure_kpa,
    sr.vibration_mm_s,
    sr.fuel_level_pct,
    sr.speed_kmh,
    sr.status,
    sr.fault_code,
    sr.hours_since_service
FROM sensor_readings sr
JOIN equipment e USING (equipment_id)
ORDER BY sr.equipment_id, sr.recorded_at DESC;

-- Open alerts summary — powers the alert panel
CREATE OR REPLACE VIEW vw_open_alerts AS
SELECT
    a.alert_id,
    a.equipment_id,
    a.site_id,
    s.site_name,
    a.triggered_at,
    a.alert_type,
    a.severity,
    a.metric,
    a.value,
    a.threshold,
    a.message,
    EXTRACT(EPOCH FROM (NOW() - a.triggered_at))/3600 AS hours_open
FROM alerts a
JOIN sites s ON a.site_id = s.site_id
WHERE a.resolved_at IS NULL
ORDER BY
    CASE a.severity WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 END,
    a.triggered_at DESC;

-- Fleet health summary per site
CREATE OR REPLACE VIEW vw_site_fleet_health AS
SELECT
    e.site_id,
    s.site_name,
    e.equipment_type,
    COUNT(*)                                               AS total_machines,
    COUNT(*) FILTER (WHERE l.status = 'operating')        AS operating,
    COUNT(*) FILTER (WHERE l.status = 'fault')            AS in_fault,
    COUNT(*) FILTER (WHERE l.status = 'maintenance')      AS in_maintenance,
    ROUND(AVG(l.fuel_level_pct), 1)                       AS avg_fuel_pct,
    ROUND(AVG(l.engine_temp_c), 1)                        AS avg_engine_temp,
    ROUND(AVG(l.vibration_mm_s), 2)                       AS avg_vibration
FROM equipment e
JOIN sites s USING (site_id)
LEFT JOIN vw_equipment_latest l USING (equipment_id)
WHERE e.active = TRUE
GROUP BY e.site_id, s.site_name, e.equipment_type;
