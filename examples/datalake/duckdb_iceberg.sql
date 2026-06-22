-- DuckDB bootstrap for the datalake example's local RustFS Iceberg tables.
--
-- Run from the repository root with:
--   duckdb :memory: -init examples/datalake/duckdb_iceberg.sql
--
-- Or use the wrapper:
--   just duckdb-datalake
--
-- This expects `just deps` and a datalake run that has committed at least one
-- batch. The script attaches the local Iceberg REST catalog and queries tables
-- through the standard catalog namespace. Tables that have not been explicitly
-- provisioned are exposed as empty typed views so the bootstrap still opens.

INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;

CREATE OR REPLACE SECRET datalake_rustfs (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'rustfsadmin',
    SECRET 'rustfsadmin',
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:9900',
    URL_STYLE 'path',
    USE_SSL false
);

ATTACH 's3://nervix-iceberg/warehouse' AS datalake_catalog (
    TYPE iceberg,
    ENDPOINT 'http://127.0.0.1:8181',
    AUTHORIZATION_TYPE none
);

CREATE OR REPLACE TEMP TABLE datalake_expected_tables(table_name VARCHAR);

INSERT INTO datalake_expected_tables VALUES
    ('datalake_connected_sessions'),
    ('datalake_device_locations'),
    ('datalake_disconnect_matched_events'),
    ('datalake_disconnect_device_only_events'),
    ('datalake_disconnect_edge_only_events'),
    ('datalake_security_events'),
    ('datalake_distance_alerts');

CREATE OR REPLACE TEMP TABLE datalake_available_tables AS
SELECT table_name
FROM duckdb_tables()
WHERE database_name = 'datalake_catalog'
  AND schema_name = 'datalake_demo';

CREATE OR REPLACE TEMP VIEW datalake_table_status AS
SELECT
    expected.table_name,
    available.table_name IS NOT NULL AS catalog_table_exists,
    CASE
        WHEN available.table_name IS NOT NULL THEN 'present'
        ELSE 'missing_unprovisioned_table'
    END AS catalog_status
FROM datalake_expected_tables AS expected
LEFT JOIN datalake_available_tables AS available USING (table_name);

SET VARIABLE datalake_connected_sessions_query = (
    SELECT CASE
        WHEN catalog_table_exists THEN
            'SELECT * FROM datalake_catalog.datalake_demo.datalake_connected_sessions'
        ELSE
            'SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS tenant_id, NULL::VARCHAR AS device_id, NULL::VARCHAR AS session_id, NULL::VARCHAR AS edge_id, NULL::VARCHAR AS principal_id, NULL::VARCHAR AS device_event_id, NULL::VARCHAR AS edge_event_id, NULL::VARCHAR AS auth_event_id, NULL::TIMESTAMP AS device_connected_at, NULL::TIMESTAMP AS edge_connected_at, NULL::TIMESTAMP AS authorized_at, NULL::VARCHAR AS source_ip, NULL::DOUBLE AS device_lat, NULL::DOUBLE AS device_lon, NULL::DOUBLE AS battery_pct, NULL::VARCHAR AS firmware, NULL::VARCHAR AS edge_name, NULL::VARCHAR AS protocol, NULL::VARCHAR AS edge_region, NULL::VARCHAR AS edge_site_tier, NULL::DOUBLE AS edge_lat, NULL::DOUBLE AS edge_lon, NULL::DOUBLE AS distance_to_edge_km, NULL::DOUBLE AS risk_score WHERE false'
    END
    FROM datalake_table_status
    WHERE table_name = 'datalake_connected_sessions'
);

CREATE OR REPLACE VIEW datalake_connected_sessions AS
SELECT * FROM query(getvariable('datalake_connected_sessions_query'));

SET VARIABLE datalake_device_locations_query = (
    SELECT CASE
        WHEN catalog_table_exists THEN
            'SELECT * FROM datalake_catalog.datalake_demo.datalake_device_locations'
        ELSE
            'SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS tenant_id, NULL::VARCHAR AS device_id, NULL::VARCHAR AS session_id, NULL::VARCHAR AS edge_id, NULL::VARCHAR AS source_ip, NULL::DOUBLE AS device_lat, NULL::DOUBLE AS device_lon, NULL::DOUBLE AS battery_pct, NULL::VARCHAR AS firmware, NULL::TIMESTAMP AS ts, NULL::BIGINT AS seq, NULL::VARCHAR AS geoip_database, NULL::VARCHAR AS geoip_continent, NULL::VARCHAR AS geoip_country, NULL::VARCHAR AS geoip_region, NULL::VARCHAR AS geoip_city, NULL::DOUBLE AS geoip_lat, NULL::DOUBLE AS geoip_lon, NULL::VARCHAR AS geoip_geohash, NULL::VARCHAR AS nearest_hub, NULL::DOUBLE AS distance_to_hub_km, NULL::VARCHAR AS edge_name, NULL::VARCHAR AS edge_region, NULL::VARCHAR AS edge_site_tier, NULL::DOUBLE AS edge_lat, NULL::DOUBLE AS edge_lon, NULL::DOUBLE AS distance_to_edge_km WHERE false'
    END
    FROM datalake_table_status
    WHERE table_name = 'datalake_device_locations'
);

CREATE OR REPLACE VIEW datalake_device_locations AS
SELECT * FROM query(getvariable('datalake_device_locations_query'));

SET VARIABLE datalake_disconnect_matched_events_query = (
    SELECT CASE
        WHEN catalog_table_exists THEN
            'SELECT * FROM datalake_catalog.datalake_demo.datalake_disconnect_matched_events'
        ELSE
            'SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS tenant_id, NULL::VARCHAR AS device_id, NULL::VARCHAR AS session_id, NULL::VARCHAR AS edge_id, NULL::VARCHAR AS disconnect_kind, NULL::VARCHAR AS device_event_id, NULL::VARCHAR AS edge_event_id, NULL::TIMESTAMP AS device_disconnected_at, NULL::TIMESTAMP AS edge_disconnected_at, NULL::VARCHAR AS source_ip, NULL::DOUBLE AS device_lat, NULL::DOUBLE AS device_lon, NULL::DOUBLE AS battery_pct, NULL::VARCHAR AS firmware, NULL::VARCHAR AS edge_name, NULL::VARCHAR AS protocol, NULL::VARCHAR AS edge_region, NULL::VARCHAR AS edge_site_tier, NULL::DOUBLE AS edge_lat, NULL::DOUBLE AS edge_lon WHERE false'
    END
    FROM datalake_table_status
    WHERE table_name = 'datalake_disconnect_matched_events'
);

CREATE OR REPLACE VIEW datalake_disconnect_matched_events AS
SELECT * FROM query(getvariable('datalake_disconnect_matched_events_query'));

SET VARIABLE datalake_disconnect_device_only_events_query = (
    SELECT CASE
        WHEN catalog_table_exists THEN
            'SELECT * FROM datalake_catalog.datalake_demo.datalake_disconnect_device_only_events'
        ELSE
            'SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS tenant_id, NULL::VARCHAR AS device_id, NULL::VARCHAR AS session_id, NULL::VARCHAR AS edge_id, NULL::VARCHAR AS disconnect_kind, NULL::VARCHAR AS device_event_id, NULL::VARCHAR AS edge_event_id, NULL::TIMESTAMP AS device_disconnected_at, NULL::TIMESTAMP AS edge_disconnected_at, NULL::VARCHAR AS source_ip, NULL::DOUBLE AS device_lat, NULL::DOUBLE AS device_lon, NULL::DOUBLE AS battery_pct, NULL::VARCHAR AS firmware, NULL::VARCHAR AS edge_name, NULL::VARCHAR AS protocol, NULL::VARCHAR AS edge_region, NULL::VARCHAR AS edge_site_tier, NULL::DOUBLE AS edge_lat, NULL::DOUBLE AS edge_lon WHERE false'
    END
    FROM datalake_table_status
    WHERE table_name = 'datalake_disconnect_device_only_events'
);

CREATE OR REPLACE VIEW datalake_disconnect_device_only_events AS
SELECT * FROM query(getvariable('datalake_disconnect_device_only_events_query'));

SET VARIABLE datalake_disconnect_edge_only_events_query = (
    SELECT CASE
        WHEN catalog_table_exists THEN
            'SELECT * FROM datalake_catalog.datalake_demo.datalake_disconnect_edge_only_events'
        ELSE
            'SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS tenant_id, NULL::VARCHAR AS device_id, NULL::VARCHAR AS session_id, NULL::VARCHAR AS edge_id, NULL::VARCHAR AS disconnect_kind, NULL::VARCHAR AS device_event_id, NULL::VARCHAR AS edge_event_id, NULL::TIMESTAMP AS device_disconnected_at, NULL::TIMESTAMP AS edge_disconnected_at, NULL::VARCHAR AS source_ip, NULL::DOUBLE AS device_lat, NULL::DOUBLE AS device_lon, NULL::DOUBLE AS battery_pct, NULL::VARCHAR AS firmware, NULL::VARCHAR AS edge_name, NULL::VARCHAR AS protocol, NULL::VARCHAR AS edge_region, NULL::VARCHAR AS edge_site_tier, NULL::DOUBLE AS edge_lat, NULL::DOUBLE AS edge_lon WHERE false'
    END
    FROM datalake_table_status
    WHERE table_name = 'datalake_disconnect_edge_only_events'
);

CREATE OR REPLACE VIEW datalake_disconnect_edge_only_events AS
SELECT * FROM query(getvariable('datalake_disconnect_edge_only_events_query'));

SET VARIABLE datalake_security_events_query = (
    SELECT CASE
        WHEN catalog_table_exists THEN
            'SELECT * FROM datalake_catalog.datalake_demo.datalake_security_events'
        ELSE
            'SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS tenant_id, NULL::VARCHAR AS device_id, NULL::VARCHAR AS session_id, NULL::VARCHAR AS edge_id, NULL::VARCHAR AS principal_id, NULL::VARCHAR AS security_reason, NULL::VARCHAR AS auth_result, NULL::DOUBLE AS risk_score, NULL::TIMESTAMP AS observed_at, NULL::VARCHAR AS source_event_id WHERE false'
    END
    FROM datalake_table_status
    WHERE table_name = 'datalake_security_events'
);

CREATE OR REPLACE VIEW datalake_security_events AS
SELECT * FROM query(getvariable('datalake_security_events_query'));

SET VARIABLE datalake_distance_alerts_query = (
    SELECT CASE
        WHEN catalog_table_exists THEN
            'SELECT * FROM datalake_catalog.datalake_demo.datalake_distance_alerts'
        ELSE
            'SELECT NULL::VARCHAR AS alert_id, NULL::VARCHAR AS alert_type, NULL::VARCHAR AS tenant_id, NULL::VARCHAR AS device_id, NULL::VARCHAR AS session_id, NULL::VARCHAR AS edge_id, NULL::VARCHAR AS edge_label, NULL::VARCHAR AS source_event_id, NULL::TIMESTAMP AS observed_at, NULL::DOUBLE AS distance_to_edge_km, NULL::DOUBLE AS threshold_km, NULL::DOUBLE AS device_lat, NULL::DOUBLE AS device_lon, NULL::DOUBLE AS edge_lat, NULL::DOUBLE AS edge_lon WHERE false'
    END
    FROM datalake_table_status
    WHERE table_name = 'datalake_distance_alerts'
);

CREATE OR REPLACE VIEW datalake_distance_alerts AS
SELECT * FROM query(getvariable('datalake_distance_alerts_query'));

SELECT table_name, catalog_status
FROM datalake_table_status
ORDER BY table_name;

WITH row_counts AS (
    SELECT 'datalake_connected_sessions' AS table_name, count(*) AS row_count
    FROM datalake_connected_sessions
    UNION ALL
    SELECT 'datalake_device_locations' AS table_name, count(*) AS row_count
    FROM datalake_device_locations
    UNION ALL
    SELECT 'datalake_disconnect_matched_events' AS table_name, count(*) AS row_count
    FROM datalake_disconnect_matched_events
    UNION ALL
    SELECT 'datalake_disconnect_device_only_events' AS table_name, count(*) AS row_count
    FROM datalake_disconnect_device_only_events
    UNION ALL
    SELECT 'datalake_disconnect_edge_only_events' AS table_name, count(*) AS row_count
    FROM datalake_disconnect_edge_only_events
    UNION ALL
    SELECT 'datalake_security_events' AS table_name, count(*) AS row_count
    FROM datalake_security_events
    UNION ALL
    SELECT 'datalake_distance_alerts' AS table_name, count(*) AS row_count
    FROM datalake_distance_alerts
)
SELECT row_counts.table_name, datalake_table_status.catalog_status, row_counts.row_count
FROM row_counts
JOIN datalake_table_status USING (table_name)
ORDER BY row_counts.table_name;

SELECT
    tenant_id,
    device_id,
    session_id,
    edge_id,
    principal_id,
    distance_to_edge_km,
    authorized_at
FROM datalake_connected_sessions
ORDER BY authorized_at DESC
LIMIT 10;
