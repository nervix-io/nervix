# Datalake Geo/Iceberg Example

This example streams source-specific activity from three systems into a
branch-preserving DAG:

- IoT devices publish CBOR activity over MQTT.
- Edge servers publish protobuf activity over NATS.
- Auth servers publish JSON decisions over Kafka.

The graph correlates device connect, edge connect, and auth-authorized events
into connected sessions. Device location reports are stored as standalone
activity. Device and edge disconnects are correlated when both sides arrive and
stored with nullable missing-side fields when only one side arrives. Uncorrelated
or denied auth attempts are sent to a Kafka security topic. Device-to-edge
distance anomalies are sent to Redis Pub/Sub. Connected sessions, device
locations, matched disconnects, one-sided disconnects, security events, and
distance alerts are also written to Iceberg on S3.

Build the Rust WASM guest before executing `datalake.nspl`:

```bash
just wasm-datalake-geo-guest
```

The build target downloads the DB-IP City Lite MMDB archive if it is not already
present. Then run `examples/datalake/datalake.nspl` from the repository root so
resource upload paths resolve correctly.

`docker compose up` runs the RustFS bucket initializer and the datalake Iceberg
REST catalog initializer by default. Nervix emitters load existing Iceberg
tables and append data; they do not create REST catalog namespaces or tables
implicitly.

```bash
docker compose up -d
```

The current Nervix WASM ABI is non-WASI, so the guest cannot open a DB-IP CSV or
MMDB file at runtime. The guest build decompresses the ignored local archive
`geo-wasm-guest/dbip-city-lite-2026-06.mmdb.gz` and embeds the MMDB bytes into
the WASM artifact. The build target stages only the final `.wasm` file under the
ignored `geo-wasm-guest/resource` directory so `UPLOAD RESOURCE geoip_wasm` does
not recursively upload Cargo build artifacts. `reference-data/dbip_city_lite_sample.csv`
is a tiny sample using DB-IP's CSV field order for readers who want to inspect
the equivalent CSV shape. `reference-data/edge_sites.jsonl` is the edge-location
hash map used to compare IoT-reported positions with the selected edge site.
The WASM guest emits `geoip_geohash` using the Rust `geohash` crate and computes
hub distances with the `geo` crate.

This example uses the DB-IP City Lite database. IP geolocation data is provided
by [DB-IP.com](https://db-ip.com).

To generate source events with IP addresses selected from well-known public
ranges that resolve through the embedded DB-IP database:

```bash
uv run generate_datalake_load.py --dry-run --duration 1
```

Remove `--dry-run` to publish CBOR device activity to MQTT, protobuf edge
activity to NATS, and JSON auth activity to Kafka:

```bash
uv run generate_datalake_load.py --duration 60
```

The default duration is `0`, which means the generator runs until Ctrl-C. Normal
publish mode reports progress to stderr; use `--quiet` to suppress it. Use
`--sources device,edge,auth` to select a subset, or `--sources all` for all
three. The generator emits extra device `location` reports by default because
those MQTT records drive the GeoIP WASM enrichment path. Use `--location-burst`
to tune that volume, for example `--location-burst 5` for a heavier GeoIP load.
`--rate` is a target event rate; progress output reports both the configured
target and the measured actual rate.

## Query Iceberg With Local DuckDB

The most useful local workflow is a checked-in DuckDB bootstrap script plus a
small `just` wrapper. The SQL file is inspectable and can be reused directly,
while the wrapper keeps the common local command short:

```bash
just duckdb-datalake
```

This opens an in-memory DuckDB shell, loads the `httpfs` and `iceberg`
extensions, configures the local RustFS S3 endpoint, attaches the Iceberg REST
catalog at `http://127.0.0.1:8181`, creates views for the event-type Iceberg
tables, and prints table status, row counts, and the latest connected sessions.
The local REST catalog fixture uses `AUTHORIZATION_TYPE none`.

You can also run the SQL directly:

```bash
duckdb :memory: -init examples/datalake/duckdb_iceberg.sql
```

Run it after at least one emitter has committed a batch. In this example the
Iceberg commit policy is `COMMIT EACH 1m`, so either wait for that boundary or
lower the commit interval while iterating locally. Tables that are not present
in the REST catalog are shown as `missing_unprovisioned_table` and are exposed
as empty views.

Inside DuckDB, the views are:

```sql
SELECT count(*) FROM datalake_connected_sessions;

SELECT
    tenant_id,
    device_id,
    session_id,
    edge_id,
    principal_id,
    distance_to_edge_km
FROM datalake_connected_sessions
ORDER BY authorized_at DESC
LIMIT 20;

SELECT
    security_reason,
    count(*) AS events,
    avg(risk_score) AS avg_risk_score
FROM datalake_security_events
GROUP BY security_reason
ORDER BY events DESC;

SELECT
    alert_type,
    count(*) AS alerts,
    max(distance_to_edge_km) AS max_distance_to_edge_km
FROM datalake_distance_alerts
GROUP BY alert_type;
```

The bootstrap queries tables through the standard REST catalog namespace:
`datalake_catalog.datalake_demo.<table>`. It does not read Nervix-specific
pointer files.
