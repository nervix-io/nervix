//! BYTES-specific processor validation tests.
//!
//! Layer: test harness.
//! - **Owns.** Public-model validation examples for unsupported BYTES contexts.
//! - **Depends on.** Processor decisions and registry test fixtures.
//! - **Must not know.** Runtime payload execution.

use std::fs;

use nervix_models::ParseAsType;

use super::*;
use crate::registry::{
    storage::Registry,
    test_fixtures::{example_graph_models, named, temp_db_path},
};

#[test]
fn lookup_schema_rejects_bytes_in_payload_fields() {
    let domain = named::<DomainName>("binary_domain");
    let schema = CreateSchema {
        name: named("lookup_values"),
        fields: vec![SchemaField {
            name: named("payload"),
            ty: ParseAsType::Bytes,
            optional: false,
            sensitive: false,
        }],
    };
    let lookup = CreateLookup {
        name: named("values_by_id"),
        key_field: named("id"),
        resource: named("lookup_data"),
        resource_version: 1,
        path: "lookup.jsonl".to_string(),
        decode_using_codec: named("lookup_codec"),
    };
    let result = validate_lookup_schema(&domain, &ModelName::from(&lookup.name), &lookup, &schema);
    let Err(error) = result else {
        panic!("lookup payload fields cannot contain BYTES");
    };
    assert!(matches!(
        error.current_context(),
        RegistryError::LookupFieldContainsBytes { field, .. } if field.as_str() == "payload"
    ));
}

#[test]
fn generator_rejects_binary_materialized_source_fields() {
    let (domain, models) = example_graph_models(
        "binary generator source",
        r#"
        CREATE SCHEMA binary_event (payload BYTES);
        CREATE RELAY source_events SCHEMA binary_event UNBRANCHED
          WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE RELAY generated_events SCHEMA binary_event UNBRANCHED;
        CREATE GENERATOR synth USING MATERIALIZED STATE source_events EACH 100ms UNBRANCHED
          TO generated_events SET payload = hex_decode('00')
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).assured("the test database path can hold a registry");
    let Err(error) = registry.apply_batch(&domain, models) else {
        panic!("generator source snapshots cannot contain BYTES");
    };
    assert!(matches!(
        error.current_context(),
        RegistryError::GeneratorSourceFieldContainsBytes { field, .. }
            if field.as_str() == "payload"
    ));
    fs::remove_dir_all(path).assured("the test database path is disposable");
}

#[test]
fn materialized_dependency_rejects_binary_snapshot_fields() {
    let (domain, models) = example_graph_models(
        "binary materialized dependency",
        r#"
        CREATE SCHEMA binary_event (payload BYTES);
        CREATE RELAY source_events SCHEMA binary_event UNBRANCHED
          WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE RELAY projected_events SCHEMA binary_event UNBRANCHED;
        CREATE JUNCTION project_binary FROM source_events UNBRANCHED
          USING MATERIALIZED STATE source_events REQUIRED SKIP
          TO projected_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).assured("the test database path can hold a registry");
    let Err(error) = registry.apply_batch(&domain, models) else {
        panic!("materialized dependencies cannot contain BYTES");
    };
    assert!(matches!(
        error.current_context(),
        RegistryError::MaterializedFieldContainsBytes { field, .. }
            if field.as_str() == "payload"
    ));
    fs::remove_dir_all(path).assured("the test database path is disposable");
}

#[test]
fn window_rejects_binary_aggregate_arguments() {
    let (domain, models) = example_graph_models(
        "binary window argument",
        r#"
        CREATE SCHEMA binary_event (payload BYTES);
        CREATE RELAY source_events SCHEMA binary_event UNBRANCHED;
        CREATE RELAY window_events SCHEMA binary_event UNBRANCHED;
        CREATE WINDOW PROCESSOR first_payload FROM source_events
          WIDTH 10s DURATION STEP 5s DURATION UNBRANCHED
          TO window_events SET payload = FIRST(input.payload)
          ON MESSAGE ERROR LOG;
        "#,
    );
    let path = temp_db_path();
    let registry = Registry::open(&path).assured("the test database path can hold a registry");
    let Err(error) = registry.apply_batch(&domain, models) else {
        panic!("window snapshots cannot retain BYTES aggregate arguments");
    };
    assert!(matches!(
        error.current_context(),
        RegistryError::WindowArgumentContainsBytes { route, .. }
            if route.as_str() == "window_events"
    ));
    fs::remove_dir_all(path).assured("the test database path is disposable");
}
