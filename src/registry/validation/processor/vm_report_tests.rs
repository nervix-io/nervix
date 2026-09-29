//! Processor validation tests for retained VM compile causes.
//!
//! Layer: test harness.
//! - **Owns.** Registry error context assertions at VM compilation boundaries.
//! - **Depends on.** Processor decisions, public Models, and test fixtures.
//! - **Must not know.** Runtime payload execution.

use nervix_models::{MessageErrorPolicy, OutputBranch, ParseAsType};

use super::*;
use crate::registry::test_fixtures::named;

fn output_with_construction(raw: &str) -> ProcessorOutput {
    ProcessorOutput {
        relay: named("mapped"),
        construction: nervix_nspl::parse_route_construction(raw)
            .expect("the test route is valid NSPL"),
        flush_policy: None,
        message_error_policy: MessageErrorPolicy::Log,
        branch: Some(OutputBranch::Unbranched),
    }
}

#[test]
fn wasm_output_type_failure_keeps_the_vm_cause() {
    let domain: DomainName = named("default");
    let identifier: ModelName = named("project_values");
    let source: RelayName = named("source");
    let output = output_with_construction("SET value = \"text\"");
    let schema = CreateSchema {
        name: named("mapped_schema"),
        fields: vec![nervix_models::SchemaField {
            name: named("value"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    };
    let error = effective_wasm_output_filter_map_schema(
        &domain,
        &identifier,
        &ModelIndex::new(),
        &[(&source, &schema)],
        &output,
        &schema,
        None,
    )
    .expect_err("a WASM route cannot assign STRING to an I64 field");
    assert!(matches!(
        error.current_context(),
        RegistryError::InvalidModel { domain, identifier, reason }
            if domain == "default"
                && identifier == "project_values"
                && reason.contains("FILTER-MAP compile failed")
    ));
    assert!(error.contains::<nervix_vm::CompileError>());
}

#[test]
fn window_route_where_type_failure_keeps_the_vm_cause() {
    let domain: DomainName = named("default");
    let identifier: ModelName = named("window_values");
    let output = output_with_construction("WHERE output.value");
    let schema = CreateSchema {
        name: named("mapped_schema"),
        fields: vec![nervix_models::SchemaField {
            name: named("value"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    };
    let error = validate_window_route_where(
        &domain,
        &identifier,
        &ModelIndex::new(),
        &output,
        &schema,
        None,
    )
    .expect_err("a window route WHERE must be boolean");
    assert!(matches!(
        error.current_context(),
        RegistryError::InvalidModel { domain, identifier, reason }
            if domain == "default"
                && identifier == "window_values"
                && reason.contains("window output 'mapped' WHERE compile failed")
    ));
    assert!(error.contains::<nervix_vm::CompileError>());
}
