//! Payload namespace admission through the production registry validators.
//!
//! Layer: test harness.
//! - **Owns.** Entity-specific refusals, exact required count types and namespace isolation.
//! - **Depends on.** Typed Models, lowering and pure registry validation inputs.
//! - **Must not know.** Connector drivers, runtime execution, storage or scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "payload registry admission checks")
)]

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{BinaryOperator, FieldReference, FieldScope, IngestorEntityKind, Literal};

use super::*;
use crate::registry::test_fixtures::{ingestor_statement, named};

fn payload(field: &str) -> Expression {
    Expression::Field(FieldReference::scoped(FieldScope::Payload, named(field)))
}

fn predicate(field: &str) -> Expression {
    Expression::Binary {
        operator: BinaryOperator::GreaterThan,
        left: Box::new(payload(field)),
        right: Box::new(Expression::Cast {
            expression: Box::new(Expression::Literal(Literal::I64(0))),
            target: ParseAsType::U64,
        }),
    }
}

fn schema(name: &str, ty: ParseAsType) -> CreateSchema {
    CreateSchema {
        labels: Default::default(),
        name: named(name),
        fields: vec![SchemaField {
            name: named("bytes"),
            ty,
            optional: false,
            sensitive: false,
        }],
    }
}

#[test]
fn ingest_payload_rows_are_available_only_to_client_producers_in_every_clause() {
    let bytes = vec![7; 2048];
    let domain = named("payload_domain");
    let identifier = named("intake");
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(predicate("rows")),
            ..RouteConstruction::default()
        },
        SemanticScopePolicy::read_only("input"),
    )
    .assured("a typed payload comparison lowers without a schema");
    for kind in IngestorEntityKind::all() {
        let ingestor = Arbitrary::new(&bytes, Domain::Plexus).create_ingestor_from(kind);
        for site in [
            IngestorPayloadReadSite::FilterWhere,
            IngestorPayloadReadSite::Route(named("events")),
        ] {
            let result = refuse_undeclared_payload_reads(
                &domain,
                &identifier,
                &ingestor.input,
                site,
                &parsed.inner,
            );
            if matches!(kind, IngestorEntityKind::Client(_)) {
                result.assured("a client producer declares its original row count");
            } else {
                let error = match result {
                    Err(error) => error,
                    Ok(()) => panic!("{kind} admitted client-only payload.rows"),
                };
                assert!(
                    matches!(error.current_context(), RegistryError::UndeclaredIngestorPayload { field, kind: actual, .. }
                    if field.as_str() == "rows" && *actual == kind)
                );
                let text = error.to_string();
                assert!(
                    text.contains(kind.phrase())
                        && text.contains("payload.rows")
                        && text.contains("payload.bytes"),
                    "{text}"
                );
            }
        }
    }
}

#[test]
fn ingest_payload_bytes_bind_as_required_unsigned_counts_in_filters_and_routes() {
    let domain = named("payload_domain");
    let identifier = named("intake");
    let ingestor = ingestor_statement("intake", "events", "codec", "broker", &[]);
    let input = schema("input_schema", ParseAsType::I64);
    let output = schema("measured", ParseAsType::U64);
    let models = ModelIndex::default();
    let mut route = ingestor.output_routes.routes[0].clone();
    route.construction = RouteConstruction {
        assignments: vec![Assignment {
            target: AssignmentTarget::bare(named("bytes")),
            value: payload("bytes"),
        }],
        where_clause: Some(predicate("bytes")),
        ..RouteConstruction::default()
    };
    let checked = effective_ingestor_output_filter_map_schema(
        &domain,
        &identifier,
        &models,
        &ingestor,
        &input,
        &route,
        &output,
    )
    .assured("a required U64 destination accepts payload.bytes without a nullable conversion");
    assert_eq!(checked.fields[0].ty, ParseAsType::U64);
    assert!(!checked.fields[0].optional);
    validate_ingestor_filter_where_for_internal_schemas(
        &domain,
        &identifier,
        &models,
        &[(&named("source"), &input)],
        None,
        Some(&predicate("bytes")),
        &ingestor.input,
    )
    .assured("the arrival filter reads the same exact unsigned byte count");
    let signed_output = schema("signed", ParseAsType::I64);
    assert!(
        effective_ingestor_output_filter_map_schema(
            &domain,
            &identifier,
            &models,
            &ingestor,
            &input,
            &route,
            &signed_output
        )
        .is_err()
    );
}

#[test]
fn ingest_payload_scope_does_not_leak_into_processor_filters() {
    let domain = named("payload_domain");
    let identifier = named("processor");
    let input = schema("input_schema", ParseAsType::I64);
    let models = ModelIndex::default();
    let result = super::super::processor::validate_filter_where_for_internal_schemas(
        &domain,
        &identifier,
        &models,
        &[(&named("source"), &input)],
        None,
        Some(&predicate("bytes")),
    );
    assert!(
        result.is_err(),
        "a processor has no transport payload declaration"
    );
}
