//! Native consumer attachment contract tests.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** Contract identity and reservation cleanup assertions over the session owner.
//! - **Depends on.** Session consumers and current emitter Models.
//! - **Must not know.** Transport framing or graph execution.

use meticulous::ResultExt as _;
use nervix_models::{
    AckMode, AckWindow, Assignment, AssignmentTarget, EmitterBody, EmitterPublishingMode,
    ErrorPolicies, Expression, Float64Literal, Literal, ParseAsType, ProcessorInputs, RetryPolicy,
    RouteConstruction,
};

use super::*;
use crate::application::test_fixtures::named;

struct EndpointFixture {
    model: CreateEmitter,
    fields: Vec<SchemaField>,
}

impl Default for EndpointFixture {
    fn default() -> Self {
        Self {
            model: CreateEmitter {
                name: named("output"),
                from: ProcessorInputs::single(named("input")),
                body: EmitterBody::Client,
                sink: Box::new(EmitSink::Client {
                    schema: named("output_schema"),
                }),
                batch: None,
                flush_policy: FlushPolicy::Immediate,
                error_policies: ErrorPolicies::handled_by_log(),
                publishing_mode: EmitterPublishingMode::ClientAck {
                    window: AckWindow::Sequential,
                    ack_timeout: "1s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "1s".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                mode: AckMode::Attached,
                construction: RouteConstruction {
                    assignments: vec![Assignment {
                        target: AssignmentTarget::bare(named("value")),
                        value: Expression::Literal(Literal::F64(Float64Literal::new(1.0))),
                    }],
                    ..RouteConstruction::default()
                },
                materialized_state: Vec::new(),
            },
            fields: vec![
                SchemaField {
                    name: named("value"),
                    ty: ParseAsType::F64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("label"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        }
    }
}

#[test]
fn an_interrupted_open_releases_its_session_reservation() {
    let consumers = SessionConsumers::default();
    let first = consumers
        .reserve(CLIENT_CONSUMER_SESSION_BYTES)
        .assured("the empty session can reserve its full capacity");
    assert!(matches!(
        consumers.reserve(1),
        Err(EmitterOpenRefusal::SessionCapacityExhausted)
    ));
    drop(first);
    let replacement = consumers
        .reserve(CLIENT_CONSUMER_SESSION_BYTES)
        .assured("dropping the sole reservation releases the session's full capacity");
    drop(replacement);
    assert_eq!(consumers.state.lock().held_count, 0);
    assert_eq!(consumers.state.lock().held_bytes, 0);
}

#[test]
fn flush_retuning_preserves_the_contract_but_construction_changes_do_not() {
    let consumers = SessionConsumers::default();
    let EndpointFixture { mut model, fields } = EndpointFixture::default();
    let contract = consumers
        .endpoint_contract(model.clone(), &fields)
        .assured("the fixture has finite literals and serializable schema fields");
    model.flush_policy = FlushPolicy::Each {
        interval: "100ms".to_string(),
        max_batch_size: "1MiB".to_string(),
    };
    assert_eq!(
        consumers.endpoint_contract(model.clone(), &fields),
        Ok(contract)
    );
    model.construction.assignments[0].value =
        Expression::Literal(Literal::F64(Float64Literal::new(2.0)));
    let changed = consumers
        .endpoint_contract(model, &fields)
        .assured("the replacement literal is finite and the schema is unchanged");
    assert_ne!(changed, contract);
}

#[test]
fn every_output_field_property_and_field_order_is_pinned() {
    let consumers = SessionConsumers::default();
    let EndpointFixture { model, fields } = EndpointFixture::default();
    let contract = consumers
        .endpoint_contract(model.clone(), &fields)
        .assured("the fixture has finite literals and serializable schema fields");
    for property in 0..5 {
        let mut changed_fields = fields.clone();
        match property {
            0 => changed_fields[0].name = named("amount"),
            1 => changed_fields[0].ty = ParseAsType::F32,
            2 => changed_fields[0].optional = true,
            3 => changed_fields[0].sensitive = true,
            _ => changed_fields.swap(0, 1),
        }
        let changed = consumers
            .endpoint_contract(model.clone(), &changed_fields)
            .assured("changing a typed field preserves its serializability");
        assert_ne!(
            changed, contract,
            "field property {property} must be pinned"
        );
    }
}

#[test]
fn an_unrepresentable_emitter_contract_is_refused() {
    let consumers = SessionConsumers::default();
    let EndpointFixture { mut model, fields } = EndpointFixture::default();
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        model.construction.assignments[0].value =
            Expression::Literal(Literal::F64(Float64Literal::new(value)));
        assert_eq!(
            consumers.endpoint_contract(model.clone(), &fields),
            Err((
                EmitterOpenRefusal::EndpointUnavailable,
                "emitter contract could not be rendered".to_string()
            ))
        );
    }
}
