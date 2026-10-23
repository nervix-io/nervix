//! Current Plexus emitter heads and their complete destination bodies.
//!
//! Layer: test harness.
//! - **Owns.** Public entity-head and external-name regression checks.
//! - **Depends on.** The composed emitter grammar and vocabulary.
//! - **Must not know.** Connector drivers or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use super::parse_create_emitter;

#[test]
fn connector_and_entity_head_preserves_external_name() {
    let parsed = parse_create_emitter(
        "CREATE DETACHED KAFKA TOPIC EMITTER outgoing FROM events TO kafka_main TOPIC 'Orders' \
         MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec INHERIT ALL FLUSH \
         IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    .unwrap_or_else(|error| panic!("the complete Plexus emitter must parse: {error:#}"));
    assert_eq!(parsed.name.as_str(), "outgoing");
    let nervix_models::EmitSink::Kafka(nervix_models::KafkaSink::Topic(destination)) =
        parsed.sink.as_ref()
    else {
        panic!("Kafka topic");
    };
    assert_eq!(destination.topic.as_str(), "Orders");
    let canonical = parsed.to_canonical_nspl().expect("canonical emitter");
    assert_eq!(
        parse_create_emitter(&canonical).expect("canonical text parses"),
        parsed
    );
}
