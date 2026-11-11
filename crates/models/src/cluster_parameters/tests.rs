//! Complete current-value round trips and parameter boundary refusals.

use std::time::Duration;

use meticulous::OptionExt as _;
use strum::IntoEnumIterator as _;

use super::*;

fn parameters(input: &[u8; 384]) -> ClusterParameterValues {
    let generated = nervix_arbitrary::Arbitrary::new(input, nervix_arbitrary::Domain::Vocabulary)
        .cluster_parameter_values();
    ClusterParameterValues::resolve(ClusterParameterSeedValues {
        licensing_request_timeout: Some(generated.licensing_request_timeout()),
        licensing_retry_backoff_max: Some(generated.licensing_retry_backoff_max()),
        licensing_stream_idle_timeout: Some(generated.licensing_stream_idle_timeout()),
        payload_unfold_limit: Some(generated.payload_unfold_limit()),
        password_iterations: Some(generated.password_iterations()),
        node_unavailability_timeout: Some(generated.node_unavailability_timeout()),
        remote_ack_alive_interval: Some(generated.remote_ack_alive_interval()),
        remote_ack_dispatch_timeout: Some(generated.remote_ack_dispatch_timeout()),
        remote_ack_silence_timeout: Some(generated.remote_ack_silence_timeout()),
        remote_ack_sweep_interval: Some(generated.remote_ack_sweep_interval()),
        remote_relay_total_timeout: Some(generated.remote_relay_total_timeout()),
        remote_relay_instantiation_wait: Some(generated.remote_relay_instantiation_wait()),
        domain_drain_timeout: Some(generated.domain_drain_timeout()),
        entity_gate_deadline: Some(generated.entity_gate_deadline()),
        transaction_idle_timeout: Some(generated.transaction_idle_timeout()),
        transaction_tombstone_retention: Some(generated.transaction_tombstone_retention()),
        command_retry_validity: Some(generated.command_retry_validity()),
        state_snapshot_interval: Some(generated.state_snapshot_interval()),
        transaction_max_statements: Some(generated.transaction_max_statements()),
        transaction_max_source_bytes: Some(generated.transaction_max_source_bytes()),
        transaction_max_open: Some(generated.transaction_max_open()),
        command_execution_capacity: Some(generated.command_execution_capacity()),
        replica_count: Some(generated.replica_count()),
        resource_max_archive_bytes: Some(generated.resource_max_archive_bytes()),
        resource_max_extracted_bytes: Some(generated.resource_max_extracted_bytes()),
        resource_max_file_count: Some(generated.resource_max_file_count()),
        max_batch_messages: Some(generated.max_batch_messages()),
        client_producer_session_bytes: Some(generated.client_producer_session_bytes()),
        client_consumer_session_bytes: Some(generated.client_consumer_session_bytes()),
        udf_code_bytes: Some(generated.udf_code_bytes()),
        http_header_bytes: Some(generated.http_header_bytes()),
        http_header_fields: Some(generated.http_header_fields()),
        client_batch_rows: Some(generated.client_batch_rows()),
        tls_cipher_suites: Some(generated.tls_cipher_suites()),
        tls_key_exchange_groups: Some(generated.tls_key_exchange_groups()),
    })
    .assured("the shared generator supplies values within every definition's bounds")
}

#[test]
fn bolero_cluster_parameter_records_preserve_every_value() {
    bolero::check!().with_type::<[u8; 384]>().for_each(|input| {
        let original = parameters(input);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&original)
            .assured("the bounded current parameter record is serializable");
        let decoded: ClusterParameterValues = rkyv::from_bytes::<_, rkyv::rancor::Error>(&bytes)
            .assured("a complete valid current parameter record decodes");
        assert_eq!(decoded, original);
        let json =
            serde_json::to_vec(&original).assured("all parameter values have JSON encodings");
        let decoded: ClusterParameterValues = serde_json::from_slice(&json)
            .assured("the JSON encoding preserves every valid parameter value");
        assert_eq!(decoded, original);
        let raw_revision = u64::from_le_bytes(
            input[..8]
                .try_into()
                .assured("the fixed input contains eight revision bytes"),
        );
        let record = ClusterParameterRecord {
            revision: std::num::NonZeroU64::new(raw_revision.max(1))
                .assured("the generator produces a positive revision"),
            values: original,
        };
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&record).assured("the complete record archives");
        assert_eq!(
            rkyv::from_bytes::<ClusterParameterRecord, rkyv::rancor::Error>(&bytes)
                .assured("the complete record recovers"),
            record
        );
        let json =
            serde_json::to_vec(&record).assured("the complete record has a JSON publication");
        assert_eq!(
            serde_json::from_slice::<ClusterParameterRecord>(&json)
                .assured("the complete publication decodes"),
            record
        );
        let publication = ClusterParameterPublication::new(record)
            .install(record)
            .assured("one record is a coherent applied publication");
        let json = serde_json::to_vec(&publication)
            .assured("the publication encodes all committed and running values");
        assert_eq!(
            serde_json::from_slice::<ClusterParameterPublication>(&json)
                .assured("the coherent publication decodes"),
            publication
        );
    });
}

#[test]
fn every_duration_refuses_zero_at_its_definition_boundary() {
    let current = ClusterParameterValues::default();
    let changes = [
        ClusterParameterChange::LicensingRequestTimeout(Duration::ZERO),
        ClusterParameterChange::LicensingRetryBackoffMax(Duration::ZERO),
        ClusterParameterChange::LicensingStreamIdleTimeout(Duration::ZERO),
        ClusterParameterChange::RemoteAckAliveInterval(Duration::ZERO),
        ClusterParameterChange::RemoteAckDispatchTimeout(Duration::ZERO),
        ClusterParameterChange::RemoteAckSilenceTimeout(Duration::ZERO),
        ClusterParameterChange::RemoteAckSweepInterval(Duration::ZERO),
        ClusterParameterChange::RemoteRelayTotalTimeout(Duration::ZERO),
        ClusterParameterChange::RemoteRelayInstantiationWait(Duration::ZERO),
        ClusterParameterChange::NodeUnavailabilityTimeout(Duration::ZERO),
        ClusterParameterChange::DomainDrainTimeout(Duration::ZERO),
        ClusterParameterChange::EntityGateDeadline(Duration::ZERO),
        ClusterParameterChange::TransactionIdleTimeout(Duration::ZERO),
        ClusterParameterChange::TransactionTombstoneRetention(Duration::ZERO),
        ClusterParameterChange::CommandRetryValidity(Duration::ZERO),
        ClusterParameterChange::StateSnapshotInterval(Duration::ZERO),
    ];
    for change in changes {
        let parameter = change.parameter();
        let updates = ClusterParameterUpdates::try_from(vec![change])
            .assured("one assignment is nonempty and unique");
        let refused = current
            .changed(&updates)
            .expect_err("a positive duration cannot become zero");
        assert_eq!(
            refused.current_context(),
            &ClusterParameterError::NonPositiveDuration { parameter }
        );
    }
}

#[test]
fn compact_parameters_preserve_count_and_full_width_byte_boundaries() {
    let count = u64::from(u32::MAX);
    let values = ClusterParameterValues::resolve(ClusterParameterSeedValues {
        payload_unfold_limit: Some(count),
        transaction_max_statements: Some(count),
        transaction_max_open: Some(count),
        command_execution_capacity: Some(count),
        replica_count: Some(count),
        max_batch_messages: Some(count),
        client_batch_rows: Some(count),
        http_header_fields: Some(HttpHeaderFields::MAXIMUM),
        password_iterations: Some(PasswordIterations::MAXIMUM),
        transaction_max_source_bytes: Some(u64::MAX),
        resource_max_file_count: Some(u64::MAX),
        resource_max_archive_bytes: Some(u64::MAX),
        resource_max_extracted_bytes: Some(u64::MAX),
        ..Default::default()
    })
    .assured("each semantic boundary remains accepted");
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&values).assured("current values archive");
    assert_eq!(
        rkyv::from_bytes::<ClusterParameterValues, rkyv::rancor::Error>(&bytes)
            .assured("current values recover without narrowing"),
        values
    );
    assert_eq!(
        serde_json::from_value::<ClusterParameterValues>(
            serde_json::to_value(values).assured("current values encode as JSON"),
        )
        .assured("JSON keeps semantic values"),
        values
    );
    assert_eq!(values.payload_unfold_limit(), count);
    assert_eq!(values.resource_max_archive_bytes(), u64::MAX);
}

#[test]
fn assignments_have_one_semantic_identity_and_current_literal_family() {
    let current = ClusterParameterValues::default();
    assert_eq!(
        current.described().len(),
        ClusterParameterKey::iter().count()
    );
    for described in current.described() {
        assert!(!described.value.is_empty());
        assert_eq!(
            described.parameter.keyword(),
            described
                .parameter
                .setting_name()
                .replace('-', "_")
                .to_ascii_uppercase()
        );
    }
    let error = ClusterParameterUpdates::try_from(Vec::new())
        .expect_err("an update requires an assignment");
    assert_eq!(error.current_context(), &ClusterParameterError::Empty);
    let error = ClusterParameterUpdates::try_from(vec![
        ClusterParameterChange::ReplicaCount(1),
        ClusterParameterChange::ReplicaCount(2),
    ])
    .expect_err("one parameter has one value in a transition");
    assert_eq!(
        error.current_context(),
        &ClusterParameterError::Duplicate {
            parameter: ClusterParameterKey::ReplicaCount
        }
    );
}

#[test]
fn a_rejected_transition_leaves_every_current_value_intact() {
    let original = parameters(&[17; 384]);
    let updates = ClusterParameterUpdates::try_from(vec![
        ClusterParameterChange::NodeUnavailabilityTimeout(Duration::from_secs(21)),
        ClusterParameterChange::CommandRetryValidity(Duration::ZERO),
    ])
    .assured("the two assignments name different parameters");
    assert!(original.changed(&updates).is_err());
    assert_eq!(original, parameters(&[17; 384]));
}

#[test]
fn decoded_duration_values_cross_the_same_positive_boundary() {
    let current = ClusterParameterValues::default();
    let mut encoded =
        serde_json::to_value(current).assured("all current parameters have JSON encodings");
    encoded["state_snapshot_interval"] = serde_json::json!({"secs": 0, "nanos": 0});
    let error = serde_json::from_value::<ClusterParameterValues>(encoded)
        .expect_err("a stored zero snapshot interval has no valid parameter value");
    assert!(
        error
            .to_string()
            .contains("state-snapshot-interval must be greater than zero")
    );
}

#[test]
fn delayed_observations_and_installations_preserve_the_newest_coherent_revision() {
    let first = ClusterParameterRecord {
        revision: std::num::NonZeroU64::MIN,
        values: ClusterParameterValues::default(),
    };
    let next = ClusterParameterRecord {
        revision: std::num::NonZeroU64::new(2).assured("the second revision is positive"),
        values: parameters(&[23; 384]),
    };
    let publication = ClusterParameterPublication::new(first)
        .observe(next)
        .assured("the next commit advances policy")
        .install(first)
        .assured("an admitted installation may finish behind the next commit");
    assert_eq!(publication.committed(), next);
    assert_eq!(publication.running(), Some(first));
    let publication = publication
        .install(next)
        .assured("catch-up installs the next commit")
        .observe(first)
        .assured("a delayed observation is harmless")
        .install(first)
        .assured("a delayed installation cannot regress running policy");
    assert_eq!(publication.committed(), next);
    assert_eq!(publication.running(), Some(next));
    let contradictory = ClusterParameterRecord {
        values: first.values,
        ..next
    };
    assert_eq!(
        publication.observe(contradictory),
        Err(ClusterParameterPublicationError::RevisionMismatch {
            revision: next.revision
        })
    );
    assert_eq!(next.catch_up(first), Ok(ClusterParameterCatchUp::Pending));
    assert_eq!(first.catch_up(next), Ok(ClusterParameterCatchUp::Ready));
    assert!(
        serde_json::from_value::<ClusterParameterPublication>(serde_json::json!({
            "committed": first, "running": next
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<ClusterParameterPublication>(serde_json::json!({
            "committed": next, "running": contradictory
        }))
        .is_err()
    );
}

#[test]
fn remote_ack_timing_changes_are_one_coherent_atomic_group() {
    let current = ClusterParameterValues::default();
    let updates = ClusterParameterUpdates::try_from(vec![
        ClusterParameterChange::RemoteAckAliveInterval(Duration::from_millis(1)),
        ClusterParameterChange::RemoteAckDispatchTimeout(Duration::from_millis(1)),
        ClusterParameterChange::RemoteAckSilenceTimeout(Duration::from_millis(10)),
        ClusterParameterChange::RemoteAckSweepInterval(Duration::from_millis(1)),
        ClusterParameterChange::RemoteRelayTotalTimeout(Duration::from_millis(50)),
        ClusterParameterChange::RemoteRelayInstantiationWait(Duration::from_millis(5)),
    ])
    .assured("one assignment per timing");
    let committed = current
        .changed(&updates)
        .assured("the complete group is coherent");
    let invalid = [
        ClusterParameterChange::RemoteAckSilenceTimeout(Duration::from_millis(4)),
        ClusterParameterChange::RemoteAckSweepInterval(Duration::from_millis(3)),
        ClusterParameterChange::RemoteRelayTotalTimeout(Duration::from_millis(2)),
    ];
    for change in invalid {
        let updates = ClusterParameterUpdates::try_from(vec![change]).assured("one assignment");
        assert!(committed.changed(&updates).is_err());
        assert_eq!(
            committed.remote_ack_silence_timeout(),
            Duration::from_millis(10)
        );
        assert_eq!(
            committed.remote_relay_total_timeout(),
            Duration::from_millis(50)
        );
    }
}

#[test]
fn lifecycle_policy_fits_each_local_shutdown_budget_independently() {
    let node = crate::ClusterNodeName::parse("node-1").assured("fixture name");
    let values = ClusterParameterValues::default();
    values
        .validate_runtime_budget(&node, crate::ShutdownSettings::default().capabilities())
        .assured("default lifecycle policy fits");
    for (drain_timeout, shutdown_timeout) in [
        (Duration::from_secs(29), Duration::from_secs(50)),
        (Duration::from_secs(50), Duration::from_secs(29)),
    ] {
        assert!(
            values
                .validate_runtime_budget(
                    &node,
                    crate::ShutdownCapabilities {
                        drain_timeout,
                        shutdown_timeout
                    }
                )
                .is_err()
        );
    }
    let updates =
        ClusterParameterUpdates::try_from(vec![ClusterParameterChange::EntityGateDeadline(
            Duration::from_secs(40),
        )])
        .assured("one assignment");
    let values = values
        .changed(&updates)
        .assured("the gate need not be shorter than the domain drain");
    values
        .validate_runtime_budget(
            &node,
            crate::ShutdownCapabilities {
                drain_timeout: Duration::from_secs(45),
                shutdown_timeout: Duration::from_secs(50),
            },
        )
        .assured("both policies fit the node");
}
