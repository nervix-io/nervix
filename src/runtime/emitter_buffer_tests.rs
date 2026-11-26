//! Emitter batch buffer tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests of how the emitter batch buffer packs, keeps, retires and settles the
//!   rows it buffers.
//! - **Depends on.** The emitter batch buffer and the runtime test fixtures.
//! - **Must not know.** Connector drivers or production task orchestration.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use super::*;
use crate::runtime::{
    emitter_ordering_group::EvaluatedOrderingGroups,
    test_fixtures::{emitter_context, input_batch, input_batch_with, input_schema, named},
};

/// A branch whose rows the buffer still holds unresolved moves from the retirements to the
/// kept branches, so its TTL never retires it with its rows; a branch whose every buffered row
/// was delivered, and one the buffer holds nothing of, stay retired.
#[nervix_primitives::test]
async fn a_branch_with_unresolved_rows_is_kept_and_a_resolved_branch_stays_retired() {
    use crate::runtime::test_fixtures::string_branch_key;

    let source: RelayName = named("events");
    let held = string_branch_key("tenant", "held");
    let delivered = string_branch_key("tenant", "delivered");
    let absent = string_branch_key("tenant", "absent");
    let mut buffer = EmitterBatchBuffer::new(
        &emitter_context(),
        &FlushPolicy::Immediate,
        Arc::new(EmitterBufferedMessages::default()),
    );
    let (held_acks, _held_completion) = AckSet::root();
    let mut held_input = input_batch_with(0, 0, held_acks);
    held_input.key = held.clone();
    let held_batch =
        EmitterPublishBatch::from_input(source.clone(), held_input, Timestamp::from_unix_nanos(0));
    buffer
        .retain_without_cadence(held_batch)
        .assured("the fixture buffer has an initialized immediate flush policy");
    let (delivered_acks, _delivered_completion) = AckSet::root();
    let mut delivered_input = input_batch_with(1, 1, delivered_acks);
    delivered_input.key = delivered.clone();
    let mut delivered_batch = EmitterPublishBatch::from_input(
        source.clone(),
        delivered_input,
        Timestamp::from_unix_nanos(1),
    );
    delivered_batch
        .mark_delivered(0, DeliveredAcknowledgements::Host)
        .assured("the fixture batch has one pending row before its delivery is marked");
    buffer
        .retain_without_cadence(delivered_batch)
        .assured("the fixture buffer has an initialized immediate flush policy");

    let mut retired = EmitterBranchRetirements::default();
    retired.insert(source.clone(), held.clone());
    retired.insert(source.clone(), delivered.clone());
    retired.insert(source.clone(), absent.clone());
    let mut kept = EmitterBranchRetirements::default();
    buffer.keep_branches_with_held_work(&mut retired, &mut kept);

    assert!(kept.contains(&source, &held));
    assert!(!retired.contains(&source, &held));
    assert!(retired.contains(&source, &delivered));
    assert!(!kept.contains(&source, &delivered));
    assert!(retired.contains(&source, &absent));
    assert!(!kept.contains(&source, &absent));
}

#[nervix_primitives::test]
async fn retiring_one_source_branch_settles_only_its_unresolved_host_rows() {
    use crate::runtime::test_fixtures::string_branch_key;

    let source: RelayName = named("events");
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    let mut buffer = EmitterBatchBuffer::new(
        &emitter_context(),
        &FlushPolicy::Immediate,
        Arc::new(EmitterBufferedMessages::default()),
    );
    let mut completions = Vec::new();
    let mut connector_acks = None;
    for index in 0..5 {
        let (acks, completion) = AckSet::root();
        if index == 4 {
            connector_acks = Some(acks.clone());
        }
        let mut input = input_batch_with(index, index, acks);
        input.key = if index == 1 {
            beta.clone()
        } else {
            acme.clone()
        };
        let mut batch = EmitterPublishBatch::from_input(
            if index == 2 {
                named("independent")
            } else {
                source.clone()
            },
            input,
            Timestamp::from_unix_nanos(index),
        );
        if index == 3 {
            batch
                .mark_delivered(0, DeliveredAcknowledgements::Host)
                .assured("each fixture batch has one pending row before its delivery is marked");
        }
        if index == 4 {
            batch
                .mark_delivered(0, DeliveredAcknowledgements::Connector)
                .assured("each fixture batch has one pending row before its delivery is marked");
        }
        buffer
            .retain_without_cadence(batch)
            .assured("the fixture buffer has an initialized immediate flush policy");
        completions.push(completion);
    }
    buffer.report_staged_messages(1);
    let mut retired = EmitterBranchRetirements::default();
    retired.insert(source.clone(), acme.clone());
    let mut sent = 0;
    buffer
        .retire_branches(&retired, &mut |report| sent += report.messages)
        .await;
    assert_eq!(sent, 1);
    assert_eq!(buffer.pending_messages, 2);
    assert_eq!(buffer.pending.len(), 2);
    assert_eq!(buffer.pending[0].relay_batch().key, beta);
    assert_eq!(buffer.pending[1].source_relay(), &named("independent"));
    assert_eq!(buffer.buffered_messages.reported.load(Ordering::Acquire), 3);
    buffer
        .retire_branches(&retired, &mut |report| sent += report.messages)
        .await;
    assert_eq!(sent, 1, "delivered rows are counted once");
    let [
        pending,
        first_survivor,
        second_survivor,
        delivered,
        mut staged,
    ] = completions
        .try_into()
        .assured("the fixture loop appended exactly five completion handles");
    assert_eq!(
        pending.wait().await,
        AckOutcome::NoAck("emitter source branch retired".to_string())
    );
    assert_eq!(delivered.wait().await, AckOutcome::Ack);
    assert!(
        nervix_primitives::time::timeout(Duration::from_millis(20), staged.wait_for_progress())
            .await
            .is_err()
    );
    connector_acks
        .assured("the fifth fixture row saved its connector acknowledgement above")
        .ack_success();
    assert_eq!(staged.wait().await, AckOutcome::Ack);
    for batch in &mut buffer.pending {
        batch
            .mark_delivered(0, DeliveredAcknowledgements::Host)
            .assured("both surviving fixture batches still have their one pending row");
    }
    assert_eq!(first_survivor.wait().await, AckOutcome::Ack);
    assert_eq!(second_survivor.wait().await, AckOutcome::Ack);
}

#[nervix_primitives::test]
async fn cancelled_message_error_delivery_keeps_the_record_pending() {
    let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
        labels: Default::default(),
        name: named("events"),
        fields: vec![nervix_models::SchemaField {
            name: named("value"),
            ty: nervix_models::ParseAsType::String,
            optional: false,
            sensitive: false,
        }],
    }));
    let (acks, _completion) = AckSet::root();
    let batch = RelayRecordBatch::single(
        schema,
        None,
        test_runtime_row([(
            "value".to_string(),
            RuntimeValue::String("poison".to_string()),
        )]),
        acks,
    )
    .expect("test emitter batch must build");
    let mut batch = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100));

    assert!(
        nervix_primitives::time::timeout(
            Duration::from_millis(20),
            batch.mark_rejected_after_delivery(0, std::future::pending()),
        )
        .await
        .is_err(),
        "the simulated message-error delivery must remain pending"
    );
    assert_eq!(
        batch.rows[0],
        BufferedRow::Pending,
        "cancelling message-error delivery must leave the poison record retryable"
    );

    batch
        .mark_rejected_after_delivery(0, std::future::ready(()))
        .await
        .expect("completed message-error delivery must account for the record");
    assert_eq!(batch.rows[0], BufferedRow::Rejected);
    assert!(
        batch.delivered_report().is_none(),
        "a rejected record is never sent"
    );
}

#[nervix_primitives::test]
async fn rejected_record_ack_remains_held_for_message_error_delivery() {
    let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
        labels: Default::default(),
        name: named("events"),
        fields: vec![nervix_models::SchemaField {
            name: named("value"),
            ty: nervix_models::ParseAsType::String,
            optional: false,
            sensitive: false,
        }],
    }));
    let (acks, mut completion) = AckSet::root();
    let message_error_acks = acks.clone();
    let batch = RelayRecordBatch::single(
        schema,
        None,
        test_runtime_row([(
            "value".to_string(),
            RuntimeValue::String("poison".to_string()),
        )]),
        acks,
    )
    .expect("test emitter batch must build");
    let mut batch = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100));
    batch
        .mark_rejected(0)
        .expect("test record must be marked rejected");
    let mut buffer = EmitterBatchBuffer::default();
    buffer.pending.push(batch);

    buffer.clear();

    assert!(
            nervix_primitives::time::timeout(
                Duration::from_millis(20),
                completion.wait_for_progress(),
            )
            .await
            .is_err(),
            "clearing an accounted poison record must not release its source ACK"
        );
    message_error_acks.ack_success();
    assert_eq!(completion.wait().await, AckOutcome::Ack);
}

#[test]
fn publish_batch_requires_one_header_row_per_record() {
    let batch = input_batch();
    let from_batch =
        EmitterPublishBatch::from_batch(batch.clone(), Timestamp::from_unix_nanos(100));
    assert!(from_batch.headers.is_none());
    assert_eq!(from_batch.message_count(), 1);
    assert_eq!(
        from_batch.domain_timestamp(),
        Some(Timestamp::from_unix_nanos(0))
    );

    let headers = vec![EmitterHeaders::Text(vec![(
        "route".to_string(),
        "fast".to_string(),
    )])];
    let with_headers = EmitterPublishBatch::new(
        named("test_relay"),
        batch.clone(),
        Some(headers.clone()),
        Timestamp::from_unix_nanos(100),
    )
    .expect("row-aligned headers must build");
    assert_eq!(with_headers.headers.as_ref(), Some(&headers));
    let header_bytes: u64 = "routefast".len().arch_into();
    assert_eq!(
        with_headers.estimated_bytes(),
        batch.estimated_bytes() + header_bytes
    );

    let error = match EmitterPublishBatch::new(
        named("test_relay"),
        batch,
        Some(Vec::new()),
        Timestamp::from_unix_nanos(100),
    ) {
        Err(error) => error,
        Ok(_) => panic!("missing row headers must be rejected"),
    };
    assert_eq!(
        *error.current_context(),
        EmitterRuntimeError::HeaderCountMismatch {
            header_count: 0,
            row_count: 1,
        }
    );
}

#[test]
fn a_batch_counts_the_request_fields_of_its_rows() {
    let fields = request_fields();
    let expected: u64 = ("POST".len() + "/events".len() + "x-key".len() + "abc".len()).arch_into();
    assert_eq!(fields.estimated_bytes(), expected);
    let request_bytes = fields.estimated_bytes();
    let plain = EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    let with_requests =
        EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100))
            .with_http_requests(AdmittedHttpRequests::published(vec![fields]))
            .expect("one request for the one row");

    assert_eq!(
        with_requests.estimated_bytes(),
        plain.estimated_bytes() + request_bytes
    );
    assert_eq!(
        with_requests
            .http_request(0)
            .expect("the row has its request")
            .target
            .as_str(),
        "/events"
    );
}

/// The request fields `POST /events` with one `X-Key` header, which a row is admitted with.
fn request_fields() -> HttpRequestFields {
    let origin = nervix_models::HttpOrigin::parse("https://api.example.com")
        .expect("the test origin has an HTTPS scheme and a host");
    let mut headers = nervix_models::HttpApplicationHeaders::default();
    headers
        .insert(
            nervix_models::HttpHeaderName::parse("X-Key").expect("a valid field name"),
            nervix_models::HttpHeaderValue::parse("abc").expect("a valid field value"),
        )
        .expect("one short header is within the envelope");
    HttpRequestFields {
        method: nervix_models::HttpMethod::parse("POST", nervix_models::HttpBodyMode::WithoutBody)
            .expect("POST is a valid method"),
        target: origin.target("/events").expect("an origin-relative target"),
        headers,
    }
}

#[test]
fn a_delivered_http_request_counts_its_codec_record_and_never_its_request_fields() {
    let published = batch_of(&[1, 2]);
    let without_body =
        EmitterPublishBatch::from_batch(published.clone(), Timestamp::from_unix_nanos(100))
            .with_http_requests(AdmittedHttpRequests::published(vec![
                request_fields(),
                request_fields(),
            ]))
            .expect("one request for each row");
    let with_body =
        EmitterPublishBatch::from_batch(published.clone(), Timestamp::from_unix_nanos(100))
            .with_http_requests(AdmittedHttpRequests::encoded(vec![
                request_fields(),
                request_fields(),
            ]))
            .expect("one request for each row");

    for mut batch in [without_body.clone(), with_body.clone()] {
        batch
            .mark_delivered(0, DeliveredAcknowledgements::Host)
            .expect("the first request can be delivered");
        batch
            .mark_rejected(1)
            .expect("the second request can be refused");
        let report = batch.delivered_report().expect("one request was delivered");
        assert_eq!(report.messages, 1);
    }

    let mut bodyless = without_body;
    let mut encoded = with_body;
    for row in 0..2 {
        bodyless
            .mark_delivered(row, DeliveredAcknowledgements::Host)
            .expect("every request can be delivered");
        encoded
            .mark_delivered(row, DeliveredAcknowledgements::Host)
            .expect("every request can be delivered");
    }
    let bodyless = bodyless
        .delivered_report()
        .expect("both requests were delivered");
    assert_eq!(bodyless.messages, 2);
    assert_eq!(bodyless.bytes, 0, "a request without a body carries none");
    let encoded = encoded
        .delivered_report()
        .expect("both requests were delivered");
    assert_eq!(encoded.messages, 2);
    assert_eq!(
        encoded.bytes,
        published.estimated_bytes(),
        "the method, target and header bytes are request metadata, not payload"
    );

    let mut partly =
        EmitterPublishBatch::from_batch(published.clone(), Timestamp::from_unix_nanos(100))
            .with_http_requests(AdmittedHttpRequests::encoded(vec![
                request_fields(),
                request_fields(),
            ]))
            .expect("one request for each row");
    partly
        .mark_rejected(0)
        .expect("the first request can be refused");
    partly
        .mark_delivered(1, DeliveredAcknowledgements::Host)
        .expect("the second request can be delivered");
    let partly = partly
        .delivered_report()
        .expect("one request was delivered");
    assert_eq!(partly.bytes, batch_of(&[2]).estimated_bytes());
}

#[test]
fn a_delivered_row_counts_only_its_own_ordering_group() {
    let mut batch =
        EmitterPublishBatch::from_batch(batch_of(&[1, 2, 3]), Timestamp::from_unix_nanos(100))
            .with_ordering_groups(OrderingGroups::Evaluated(
                EvaluatedOrderingGroups::from_rows([
                    Ok("tenant-a"),
                    Ok("tenant-bb"),
                    Err(OrderingGroupError::Null),
                ]),
            ))
            .expect("one group for each row must attach");
    batch
        .mark_delivered(0, DeliveredAcknowledgements::Host)
        .expect("the first row can be delivered");
    batch
        .mark_rejected(1)
        .expect("the second row can be rejected");
    batch
        .mark_delivered(2, DeliveredAcknowledgements::Host)
        .expect("the third row can be delivered");

    let report = batch.delivered_report().expect("two rows were delivered");

    let group_bytes: u64 = "tenant-a".len().arch_into();
    assert_eq!(report.messages, 2);
    assert_eq!(
        report.bytes,
        batch_of(&[1, 3]).estimated_bytes() + group_bytes,
        "a row without a group adds none, and a refused row's group is not sent"
    );
}

#[test]
fn publish_batch_rejects_misaligned_groups_and_row_updates() {
    let groups =
        match EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100))
            .with_ordering_groups(OrderingGroups::Evaluated(
                EvaluatedOrderingGroups::from_rows([]),
            )) {
            Ok(_) => panic!("ordering groups must stay aligned with source rows"),
            Err(error) => error,
        };
    assert_eq!(
        *groups.current_context(),
        EmitterRuntimeError::OrderingGroupCountMismatch {
            group_count: 0,
            row_count: 1,
        }
    );

    let mut delivered =
        EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    let error = delivered
        .mark_delivered(1, DeliveredAcknowledgements::Host)
        .expect_err("delivery cannot address a missing row");
    assert_eq!(
        *error.current_context(),
        EmitterRuntimeError::DeliveryRowOutOfBounds {
            row: 1,
            row_count: 1,
        }
    );

    delivered.batch.acks.clear();
    let error = delivered
        .mark_delivered(0, DeliveredAcknowledgements::Host)
        .expect_err("delivery requires the row's acknowledgement set");
    assert_eq!(
        *error.current_context(),
        EmitterRuntimeError::AcknowledgementRowOutOfBounds {
            row: 0,
            row_count: 0,
        }
    );

    let mut rejected =
        EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    let error = rejected
        .mark_rejected(1)
        .expect_err("rejection cannot address a missing row");
    assert_eq!(
        *error.current_context(),
        EmitterRuntimeError::RejectionRowOutOfBounds {
            row: 1,
            row_count: 1,
        }
    );
}

#[nervix_primitives::test]
async fn a_row_resolves_once_however_often_it_is_answered() {
    let (root, mut completion) = AckSet::root();
    let mut batch = EmitterPublishBatch::from_batch(
        input_batch_with(1, 0, root.attached()),
        Timestamp::from_unix_nanos(100),
    );
    batch
        .mark_prepared(0)
        .expect("a pending row can be prepared");
    let error = batch
        .mark_prepared(0)
        .expect_err("a prepared row cannot be prepared again");
    assert_eq!(
        *error.current_context(),
        EmitterRuntimeError::RowAlreadyPrepared { row: 0 }
    );
    assert!(batch.pending_record_rows().is_empty());

    batch
        .mark_delivered(0, DeliveredAcknowledgements::Host)
        .expect("a prepared row can be delivered");
    batch
        .mark_delivered(0, DeliveredAcknowledgements::Host)
        .expect("delivering a resolved row changes nothing");
    batch
        .release_prepared(0)
        .expect("releasing a resolved row changes nothing");

    assert_eq!(batch.resolved_rows(), vec![true]);
    assert!(
        nervix_primitives::time::timeout(Duration::from_millis(20), completion.wait_for_progress())
            .await
            .is_err(),
        "a second delivery must not resolve the row's share of the root again"
    );
    root.ack_success();
    assert_eq!(completion.wait().await, AckOutcome::Ack);
    let error = batch
        .mark_prepared(1)
        .expect_err("preparing needs a row of the batch");
    assert_eq!(
        *error.current_context(),
        EmitterRuntimeError::PreparedRowOutOfBounds {
            row: 1,
            row_count: 1,
        }
    );
}

#[test]
fn packing_reads_a_retained_row_as_a_boundary_and_skips_a_resolved_one() {
    let mut batch = EmitterPublishBatch::from_batch(
        RelayRecordBatch::from_messages(
            input_schema(),
            (1..=4)
                .map(|value| RelayMessage {
                    key: None,
                    record: test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
                    acks: AckSet::empty(),
                })
                .collect(),
        )
        .expect("valid four-row emitter batch"),
        Timestamp::from_unix_nanos(100),
    );
    batch
        .mark_prepared(1)
        .expect("a pending row can be prepared");
    batch
        .mark_delivered(3, DeliveredAcknowledgements::Host)
        .expect("a pending row can be delivered");

    assert_eq!(
        batch.rows_to_pack(),
        vec![
            RowToPack::Pending(0),
            RowToPack::Retained,
            RowToPack::Pending(2),
        ]
    );
}

#[test]
fn a_released_row_waits_for_the_next_attempt() {
    let mut batch = EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    batch
        .mark_prepared(0)
        .expect("a pending row can be prepared");
    batch
        .release_prepared(0)
        .expect("a prepared row can be released");

    assert_eq!(batch.pending_record_rows(), vec![0]);
    assert_eq!(batch.resolved_rows(), vec![false]);
}

#[test]
fn publish_batch_reads_each_row_group_only_where_the_emitter_declares_one() {
    let undeclared =
        EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    assert_eq!(
        undeclared
            .ordering_group(0)
            .expect("an emitter without a group reads none"),
        Ok(None)
    );

    let batch = input_batch();
    let batch_bytes = batch.estimated_bytes();
    let evaluated = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100))
        .with_ordering_groups(OrderingGroups::Evaluated(
            EvaluatedOrderingGroups::from_rows([Ok("tenant-a")]),
        ))
        .expect("one group for one row must attach");
    assert_eq!(
        evaluated
            .ordering_group(0)
            .expect("the row has a group entry"),
        Ok(Some("tenant-a".to_string()))
    );
    let group_bytes: u64 = "tenant-a".len().arch_into();
    assert_eq!(evaluated.estimated_bytes(), batch_bytes + group_bytes);
    let error = evaluated
        .ordering_group(1)
        .expect_err("a row outside the batch has no group entry");
    assert_eq!(*error.current_context(), EmitterRuntimeError::EncodeBatch);
}
#[nervix_primitives::test]
async fn publish_batch_ack_helpers_preserve_and_complete_all_roots() {
    let (acks, completion) = AckSet::root();
    let batch = EmitterPublishBatch::from_batch(
        input_batch_with(1, 0, acks),
        Timestamp::from_unix_nanos(100),
    );

    assert!(!batch.merged_acks().is_empty());
    batch.merged_acks().ack_success();
    assert_eq!(completion.wait().await, AckOutcome::Ack);
}

#[test]
fn batch_buffer_rejects_input_without_an_initialized_flush_policy() {
    let mut buffer = EmitterBatchBuffer::default();
    let error = buffer
        .push(
            &emitter_context(),
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .expect_err("an unconfigured buffer must reject input");

    assert_eq!(
        *error.current_context(),
        EmitterRuntimeError::FlushPolicyNotInitialized
    );
    assert!(buffer.is_empty());
    assert_eq!(buffer.pending_bytes, 0);
    assert!(buffer.deadline().is_none());
}

#[test]
fn batch_buffer_tracks_size_messages_deadline_and_latest_timestamp() {
    let reported_messages = Arc::new(AtomicUsize::new(0));
    let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
    let mut buffer = EmitterBatchBuffer::default();
    buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
        interval: Duration::from_secs(60),
        max_batch_size: u64::MAX,
    });
    buffer.buffered_messages = buffered_messages.clone();
    let first = EmitterPublishBatch::from_batch(
        input_batch_with(1, 10, AckSet::empty()),
        Timestamp::from_unix_nanos(100),
    );
    let second_batch = RelayRecordBatch::from_messages(
        input_schema(),
        vec![
            RelayMessage {
                key: None,
                record: test_runtime_row([("value".to_string(), RuntimeValue::I64(2))])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(20)),
                acks: AckSet::empty(),
            },
            RelayMessage {
                key: None,
                record: test_runtime_row([("value".to_string(), RuntimeValue::I64(3))])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(20)),
                acks: AckSet::empty(),
            },
        ],
    )
    .expect("valid multi-row emitter input batch");
    let second = EmitterPublishBatch::new(
        named("test_relay"),
        second_batch,
        Some(vec![
            EmitterHeaders::Text(vec![("name".to_string(), "value".to_string())]),
            EmitterHeaders::Text(Vec::new()),
        ]),
        Timestamp::from_unix_nanos(100),
    )
    .expect("headers must align");
    let expected_bytes = first
        .estimated_bytes()
        .checked_add(second.estimated_bytes())
        .assured("the two test batches are far smaller than the u64 byte range");

    let context = emitter_context();
    assert!(
        !buffer
            .push(&context, first)
            .expect("first batch must buffer")
    );
    assert_eq!(buffer.pending_messages, 1);
    assert!(
        buffer.deadline().is_some(),
        "the first push arms the cadence"
    );
    assert!(
        !buffer
            .push(&context, second)
            .expect("second batch must buffer")
    );

    assert!(
        !buffer
            .is_due(&context)
            .expect("the fixture clock stays installed"),
        "a second push does not bring the cadence forward"
    );
    assert_eq!(reported_messages.load(Ordering::Acquire), 3);
    assert_eq!(buffer.pending_messages, 3);
    assert!(
        buffer.delivered_report().is_none(),
        "nothing is sent before the emitter delivers it"
    );
    for batch in &mut buffer.pending {
        for row in 0..batch.rows.len() {
            batch
                .mark_delivered(row, DeliveredAcknowledgements::Host)
                .expect("every buffered row can be delivered");
        }
    }
    let report = buffer
        .delivered_report()
        .expect("delivered batches must produce a report");
    assert_eq!(report.messages, 3);
    assert_eq!(report.bytes, expected_bytes);
    assert_eq!(report.domain_timestamp, Timestamp::from_unix_nanos(20));
    assert_eq!(buffer.pending_bytes, expected_bytes);
}

/// A batch of `values` whose every record has one acknowledgement-free message.
fn batch_of(values: &[i64]) -> RelayRecordBatch {
    let mut messages = Vec::with_capacity(values.len());
    for value in values {
        messages.push(RelayMessage {
            key: None,
            record: test_runtime_row([("value".to_string(), RuntimeValue::I64(*value))])
                .with_ingested_at_watermarks(Timestamp::from_unix_nanos(30)),
            acks: AckSet::empty(),
        });
    }
    RelayRecordBatch::from_messages(input_schema(), messages)
        .expect("the test records match the emitter input schema")
}

#[test]
fn a_flush_reports_each_delivered_row_once_and_no_rejected_row() {
    let headers = vec![
        EmitterHeaders::Text(vec![("a".to_string(), "1".to_string())]),
        EmitterHeaders::Text(vec![("b".to_string(), "22".to_string())]),
        EmitterHeaders::Text(vec![("c".to_string(), "333".to_string())]),
    ];
    let mut mixed = EmitterPublishBatch::new(
        named("test_relay"),
        batch_of(&[1, 2, 3]),
        Some(headers),
        Timestamp::from_unix_nanos(100),
    )
    .expect("headers must align");
    mixed
        .mark_delivered(0, DeliveredAcknowledgements::Host)
        .expect("the first row can be delivered");
    mixed
        .mark_rejected(1)
        .expect("the second row can be rejected");
    mixed
        .mark_delivered(2, DeliveredAcknowledgements::Host)
        .expect("the third row can be delivered");
    mixed
        .mark_rejected(2)
        .expect("rejecting a delivered row changes nothing");
    let mut refused =
        EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    refused
        .mark_rejected(0)
        .expect("the only row can be rejected");
    assert!(
        refused.delivered_report().is_none(),
        "a batch whose every row was refused sends nothing"
    );
    let mut buffer = EmitterBatchBuffer::default();
    buffer.set_flush_policy(RuntimeFlushPolicy::Immediate {
        minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum(),
    });
    buffer
        .retain_without_cadence(mixed)
        .expect("the configured buffer retains the batch");
    buffer
        .retain_without_cadence(refused)
        .expect("the configured buffer retains the batch");

    let report = buffer.delivered_report().expect("two rows were delivered");

    // The delivered rows carry their own records and headers, and nothing of the refused ones.
    let header_bytes: u64 = "a1c333".len().arch_into();
    assert_eq!(report.messages, 2);
    assert_eq!(
        report.bytes,
        batch_of(&[1, 3]).estimated_bytes() + header_bytes
    );
    assert_eq!(report.domain_timestamp, Timestamp::from_unix_nanos(30));
}

#[test]
fn batch_buffer_honors_its_size_boundary_and_logical_cadence() {
    let context = emitter_context();
    let mut buffer = EmitterBatchBuffer::default();
    buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
        interval: Duration::from_secs(60),
        max_batch_size: 1,
    });

    assert!(
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100),)
            )
            .expect("batch must buffer")
    );
    assert!(matches!(
        buffer.deadline(),
        Some(BranchBufferDeadline::Logical(_))
    ));
    assert!(
        !buffer
            .is_due(&context)
            .expect("the fixture clock stays installed")
    );

    let mut immediate = EmitterBatchBuffer::default();
    immediate.flush_policy = Some(RuntimeFlushPolicy::Immediate {
        minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum(),
    });
    immediate
        .push(
            &context,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .expect("batch must buffer");
    assert!(matches!(
        immediate.deadline(),
        Some(BranchBufferDeadline::Physical(_))
    ));
}

#[test]
fn forced_retry_ignores_the_ordinary_buffer_deadline() {
    let context = emitter_context();
    let mut buffer = EmitterBatchBuffer::default();
    buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
        interval: Duration::from_secs(60),
        max_batch_size: u64::MAX,
    });
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .expect("retry batch must buffer");
    assert!(
        !buffer
            .is_due(&context)
            .expect("the fixture clock stays installed")
    );

    assert!(
        buffer
            .should_flush(&context, true)
            .expect("a forced flush needs no clock read")
    );
    assert!(
        !buffer
            .should_flush(&context, false)
            .expect("the fixture clock stays installed")
    );
}

#[nervix_primitives::test]
async fn a_emitter_that_commits_separately_takes_the_rows_it_accepted() {
    let (first_acks, mut first_completion) = AckSet::root();
    let (second_acks, second_completion) = AckSet::root();
    let mut batch = EmitterPublishBatch::from_batch(
        RelayRecordBatch::from_messages(
            input_schema(),
            vec![
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
                    acks: first_acks,
                },
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("value".to_string(), RuntimeValue::I64(2))]),
                    acks: second_acks,
                },
            ],
        )
        .expect("valid two-row emitter batch"),
        Timestamp::from_unix_nanos(100),
    );

    let retained = batch.acks_for_rows(&[0]);
    batch
        .mark_delivered(0, DeliveredAcknowledgements::Connector)
        .expect("a staged row must be marked delivered");
    batch
        .mark_delivered(1, DeliveredAcknowledgements::Host)
        .expect("a published row must be marked delivered");

    assert!(batch.pending_record_rows().is_empty());
    assert_eq!(batch.resolved_rows(), vec![true, true]);
    // The emitter's commit counts the staged row as sent, so the batch counts only the other.
    let report = batch.delivered_report().expect("the published row is sent");
    assert_eq!(report.messages, 1);
    assert_eq!(second_completion.wait().await, AckOutcome::Ack);
    // The staged row is the emitter's until its commit resolves it, so the host left it alone.
    retained.ack_alive();
    assert_eq!(
        first_completion.wait_for_progress().await,
        AckProgress::Alive
    );
    retained.ack_success();
    assert_eq!(first_completion.wait().await, AckOutcome::Ack);
}

#[test]
fn buffered_and_staged_message_counts_are_summed_independently() {
    let reported = Arc::new(AtomicUsize::new(0));
    let buffered = EmitterBufferedMessages::new(reported.clone());

    buffered.set_buffered(2);
    buffered.set_staged(3);
    assert_eq!(reported.load(Ordering::Acquire), 5);

    buffered.set_buffered(0);
    assert_eq!(reported.load(Ordering::Acquire), 3);
    buffered.set_staged(0);
    assert_eq!(reported.load(Ordering::Acquire), 0);
}

/// An emitter that commits on its own cadence still holds staged messages when its emitter task is
/// torn down. Nothing holds them once the task is gone, so the count drains read for the
/// emitter drops to zero instead of keeping them until a restarted task first changes its
/// buffer.
#[test]
fn a_torn_down_emitter_keeps_none_of_its_staged_messages_counted() {
    let context = emitter_context();
    let reported_messages = Arc::new(AtomicUsize::new(0));
    let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
    let mut buffer =
        EmitterBatchBuffer::new(&context, &FlushPolicy::Immediate, buffered_messages.clone());
    buffer.report_staged_messages(3);
    assert_eq!(reported_messages.load(Ordering::Acquire), 3);

    drop(buffer);

    assert_eq!(
        reported_messages.load(Ordering::Acquire),
        0,
        "a torn-down emitter still reports the messages its emitter had staged"
    );
}

#[test]
fn batch_buffer_drain_clear_reconfigure_and_drop_reset_accounting() {
    let context = emitter_context();
    let reported_messages = Arc::new(AtomicUsize::new(0));
    let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
    let mut buffer = EmitterBatchBuffer::new(
        &context,
        &FlushPolicy::Each {
            interval: "10s".to_string(),
            max_batch_size: "1MiB".to_string(),
        },
        buffered_messages.clone(),
    );
    assert!(buffer.flush_policy.is_some());
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .expect("configured buffer must accept input");
    assert_eq!(buffer.pending_messages, 1);
    buffer
        .reconfigure(&context, &FlushPolicy::Immediate)
        .expect("the fixture clock stays installed");
    assert_eq!(
        buffer.flush_policy,
        Some(RuntimeFlushPolicy::Immediate {
            minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum()
        })
    );
    assert!(
        matches!(buffer.deadline(), Some(BranchBufferDeadline::Physical(_))),
        "a reconfigured Immediate cadence re-arms the retained batches physically"
    );

    let drained = buffer.drain_pending();
    assert_eq!(drained.len(), 1);
    assert!(buffer.is_empty());
    assert_eq!(buffer.pending_bytes, 0);
    assert_eq!(buffer.pending_messages, 0);
    assert!(buffer.deadline().is_none());
    assert_eq!(reported_messages.load(Ordering::Acquire), 0);

    buffer
        .push(
            &emitter_context(),
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .expect("reconfigured buffer must accept input");
    assert_eq!(buffer.pending_messages, 1);
    buffer.clear();
    assert!(buffer.delivered_report().is_none());
    assert_eq!(buffer.pending_messages, 0);
    assert_eq!(reported_messages.load(Ordering::Acquire), 0);

    buffered_messages.set_buffered(7);
    drop(buffer);
    assert_eq!(reported_messages.load(Ordering::Acquire), 0);
}

#[nervix_primitives::test]
async fn batch_buffer_ack_helpers_merge_and_complete_pending_batches() {
    let (first_acks, first_completion) = AckSet::root();
    let (second_acks, second_completion) = AckSet::root();
    let context = emitter_context();
    let mut buffer = EmitterBatchBuffer::default();
    buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate {
        minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum(),
    });
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(
                input_batch_with(1, 0, first_acks),
                Timestamp::from_unix_nanos(100),
            ),
        )
        .expect("first batch must buffer");
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(
                input_batch_with(2, 0, second_acks),
                Timestamp::from_unix_nanos(100),
            ),
        )
        .expect("second batch must buffer");

    assert!(!buffer.pending_acks().is_empty());
    buffer.pending_acks().ack_success();
    assert_eq!(first_completion.wait().await, AckOutcome::Ack);
    assert_eq!(second_completion.wait().await, AckOutcome::Ack);
}

#[nervix_primitives::test]
async fn retained_batch_clone_owns_exactly_one_attached_ack_share() {
    let (root, mut completion) = AckSet::root();
    let attached = root.attached();
    let batch = EmitterPublishBatch::from_batch(
        input_batch_with(1, 0, attached),
        Timestamp::from_unix_nanos(100),
    );
    let mut buffer = EmitterBatchBuffer::default();
    buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate {
        minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum(),
    });

    buffer
        .push(&emitter_context(), batch.clone())
        .expect("retry buffer must retain the batch");
    root.ack_success();
    assert!(
        nervix_primitives::time::timeout(Duration::from_millis(20), completion.wait_for_progress())
            .await
            .is_err(),
        "the retained attached share must keep the root pending"
    );

    buffer.pending_acks().ack_success();
    buffer.clear();
    assert_eq!(completion.wait().await, AckOutcome::Ack);
    drop(batch);
}

#[nervix_primitives::test]
async fn batch_buffer_drop_no_acks_every_retained_batch() {
    let (first_acks, first_completion) = AckSet::root();
    let (second_acks, second_completion) = AckSet::root();
    let reported_messages = Arc::new(AtomicUsize::new(0));
    let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
    let context = emitter_context();
    let mut buffer = EmitterBatchBuffer::default();
    buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate {
        minimum: nervix_models::DataPlaneSettings::default().immediate_flush_minimum(),
    });
    buffer.buffered_messages = buffered_messages.clone();
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(
                input_batch_with(1, 0, first_acks),
                Timestamp::from_unix_nanos(100),
            ),
        )
        .expect("first batch must buffer");
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(
                input_batch_with(2, 0, second_acks),
                Timestamp::from_unix_nanos(100),
            ),
        )
        .expect("second batch must buffer");

    drop(buffer);

    assert_eq!(
        first_completion.wait().await,
        AckOutcome::NoAck("emitter dropped buffered batch".to_string())
    );
    assert_eq!(
        second_completion.wait().await,
        AckOutcome::NoAck("emitter dropped buffered batch".to_string())
    );
    assert_eq!(reported_messages.load(Ordering::Acquire), 0);
}

/// The source batch `orders (order_id STRING, amount I64)`, one row for each of `rows`.
fn orders(rows: &[(&str, i64)]) -> RelayRecordBatch {
    let schema = crate::runtime::test_fixtures::test_schema(&[
        ("order_id", nervix_models::ParseAsType::String),
        ("amount", nervix_models::ParseAsType::I64),
    ]);
    let mut messages = Vec::with_capacity(rows.len());
    for (order_id, amount) in rows {
        messages.push(RelayMessage {
            key: None,
            record: test_runtime_row([
                (
                    "order_id".to_string(),
                    RuntimeValue::String((*order_id).to_string()),
                ),
                ("amount".to_string(), RuntimeValue::I64(*amount)),
            ])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(30)),
            acks: AckSet::empty(),
        });
    }
    RelayRecordBatch::from_messages(schema, messages)
        .expect("the test rows match the orders schema")
}

/// The `invoice (reference STRING)` records an emitter's route built, one for each of
/// `references`.
fn invoices(references: &[&str]) -> RelayRecordBatch {
    let schema = crate::runtime::test_fixtures::test_schema(&[(
        "reference",
        nervix_models::ParseAsType::String,
    )]);
    let mut messages = Vec::with_capacity(references.len());
    for reference in references {
        messages.push(RelayMessage {
            key: None,
            record: test_runtime_row([(
                "reference".to_string(),
                RuntimeValue::String((*reference).to_string()),
            )])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(30)),
            acks: AckSet::empty(),
        });
    }
    RelayRecordBatch::from_messages(schema, messages)
        .expect("the test rows match the invoice schema")
}

/// The materialized state a batch of these tests was admitted with.
fn north_state() -> HashMap<String, RuntimeValue> {
    HashMap::from_iter([(
        "relay_state.settings.label".to_string(),
        RuntimeValue::String("north-ready".to_string()),
    )])
}

/// The text value of field `field` of `record`.
fn text(record: &RuntimeRow, field: &str) -> String {
    match record.value(field) {
        Ok(Some(RuntimeValue::String(value))) => value,
        other => panic!("field '{field}' is not a text value: {other:?}"),
    }
}

#[test]
fn a_row_that_fails_after_its_route_ran_reads_its_source_row_its_built_record_and_its_state() {
    let source = orders(&[("n1", 7), ("dropped", 0), ("n2", 9)]);
    // The route kept source rows 2 and 0, in that order, and built one invoice from each.
    let batch = EmitterPublishBatch::from_batch(
        invoices(&["inv-n2", "inv-n1"]),
        Timestamp::from_unix_nanos(100),
    )
    .with_error_route_inputs(ErrorRouteInputs::new(
        Some(RouteSourceRows::new(SourceRecords::of(&source), vec![2, 0])),
        north_state(),
    ))
    .expect("one source row for each published row");

    for (row, order_id, amount, reference) in [(0, "n2", 9, "inv-n2"), (1, "n1", 7, "inv-n1")] {
        let input = batch
            .rejected_record_input(row)
            .expect("a published row has the source row it was built from");
        assert_eq!(text(&input.record, "order_id"), order_id);
        assert_eq!(
            input.record.value("amount").expect("readable amount"),
            Some(RuntimeValue::I64(amount)),
            "the error route reads a field only the source row has"
        );
        let built = input
            .partial_output
            .expect("the error route reads the record the route built");
        assert_eq!(built.batch().num_rows(), 1);
        let field = built
            .batch()
            .schema()
            .field_with_name("reference")
            .expect("the built record keeps its fields")
            .clone();
        assert!(
            field.is_nullable(),
            "every partial_output field is optional"
        );
        let references = built
            .batch()
            .column_by_name("reference")
            .expect("the built record has its reference")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("a reference is text")
            .value(0)
            .to_string();
        assert_eq!(references, reference);
        assert_eq!(input.materialized_state, north_state());
    }
    let Err(missing) = batch.rejected_record_input(2) else {
        panic!("the batch publishes two rows");
    };
    assert_eq!(
        *missing.current_context(),
        EmitterRuntimeError::MissingSourceRow { row: 2 }
    );
    let Err(mismatch) =
        EmitterPublishBatch::from_batch(invoices(&["inv-n1"]), Timestamp::from_unix_nanos(100))
            .with_error_route_inputs(ErrorRouteInputs::new(
                Some(RouteSourceRows::new(SourceRecords::of(&source), vec![0, 2])),
                HashMap::default(),
            ))
    else {
        panic!("every published row needs exactly one source row");
    };
    assert_eq!(
        *mismatch.current_context(),
        EmitterRuntimeError::SourceRowCountMismatch {
            source_rows: 2,
            row_count: 1,
        }
    );
}

#[test]
fn a_row_its_route_did_not_build_reads_itself_with_the_state_its_emitter_kept() {
    let published = orders(&[("n1", 7)]);
    let keeps_nothing =
        EmitterPublishBatch::from_batch(published.clone(), Timestamp::from_unix_nanos(100));
    let input = keeps_nothing
        .rejected_record_input(0)
        .expect("the row is its own source row");
    assert_eq!(text(&input.record, "order_id"), "n1");
    assert!(input.partial_output.is_none());
    assert!(input.materialized_state.is_empty());

    let keeps_state = EmitterPublishBatch::from_batch(published, Timestamp::from_unix_nanos(100))
        .with_error_route_inputs(ErrorRouteInputs::new(None, north_state()))
        .expect("state needs no source rows");
    let input = keeps_state
        .rejected_record_input(0)
        .expect("the row is its own source row");
    assert_eq!(text(&input.record, "order_id"), "n1");
    assert!(
        input.partial_output.is_none(),
        "a route that built nothing offers no partial_output"
    );
    assert_eq!(input.materialized_state, north_state());
    assert!(ErrorRouteInputs::new(None, HashMap::default()).is_empty());
}

#[test]
fn a_batch_failed_as_a_whole_hands_every_unresolved_row_its_source_row() {
    let source = orders(&[("n1", 7), ("s1", 8), ("n2", 9)]);
    let mut batch = EmitterPublishBatch::from_batch(
        invoices(&["inv-n1", "inv-s1", "inv-n2"]),
        Timestamp::from_unix_nanos(100),
    )
    .with_error_route_inputs(ErrorRouteInputs::new(
        Some(RouteSourceRows::new(
            SourceRecords::of(&source),
            vec![0, 1, 2],
        )),
        north_state(),
    ))
    .expect("one source row for each published row");
    batch
        .mark_delivered(1, DeliveredAcknowledgements::Host)
        .expect("the second row can be delivered");

    let inputs = batch
        .unresolved_row_inputs()
        .expect("every unresolved row has its source row");

    assert_eq!(inputs.len(), 3, "one entry for each row, in row order");
    assert!(inputs[1].is_none(), "a delivered row is not failed again");
    for (input, order_id) in [(&inputs[0], "n1"), (&inputs[2], "n2")] {
        let input = input.as_ref().expect("an unresolved row is failed");
        assert_eq!(text(&input.record, "order_id"), order_id);
        assert!(input.partial_output.is_some());
        assert_eq!(input.materialized_state, north_state());
    }
}

#[test]
fn what_a_batch_keeps_for_its_error_route_has_a_flush_allowance_of_its_own() {
    let source = orders(&[("n1", 7), ("s1", 8)]);
    let published = invoices(&["inv-1", "inv-2"]);
    let state_bytes: u64 = ("relay_state.settings.label".len() + "north-ready".len()).arch_into();
    let plain = EmitterPublishBatch::from_batch(published.clone(), Timestamp::from_unix_nanos(100));
    let keeping = EmitterPublishBatch::from_batch(published, Timestamp::from_unix_nanos(100))
        .with_error_route_inputs(ErrorRouteInputs::new(
            Some(RouteSourceRows::new(SourceRecords::of(&source), vec![0, 1])),
            north_state(),
        ))
        .expect("one source row for each published row");

    assert_eq!(plain.retained_bytes(), 0);
    assert_eq!(
        keeping.retained_bytes(),
        source.estimated_bytes() + state_bytes,
        "the whole source batch stays alive, with the state snapshot"
    );
    assert_eq!(
        keeping.estimated_bytes(),
        plain.estimated_bytes(),
        "what the batch keeps for its error route is not what it publishes"
    );
    let mut sent_plain = plain.clone();
    let mut sent_keeping = keeping.clone();
    for row in 0..2 {
        sent_plain
            .mark_delivered(row, DeliveredAcknowledgements::Host)
            .expect("every row can be delivered");
        sent_keeping
            .mark_delivered(row, DeliveredAcknowledgements::Host)
            .expect("every row can be delivered");
    }
    assert_eq!(
        sent_keeping.delivered_report().map(|report| report.bytes),
        sent_plain.delivered_report().map(|report| report.bytes),
        "the sent bytes count only the published records"
    );

    // The bound is reached by what the batches keep for their error routes, while what they
    // publish stays below it.
    assert!(keeping.estimated_bytes() * 2 < keeping.retained_bytes());
    let context = emitter_context();
    let mut buffer = EmitterBatchBuffer::default();
    buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
        interval: Duration::from_secs(60),
        max_batch_size: keeping.retained_bytes(),
    });
    assert!(
        !buffer.push(&context, plain).expect("the batch must buffer"),
        "a batch that keeps nothing counts only its published bytes"
    );
    assert!(
        buffer
            .push(&context, keeping)
            .expect("the batch must buffer"),
        "what the batches keep reached the bound"
    );
    assert_eq!(
        buffer.retained_bytes,
        source.estimated_bytes() + state_bytes
    );
    let drained = buffer.drain_pending();
    assert_eq!(drained.len(), 2);
    assert_eq!(buffer.retained_bytes, 0);
    assert_eq!(buffer.pending_bytes, 0);
}

#[test]
fn a_kept_state_snapshot_counts_its_names_and_the_data_of_its_values() {
    let state = HashMap::from_iter([
        ("relay_state.a.flag".to_string(), RuntimeValue::Bool(true)),
        ("relay_state.a.small".to_string(), RuntimeValue::U16(7)),
        ("relay_state.a.count".to_string(), RuntimeValue::I32(-3)),
        ("relay_state.a.total".to_string(), RuntimeValue::I64(9)),
        (
            "relay_state.a.seen".to_string(),
            RuntimeValue::Datetime(
                chrono::DateTime::parse_from_rfc3339("2026-11-26T00:00:00Z")
                    .expect("the test datetime parses"),
            ),
        ),
        (
            "relay_state.a.tags".to_string(),
            RuntimeValue::Vec(vec![
                RuntimeValue::String("ab".to_string()),
                RuntimeValue::String("c".to_string()),
            ]),
        ),
        (
            "relay_state.a.pair".to_string(),
            RuntimeValue::Array(vec![
                RuntimeValue::U8(1),
                RuntimeValue::F64(OrderedFloat(2.5)),
            ]),
        ),
    ]);
    let names: usize = state.keys().map(String::len).sum();
    // A flag, a 16-bit, a 32-bit, a 64-bit and a datetime value, two strings and a pair.
    let values: usize = 1 + 2 + 4 + 8 + 8 + (2 + 1) + (1 + 8);
    let expected: u64 = (names + values).arch_into();
    assert_eq!(state_bytes(&state), expected);

    let kept = ErrorRouteInputs::new(None, state);
    assert!(!kept.is_empty());
    assert_eq!(
        kept.retained_bytes(),
        expected,
        "rows that are their own source rows keep only the state"
    );
}
