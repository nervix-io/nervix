use meticulous::ResultExt as _;

use crate::{
    AckMode, AlterEmitter, AlterEmitterError, AlterEmitterOperation, ClickHouseEmitter,
    ClickHouseTableEmitter, CreateEmitter, EmitterBatchContractError, EmitterBody, EmitterEntity,
    ErrorPolicies, Expression, FlushPolicy, HttpEmitter, HttpRequestEmitter, KafkaEmitter,
    KafkaTopicEmitter, Literal, MongoDbCollectionEmitter, MongoDbConflictAction, MongoDbEmitter,
    MySqlConflictAction, MySqlEmitter, MySqlTableEmitter, PostgresConflictAction, PostgresEmitter,
    PostgresTableEmitter, ProcessorInputs, RetryPolicy, SentryEmitter, SentryEventEmitter,
    SqsEmitter, SqsQueueEmitter, ZeroMqEmitter, ZeroMqPushEmitter,
};

fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).expect("valid name")
}

#[test]
fn ack_mode_default_is_attached() {
    assert_eq!(AckMode::default(), AckMode::Attached);
}

#[test]
fn emitter_alter_applies_operations_in_order_and_is_atomic() {
    let mut emitter = CreateEmitter {
        name: named("event_emitter"),
        from: ProcessorInputs::single(named("events")),
        body: EmitterBody::Codec {
            codec: named("event_codec"),
        },
        envelope: Box::new(crate::EmitterEnvelope::default()),
        connector: Box::new(EmitterEntity::ZeroMq(ZeroMqEmitter::Push(
            ZeroMqPushEmitter {
                mode: crate::ZeroMqPushPublishingMode::NoAck {
                    retry_policy: RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("emitter_a"),
            },
        ))),
        batch: None,
        flush_policy: FlushPolicy::Each {
            interval: "1s".to_string(),
            max_batch_size: "1MiB".to_string(),
        },
        error_policies: ErrorPolicies::handled_by_log(),
        mode: AckMode::Attached,
        construction: crate::RouteConstruction::default(),
        materialized_state: Vec::new(),
    };
    emitter
        .apply_alter(&AlterEmitter {
            kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
            emitter: named("event_emitter"),
            operations: vec![
                AlterEmitterOperation::AddFrom {
                    relay: named("backup_events"),
                    where_clause: Some(Expression::Literal(Literal::Bool(true))),
                },
                AlterEmitterOperation::AlterFromDropWhere {
                    relay: named("backup_events"),
                },
                AlterEmitterOperation::SetClient {
                    client: named("emitter_b"),
                },
                AlterEmitterOperation::SetFlush {
                    flush_policy: FlushPolicy::Each {
                        interval: "2s".to_string(),
                        max_batch_size: "2MiB".to_string(),
                    },
                },
                AlterEmitterOperation::SetFlush {
                    flush_policy: FlushPolicy::Immediate,
                },
                AlterEmitterOperation::SetAttachment {
                    mode: AckMode::Detached,
                },
            ],
        })
        .expect("emitter alter should apply");
    assert_eq!(emitter.connector.client(), Some(&named("emitter_b")));
    assert_eq!(emitter.flush_policy, FlushPolicy::Immediate);
    assert_eq!(emitter.mode, AckMode::Detached);
    assert_eq!(
        emitter.from.relays(),
        &[named("events"), named("backup_events")]
    );
    assert!(emitter.from.where_clauses().is_empty());

    let before = emitter.clone();
    let error = emitter
        .apply_alter(&AlterEmitter {
            kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
            emitter: named("event_emitter"),
            operations: vec![
                AlterEmitterOperation::SetClient {
                    client: named("emitter_c"),
                },
                AlterEmitterOperation::DropEncode,
                AlterEmitterOperation::DropEncode,
            ],
        })
        .expect_err("the second codec drop must fail");
    assert_eq!(
        error.current_context(),
        &AlterEmitterError::EncodeNotConfigured
    );
    assert_eq!(emitter, before, "failed ALTER must not partially apply");
}

#[test]
fn http_emitter_alter_replaces_body_explicitly_and_rejects_drop_encode() {
    let retry_policy = RetryPolicy {
        backoff: "250ms".to_string(),
        max_backoff: "30s".to_string(),
    };
    let mut emitter = CreateEmitter {
        name: named("request_emitter"),
        from: ProcessorInputs::single(named("events")),
        body: EmitterBody::WithoutBody,
        envelope: Box::new(crate::EmitterEnvelope::default()),
        connector: Box::new(EmitterEntity::Http(HttpEmitter::Request(
            HttpRequestEmitter {
                mode: crate::HttpRequestPublishingMode::Ack {
                    retry_policy: retry_policy.clone(),
                },
                client: named("api"),
                method: Expression::Literal(Literal::String("DELETE".to_string())),
                path: Expression::Literal(Literal::String("/events".to_string())),
            },
        ))),
        batch: None,
        flush_policy: FlushPolicy::Immediate,
        error_policies: ErrorPolicies::handled_by_log(),
        mode: AckMode::Attached,
        construction: crate::RouteConstruction::default(),
        materialized_state: Vec::new(),
    };
    let replacement = AlterEmitter {
        kind: crate::EmitterEntityKind::Http(crate::HttpEmitterEntity::Request),
        emitter: named("request_emitter"),
        operations: vec![AlterEmitterOperation::SetTo {
            destination: Box::new(EmitterEntity::Http(HttpEmitter::Request(
                HttpRequestEmitter {
                    mode: crate::HttpRequestPublishingMode::Ack { retry_policy },
                    client: named("other_api"),
                    method: Expression::Literal(Literal::String("HEAD".to_string())),
                    path: Expression::Literal(Literal::String("/health".to_string())),
                },
            ))),
            body: EmitterBody::WithoutBody,
            envelope: Box::new(crate::EmitterEnvelope::default()),
            construction: None,
        }],
    };
    emitter
        .apply_alter(&replacement)
        .expect("complete HTTP replacement");
    assert_eq!(emitter.body, EmitterBody::WithoutBody);
    assert_eq!(emitter.connector.client(), Some(&named("other_api")));
    let stored = serde_json::to_vec(&emitter).expect("current emitter should serialize");
    let restored: CreateEmitter =
        serde_json::from_slice(&stored).expect("current emitter should deserialize");
    assert_eq!(restored, emitter);

    let before = emitter.clone();
    let error = emitter.apply_alter(&AlterEmitter {
        kind: crate::EmitterEntityKind::Http(crate::HttpEmitterEntity::Request),
        emitter: named("request_emitter"),
        operations: vec![
            AlterEmitterOperation::SetClient {
                client: named("third_api"),
            },
            AlterEmitterOperation::DropEncode,
        ],
    });
    assert_eq!(
        error
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterEmitterError::BodySelectionRequired {
            entity: crate::EmitterEntityKind::Http(crate::HttpEmitterEntity::Request),
        }
    );
    assert_eq!(emitter, before);

    let error = emitter
        .apply_alter(&AlterEmitter {
            kind: crate::EmitterEntityKind::Http(crate::HttpEmitterEntity::Request),
            emitter: named("request_emitter"),
            operations: vec![AlterEmitterOperation::SetBatch {
                policy: batch_policy(10, "64KiB"),
            }],
        })
        .expect_err("an HTTP emitter sends one request per record");
    let unavailable = EmitterBatchContractError::Unavailable {
        connector: "HTTP REQUEST",
        reason: crate::BatchUnavailability::OneRequestPerRecord,
    };
    assert_eq!(
        error.current_context(),
        &AlterEmitterError::Batch(unavailable.clone())
    );
    assert_eq!(
        error.current_context().to_string(),
        "HTTP REQUEST emitters send one request per record and do not support BATCH"
    );
    assert_eq!(emitter, before);
    let batched = CreateEmitter {
        batch: Some(batch_policy(10, "64KiB")),
        ..emitter
    };
    assert_eq!(batch_violation(&batched), Some(unavailable));
}

fn batch_policy(max_messages: u32, max_size: &str) -> crate::EmitterBatchPolicy {
    crate::EmitterBatchPolicy {
        max_messages: crate::BatchMessageLimit::try_from(max_messages)
            .assured("the fixture message limit is within range"),
        max_size: max_size
            .parse()
            .assured("the fixture size is a whole number of bytes"),
    }
}

fn batching_emitter(
    connector: EmitterEntity,
    batch: Option<crate::EmitterBatchPolicy>,
) -> CreateEmitter {
    CreateEmitter {
        name: named("event_emitter"),
        from: ProcessorInputs::single(named("events")),
        body: EmitterBody::Values,
        envelope: Box::new(crate::EmitterEnvelope::default()),
        connector: Box::new(connector),
        batch,
        flush_policy: FlushPolicy::Immediate,
        error_policies: ErrorPolicies::handled_by_log(),

        mode: AckMode::Attached,
        construction: crate::RouteConstruction::default(),
        materialized_state: Vec::new(),
    }
}

fn postgres_emitter() -> EmitterEntity {
    EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
        mode: crate::PostgresTablePublishingMode::Ack {
            retry_policy: crate::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("db"),
        table: named("events"),
        values: Vec::new(),
        conflict_action: PostgresConflictAction::None,
    }))
}

#[test]
fn emitter_alter_sets_replaces_and_drops_the_batch_clause() {
    let mut emitter = batching_emitter(
        EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
            mode: crate::ZeroMqPushPublishingMode::NoAck {
                retry_policy: crate::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named("emitter"),
        })),
        None,
    );
    let set = |policy| AlterEmitter {
        kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
        emitter: named("event_emitter"),
        operations: vec![AlterEmitterOperation::SetBatch { policy }],
    };

    emitter
        .apply_alter(&set(batch_policy(500, "1MiB")))
        .expect("SET BATCH adds the clause");
    assert_eq!(emitter.batch, Some(batch_policy(500, "1MiB")));
    emitter
        .apply_alter(&set(batch_policy(10, "64KiB")))
        .expect("SET BATCH replaces the clause");
    assert_eq!(emitter.batch, Some(batch_policy(10, "64KiB")));

    let drop = AlterEmitter {
        kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
        emitter: named("event_emitter"),
        operations: vec![AlterEmitterOperation::DropBatch],
    };
    emitter.apply_alter(&drop).expect("DROP BATCH removes it");
    assert_eq!(emitter.batch, None);
    assert_eq!(
        emitter
            .apply_alter(&drop)
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterEmitterError::BatchNotConfigured
    );
}

#[test]
fn emitter_alter_keeps_the_batch_clause_a_database_emitter_requires() {
    let base = batching_emitter(postgres_emitter(), Some(batch_policy(500, "8MiB")));

    let mut dropped = base.clone();
    let error = dropped
        .apply_alter(&AlterEmitter {
            kind: crate::EmitterEntityKind::Postgres(crate::PostgresEmitterEntity::Table),
            emitter: named("event_emitter"),
            operations: vec![AlterEmitterOperation::DropBatch],
        })
        .expect_err("a Postgres emitter cannot drop its batching clause");
    assert_eq!(
        error.current_context(),
        &AlterEmitterError::Batch(EmitterBatchContractError::Required {
            connector: "POSTGRES TABLE"
        })
    );
    assert_eq!(dropped, base, "a failed ALTER must not partially apply");

    let mut unbatched = batching_emitter(postgres_emitter(), None);
    let replacement = AlterEmitterOperation::SetTo {
        destination: Box::new(postgres_emitter()),
        body: EmitterBody::Values,
        envelope: Box::new(crate::EmitterEnvelope::default()),
        construction: None,
    };
    let alter = |operations| AlterEmitter {
        kind: crate::EmitterEntityKind::Postgres(crate::PostgresEmitterEntity::Table),
        emitter: named("event_emitter"),
        operations,
    };
    assert_eq!(
        unbatched
            .clone()
            .apply_alter(&alter(vec![replacement.clone()]))
            .expect_err("batch required")
            .current_context(),
        &AlterEmitterError::Batch(EmitterBatchContractError::Required {
            connector: "POSTGRES TABLE"
        })
    );
    unbatched
        .apply_alter(&alter(vec![
            replacement,
            AlterEmitterOperation::SetBatch {
                policy: batch_policy(100, "1MiB"),
            },
        ]))
        .expect("replacement and batch are atomic");
    assert_eq!(unbatched.batch, Some(batch_policy(100, "1MiB")));
}

/// The contract violation `emitter` reports, or none when its batching clause is valid.
fn batch_violation(emitter: &CreateEmitter) -> Option<EmitterBatchContractError> {
    match emitter.validate_batch() {
        Ok(()) => None,
        Err(report) => Some(report.current_context().clone()),
    }
}

#[test]
fn batch_size_is_bounded_by_the_largest_sqs_message() {
    let sqs = || {
        EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
            mode: crate::SqsQueuePublishingMode::Single {
                retry_policy: crate::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named("queue_client"),
            queue: crate::SqsQueueName::parse("events").expect("valid queue"),
            fifo_group: None,
        }))
    };

    assert_eq!(
        batch_violation(&batching_emitter(sqs(), Some(batch_policy(10, "256KiB")))),
        None
    );
    assert_eq!(
        batch_violation(&batching_emitter(sqs(), Some(batch_policy(10, "262145B")))),
        Some(EmitterBatchContractError::SizeAboveEmitterMaximum {
            connector: "SQS QUEUE",
            declared: "262145B".parse().assured("a whole number of bytes"),
            maximum: "256KiB".parse().assured("a whole number of bytes"),
        })
    );
    let kafka = EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
        mode: crate::KafkaTopicPublishingMode::NoAck {
            retry_policy: crate::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("broker"),
        topic: named("events"),
    }));
    assert_eq!(
        batch_violation(&batching_emitter(kafka, Some(batch_policy(10, "1GiB")))),
        None
    );
}

#[test]
fn database_emitters_require_the_batch_clause_and_the_others_do_not() {
    let database_emitters = [
        EmitterEntity::ClickHouse(ClickHouseEmitter::Table(ClickHouseTableEmitter {
            mode: crate::ClickHouseTablePublishingMode::Ack {
                retry_policy: crate::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named("db"),
            table: named("events"),
            values: Vec::new(),
        })),
        postgres_emitter(),
        EmitterEntity::MySql(MySqlEmitter::Table(MySqlTableEmitter {
            mode: crate::MySqlTablePublishingMode::Ack {
                retry_policy: crate::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named("db"),
            table: named("events"),
            values: Vec::new(),
            conflict_action: MySqlConflictAction::None,
        })),
        EmitterEntity::MongoDb(MongoDbEmitter::Collection(MongoDbCollectionEmitter {
            mode: crate::MongoDbCollectionPublishingMode::Ack {
                retry_policy: crate::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: named("db"),
            collection: named("events"),
            values: Vec::new(),
            conflict_action: MongoDbConflictAction::None,
        })),
    ];
    for connector in database_emitters {
        let label = connector.transport_label();
        assert_eq!(
            batch_violation(&batching_emitter(connector.clone(), None)),
            Some(EmitterBatchContractError::Required { connector: label })
        );
        assert_eq!(
            batch_violation(&batching_emitter(connector, Some(batch_policy(1, "1B")))),
            None
        );
    }
    let sentry = EmitterEntity::Sentry(SentryEmitter::Event(SentryEventEmitter {
        mode: crate::SentryEventPublishingMode::Ack {
            retry_policy: crate::RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: named("sentry"),
        platform: None,
    }));
    assert_eq!(batch_violation(&batching_emitter(sentry, None)), None);
}

#[test]
fn emitter_alter_reports_name_and_commit_policy_errors() {
    let emitter = CreateEmitter {
        name: named("event_emitter"),
        from: ProcessorInputs::single(named("events")),
        body: EmitterBody::Codec {
            codec: named("event_codec"),
        },
        envelope: Box::new(crate::EmitterEnvelope::default()),
        connector: Box::new(EmitterEntity::ZeroMq(ZeroMqEmitter::Push(
            ZeroMqPushEmitter {
                mode: crate::ZeroMqPushPublishingMode::NoAck {
                    retry_policy: RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("emitter"),
            },
        ))),
        batch: None,
        flush_policy: FlushPolicy::Immediate,
        error_policies: ErrorPolicies::handled_by_log(),
        mode: AckMode::Attached,
        construction: crate::RouteConstruction::default(),
        materialized_state: Vec::new(),
    };
    let cases = [
        (
            AlterEmitter {
                kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
                emitter: named("other"),
                operations: Vec::new(),
            },
            AlterEmitterError::EmitterNameMismatch {
                stored: named("event_emitter"),
                requested: named("other"),
            },
        ),
        (
            AlterEmitter {
                kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
                emitter: named("event_emitter"),
                operations: vec![AlterEmitterOperation::SetCommit {
                    commit_each: "1m".to_string(),
                    max_commit_size: "1GiB".parse().expect("valid commit size"),
                }],
            },
            AlterEmitterError::CommitPolicyUnsupported,
        ),
        (
            AlterEmitter {
                kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
                emitter: named("event_emitter"),
                operations: vec![AlterEmitterOperation::AddFrom {
                    relay: named("events"),
                    where_clause: None,
                }],
            },
            AlterEmitterError::InputAlreadyExists {
                relay: named("events"),
            },
        ),
        (
            AlterEmitter {
                kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
                emitter: named("event_emitter"),
                operations: vec![AlterEmitterOperation::DropFrom {
                    relay: named("missing"),
                }],
            },
            AlterEmitterError::InputNotFound {
                relay: named("missing"),
            },
        ),
        (
            AlterEmitter {
                kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
                emitter: named("event_emitter"),
                operations: vec![AlterEmitterOperation::DropFrom {
                    relay: named("events"),
                }],
            },
            AlterEmitterError::CannotDropLastInput,
        ),
        (
            AlterEmitter {
                kind: crate::EmitterEntityKind::ZeroMq(crate::ZeroMqEmitterEntity::Push),
                emitter: named("event_emitter"),
                operations: vec![AlterEmitterOperation::AlterFromDropWhere {
                    relay: named("events"),
                }],
            },
            AlterEmitterError::InputWhereNotConfigured {
                relay: named("events"),
            },
        ),
    ];
    for (alter, expected) in cases {
        let mut candidate = emitter.clone();
        assert_eq!(
            candidate
                .apply_alter(&alter)
                .expect_err("the alteration is rejected")
                .current_context(),
            &expected
        );
        assert_eq!(candidate, emitter);
    }
}

#[test]
fn emitter_alter_checks_the_header_and_complete_destination_kind_atomically() {
    let base = batching_emitter(postgres_emitter(), Some(batch_policy(1, "1MiB")));
    let requested = crate::EmitterEntityKind::Kafka(crate::KafkaEmitterEntity::Topic);
    let mut candidate = base.clone();
    let wrong_head = AlterEmitter {
        kind: requested,
        emitter: base.name.clone(),
        operations: Vec::new(),
    };
    let error = candidate
        .apply_alter(&wrong_head)
        .expect_err("declared kind must match");
    assert_eq!(
        error.current_context(),
        &AlterEmitterError::EmitterKindMismatch {
            stored: base.connector.kind(),
            requested
        }
    );
    assert!(error.to_string().contains("POSTGRES TABLE"));
    assert!(error.to_string().contains("KAFKA TOPIC"));
    assert_eq!(candidate, base);
    let destination = EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
        client: named("broker"),
        topic: named("Orders"),
        mode: crate::KafkaTopicPublishingMode::NoAck {
            retry_policy: RetryPolicy {
                backoff: "1s".to_string(),
                max_backoff: "2s".to_string(),
            },
        },
    }));
    let wrong_destination = AlterEmitter {
        kind: base.connector.kind(),
        emitter: base.name.clone(),
        operations: vec![
            AlterEmitterOperation::SetClient {
                client: named("another_database"),
            },
            AlterEmitterOperation::SetTo {
                destination: Box::new(destination),
                body: EmitterBody::Codec {
                    codec: named("wire"),
                },
                envelope: Box::new(crate::EmitterEnvelope::default()),
                construction: None,
            },
        ],
    };
    let error = candidate
        .apply_alter(&wrong_destination)
        .expect_err("destination kind must match");
    assert_eq!(
        error.current_context(),
        &AlterEmitterError::DestinationKindMismatch {
            stored: base.connector.kind(),
            requested
        }
    );
    assert_eq!(candidate, base);
}
