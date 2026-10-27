//! Regression evidence for Nervix model-mutation commands.
//!
//! Layer: test harness.
//!
//! - **Owns.** Command results, exact replay and committed model-mutation regressions.
//! - **Depends on.** The session adapter, test clusters, typed Models and Plexus parsing.
//! - **Must not know.** Listener framing or connector transport internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use nervix_client_wire::CommandRequest;
use nervix_models::{
    AckMode, CreateDeduplicator, CreateEmitter, CreateJunction, CreateRelay, CreateSchema,
    DomainName, Model, ModelName, Statement, TransactionLifecycle,
};

use super::super::{
    subscription::SessionSubscriptions,
    test_fixtures::{
        TestService, build_test_service, command_transaction_state, create_test_domain, named,
        test_command_request, test_execution_reference,
    },
};

#[nervix_primitives::test]
async fn process_command_create_if_not_exists_returns_already_existed_for_models() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();

    let first = service
        .test_command(
            test_command_request(
                "CREATE IF NOT EXISTS SCHEMA notification ( user_id U32 );",
                "default",
            ),
            &mut subscriptions,
        )
        .await;
    assert!(first.succeeded());
    assert!(!first.found_existing());

    let duplicate = service
        .test_command(
            test_command_request(
                "CREATE IF NOT EXISTS SCHEMA notification ( user_id U32 );",
                "default",
            ),
            &mut subscriptions,
        )
        .await;
    assert!(duplicate.succeeded());
    assert!(duplicate.found_existing());
    assert!(duplicate.message.contains("already exists"));

    let schema = registry
        .get::<CreateSchema>(
            &DomainName::parse("default").expect("valid domain"),
            named::<ModelName>("notification"),
        )
        .expect("registry get should succeed")
        .expect("schema should exist");
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].name.as_str(), "user_id");

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn persistent_model_command_replays_one_terminal_result_for_its_reference() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    let reference = test_execution_reference();
    let request = CommandRequest {
        execution_reference: reference.clone(),
        ..test_command_request("CREATE SCHEMA retained_result ( user_id U32 );", "default")
    };

    let first = service
        .test_command(request.clone(), &mut subscriptions)
        .await;
    assert!(first.succeeded(), "{first:?}");
    let replay = service.test_command(request, &mut subscriptions).await;
    assert_eq!(replay, first);

    let changed = service
        .test_command(
            CommandRequest {
                execution_reference: reference,
                ..test_command_request("CREATE SCHEMA retained_result ( user_id U64 );", "default")
            },
            &mut subscriptions,
        )
        .await;
    assert!(!changed.succeeded());
    assert!(changed.message.contains("conflicts by content"));
    assert_eq!(
        service.inner.consensus.current_transactions().await.len(),
        1
    );

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn process_command_rejects_implicit_semicolon_batch() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(false).await;
    let mut subscriptions = SessionSubscriptions::new();

    let result = service
        .test_command(
            test_command_request(
                "CREATE DOMAIN prod; CREATE SCHEMA notification ( user_id U32 )",
                "prod",
            ),
            &mut subscriptions,
        )
        .await;

    assert!(!result.succeeded());
    assert_eq!(result.message, "multiple commands require BEGIN");
    assert_eq!(command_transaction_state(&result), None);
    assert!(
        registry
            .get::<CreateSchema>(
                &DomainName::parse("prod").expect("valid domain"),
                named::<ModelName>("notification"),
            )
            .expect("registry get should succeed")
            .is_none(),
        "implicit batch must not create later models"
    );

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn process_command_batch_returns_prior_successes_before_error() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(false).await;
    create_test_domain(&service.inner.consensus, "prod").await;
    let mut subscriptions = SessionSubscriptions::new();

    let result = service
        .test_command(
            test_command_request(
                "BEGIN; CREATE SCHEMA duplicated ( user_id U32 ); CREATE SCHEMA duplicated ( \
                 user_id U32 ); COMMIT",
                "prod",
            ),
            &mut subscriptions,
        )
        .await;

    assert!(!result.succeeded());
    assert_eq!(
        command_transaction_state(&result),
        Some(TransactionLifecycle::Open)
    );
    assert!(result.message.contains("transaction started"));
    assert!(result.message.contains("already exists"));
    assert!(
        registry
            .get::<CreateSchema>(
                &DomainName::parse("prod").expect("valid domain"),
                named::<ModelName>("duplicated"),
            )
            .expect("registry get should succeed")
            .is_none(),
        "queue preflight failure must not execute the admitted prefix"
    );
    let transaction = service
        .inner
        .consensus
        .current_transaction(
            subscriptions
                .transaction_id()
                .expect("failed queue preflight must leave the transaction attached"),
        )
        .await
        .expect("open transaction must remain replicated");
    assert_eq!(transaction.pending_statement_count(), 1);

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn process_command_model_create_batch_is_atomic_on_registry_failure() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(false).await;
    create_test_domain(&service.inner.consensus, "prod").await;
    let mut subscriptions = SessionSubscriptions::new();

    let result = service
        .test_command(
            test_command_request(
                "BEGIN; CREATE RELAY notifications SCHEMA missing_schema UNBRANCHED; CREATE \
                 SCHEMA notification ( user_id U32 ); COMMIT",
                "prod",
            ),
            &mut subscriptions,
        )
        .await;

    assert!(!result.succeeded());
    assert!(matches!(
        command_transaction_state(&result),
        Some(TransactionLifecycle::Failed { .. })
    ));

    let domain = DomainName::parse("prod").expect("valid domain");
    let relay = registry
        .get::<CreateRelay>(&domain, named::<ModelName>("notifications"))
        .expect("registry get should succeed");
    assert!(relay.is_none(), "failed model batch must not persist relay");
    let schema = registry
        .get::<CreateSchema>(&domain, named::<ModelName>("notification"))
        .expect("registry get should succeed");
    assert!(
        schema.is_none(),
        "failed model batch must not persist schema"
    );

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn process_command_preserves_detached_deduplicator_and_emitter_modes() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    let commands = [
        "CREATE SCHEMA notification ( user_id I64 );",
        "CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );",
        "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
         notification;",
        "CREATE RELAY notifications SCHEMA notification UNBRANCHED;",
        "CREATE RELAY forwarded_notifications SCHEMA notification UNBRANCHED;",
        "CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };",
        "CREATE KAFKA TOPIC INGESTOR notifications_ingestor FROM kafka_main TOPIC 'notifications' \
         OFFSET BY CONSUMER GROUP 'notifications_group' MODE NO ACK ON QUIESCE SUSPEND DECODE \
         USING notification_codec TIMESTAMP NOW TO notifications INHERIT ALL UNBRANCHED FLUSH \
         EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        "CREATE DETACHED DEDUPLICATOR passthrough FROM notifications DEDUPLICATE ON input.user_id \
         MAX TIME 10m UNBRANCHED TO forwarded_notifications INHERIT ALL FLUSH EACH 100ms MAX \
         BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        "CREATE DETACHED KAFKA TOPIC EMITTER kafka_forward FROM notifications TO kafka_main TOPIC \
         'notifications_out' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING \
         notification_codec INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG \
         ON GENERAL ERROR LOG;",
    ];

    for command in commands {
        nervix_primitives::task::consume_budget().await;
        let result = service
            .test_command(test_command_request(command, "default"), &mut subscriptions)
            .await;
        assert!(
            result.succeeded(),
            "command must succeed: {command}: {}",
            result.message
        );
    }

    let deduplicator = registry
        .get::<CreateDeduplicator>(
            &DomainName::parse("default").expect("valid domain"),
            ModelName::parse("passthrough").expect("valid model name"),
        )
        .expect("registry get should succeed")
        .expect("deduplicator should exist");
    let emitter = registry
        .get::<CreateEmitter>(
            &DomainName::parse("default").expect("valid domain"),
            ModelName::parse("kafka_forward").expect("valid model name"),
        )
        .expect("registry get should succeed")
        .expect("emitter should exist");

    assert_eq!(deduplicator.mode, AckMode::Detached);
    assert_eq!(emitter.mode, AckMode::Detached);

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn process_command_creates_junction_model() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    for command in [
        "CREATE SCHEMA notification ( user_id I64 );",
        "CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );",
        "CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA \
         notification;",
        "CREATE RELAY notifications_a SCHEMA notification UNBRANCHED;",
        "CREATE RELAY notifications_b SCHEMA notification UNBRANCHED;",
        "CREATE RELAY notifications_all SCHEMA notification UNBRANCHED;",
        "CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };",
        "CREATE KAFKA TOPIC INGESTOR ingest_a FROM kafka_main TOPIC 'notifications_a' OFFSET BY \
         CONSUMER GROUP 'notifications_a_group' MODE NO ACK ON QUIESCE SUSPEND DECODE USING \
         notification_codec TIMESTAMP NOW TO notifications_a INHERIT ALL UNBRANCHED FLUSH EACH \
         100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        "CREATE KAFKA TOPIC INGESTOR ingest_b FROM kafka_main TOPIC 'notifications_b' OFFSET BY \
         CONSUMER GROUP 'notifications_b_group' MODE NO ACK ON QUIESCE SUSPEND DECODE USING \
         notification_codec TIMESTAMP NOW TO notifications_b INHERIT ALL UNBRANCHED FLUSH EACH \
         100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        "CREATE JUNCTION join_streams FROM notifications_a, notifications_b UNBRANCHED TO \
         notifications_all INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
    ] {
        nervix_primitives::task::consume_budget().await;
        let result = service
            .test_command(test_command_request(command, "default"), &mut subscriptions)
            .await;
        assert!(
            result.succeeded(),
            "command must succeed: {command}: {}",
            result.message
        );
    }

    let junction = registry
        .get::<CreateJunction>(
            &DomainName::parse("default").expect("valid domain"),
            ModelName::parse("join_streams").expect("valid model name"),
        )
        .expect("registry get should succeed")
        .expect("junction should exist");
    assert_eq!(junction.from.relays().len(), 2);
    assert_eq!(
        junction
            .output_routes
            .relays()
            .next()
            .expect("junction should declare an output")
            .as_str(),
        "notifications_all"
    );

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn process_command_creates_deduplicator_model() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    for command in [
        "CREATE SCHEMA transaction ( transaction_id STRING, amount I64 );",
        "CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT ( transaction_id string, amount \
         integer );",
        "CREATE CODEC transaction_codec FROM WIRE JSON SCHEMA transaction_wire TO SCHEMA \
         transaction;",
        "CREATE RELAY inbound SCHEMA transaction UNBRANCHED;",
        "CREATE RELAY deduped SCHEMA transaction UNBRANCHED;",
        "CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };",
        "CREATE KAFKA TOPIC INGESTOR inbound_ingestor FROM kafka_main TOPIC 'inbound' OFFSET BY \
         CONSUMER GROUP 'inbound_group' MODE NO ACK ON QUIESCE SUSPEND DECODE USING \
         transaction_codec TIMESTAMP NOW TO inbound INHERIT ALL UNBRANCHED FLUSH EACH 100ms MAX \
         BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        "CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id MAX TIME \
         10m UNBRANCHED TO deduped INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE \
         ERROR LOG;",
    ] {
        nervix_primitives::task::consume_budget().await;
        let result = service
            .test_command(test_command_request(command, "default"), &mut subscriptions)
            .await;
        assert!(
            result.succeeded(),
            "command must succeed: {command}: {}",
            result.message
        );
    }

    let deduplicator = registry
        .get::<CreateDeduplicator>(
            &DomainName::parse("default").expect("valid domain"),
            ModelName::parse("dedup_txns").expect("valid model name"),
        )
        .expect("registry get should succeed")
        .expect("deduplicator should exist");
    assert_eq!(
        deduplicator
            .from
            .first()
            .expect("deduplicator should declare an input")
            .as_str(),
        "inbound"
    );
    assert_eq!(
        deduplicator
            .output_routes
            .relays()
            .next()
            .expect("deduplicator should declare an output")
            .as_str(),
        "deduped"
    );
    assert_eq!(
        deduplicator.deduplicate_on,
        vec![nervix_plexus::parse_expression("input.transaction_id").expect("valid expression")]
    );
    assert_eq!(deduplicator.max_time, "10m");
    assert_eq!(deduplicator.mode, nervix_models::AckMode::Attached);

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn parse_server_statement_accepts_junction_from_application_crate() {
    let parsed = nervix_plexus::server_statement::parse_server_statement(
        "CREATE JUNCTION join_streams FROM ss1, ss2 UNBRANCHED TO ss10 INHERIT ALL FLUSH EACH \
         100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
    )
    .expect("junction statement should parse");
    let Statement::Create(model) = parsed else {
        panic!("expected create statement");
    };
    assert!(matches!(model.body.as_ref(), Model::Junction(_)));
}

#[test]
fn parse_server_statement_accepts_deduplicator_from_application_crate() {
    let parsed = nervix_plexus::server_statement::parse_server_statement(
        "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.transaction_id MAX TIME 10m \
         UNBRANCHED TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
    )
    .expect("deduplicator statement should parse");
    let Statement::Create(model) = parsed else {
        panic!("expected create statement");
    };
    assert!(matches!(model.body.as_ref(), Model::Deduplicator(_)));
}
