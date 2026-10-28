//! Public materialized restore workloads with acknowledged input rows and small observable outputs.
//!
//! Layer: test harness.
//! - **Owns.** Plexus graphs, client inputs, archive inventory assertions and generator subscriptions
//!   that qualify many containers and a single container larger than the bulk memory budget,
//!   plus bounded public state observations after an exact-row deadline expires.
//! - **Depends on.** Public commands, acknowledged client producers, Row subscriptions and the
//!   public archive reader.
//! - **Must not know.** Runtime maps, checkpoint storage keys or private restore installation.

use arrow_array::TimestampNanosecondArray;
use nervix_backup::DescribedRuntimeState;
use nervix_client_core::ProducerOutcome;
use nervix_models::{ClientProducerLimits, ClusterNodeName, IngestorName};

use super::*;
use crate::common::backup_phase::BackupPhase;

#[when(expr = "a materialized restore workload with {int} relays is created")]
async fn when_materialized_workload_is_created(world: &mut ScenarioWorld, relays: usize) {
    assert!((1..=40).contains(&relays));
    let domain = &world.domain;
    let mut commands = format!(
        "CREATE UNPACED DOMAIN {domain};
         CREATE SCHEMA event ( tenant STRING, value STRING, round I64, occurred_at DATETIME );
         CREATE SCHEMA tenant_key ( tenant STRING );
         CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
         CREATE SCHEMA summary ( tenant STRING, relay STRING, length I64, round I64 );
         CREATE RELAY summaries SCHEMA summary BRANCHED BY by_tenant;"
    );
    for index in 0..relays {
        commands.push_str(&format!(
            "CREATE RELAY state_{index} SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED \
             STATE LAST BY TIMESTAMP;
             CREATE GENERATOR produce_{index} USING MATERIALIZED STATE state_{index} EACH 100ms \
             BRANCHED BY by_tenant
               TO summaries SET tenant = branch.tenant, relay = \"state_{index}\", length = \
             octet_length(relay_state.state_{index}.value), round = \
             relay_state.state_{index}.round
                 FLUSH IMMEDIATE ON MESSAGE ERROR LOG;"
        ));
    }
    commands.push_str(
        "CREATE CLIENT PRODUCER INGESTOR source FROM SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT \
         30s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND TIMESTAMP AT occurred_at ",
    );
    for index in 0..relays {
        commands.push_str(&format!(
            "TO state_{index} INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG "
        ));
    }
    commands.push_str("ON GENERAL ERROR LOG; START;");
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .assured("the workload leader has a public gRPC endpoint");
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(&world.domain),
        client_connect_options(&grpc_uri).assured("the workload client is configured"),
    )
    .await
    .assured("the workload client connects");
    for command in plexus_statements(&commands) {
        let started = Instant::now();
        append_cucumber_log_line(&format!(
            "materialized workload command started: domain={}, command={command}",
            world.domain
        ));
        let outcome = match client.execute(command.clone()).await {
            Ok(outcome) => outcome,
            Err(error) => panic!(
                "materialized workload command failed after {:?}: domain={}, command={command}: \
                 {error:#}",
                started.elapsed(),
                world.domain
            ),
        };
        assert!(
            outcome.succeeded(),
            "the public materialized workload is valid: {command}: {}",
            outcome.message
        );
        append_cucumber_log_line(&format!(
            "materialized workload command completed: domain={}, elapsed={:?}, command={command}",
            world.domain,
            started.elapsed()
        ));
        world.last_command_output = Some(outcome.message);
    }
    let leader = current_leader_node(world).await;
    let session = crate::execute_plexus_commands_on_node(
        world,
        &leader,
        "CREATE SUBSCRIPTION summary_subscription TO summaries;",
    )
    .await
    .assured("the materialized workload subscription opens");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = true;
}

#[when(expr = "round {int} of {int} KiB materialized rows is submitted for {int} tenants")]
async fn when_large_materialized_round_is_submitted(
    world: &mut ScenarioWorld,
    round: u32,
    kibibytes: usize,
    tenants: usize,
) {
    assert!((1..=1024).contains(&kibibytes));
    assert!((2..=40).contains(&tenants));
    let value = "a".repeat(
        kibibytes
            .checked_mul(1024)
            .assured("bounded row length fits"),
    );
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .assured("the input producer has a public gRPC endpoint");
    let domain = DomainName::parse(&world.domain).assured("the fixture domain name is valid");
    let client = Client::connect_with_options(
        &grpc_uri,
        Some(domain.clone()),
        client_connect_options(&grpc_uri).assured("the input client is configured"),
    )
    .await
    .assured("the input client connects");
    let producer = client
        .open_ingestor(
            domain,
            IngestorName::parse("source").assured("the fixture ingestor name is valid"),
            crate::client_producers::expected_fields(
                "tenant STRING, value STRING, round I64, occurred_at DATETIME",
            ),
            ClientProducerLimits {
                batches: std::num::NonZeroU32::new(1).assured("one batch is nonzero"),
                bytes: NonZeroU64::new(2 * 1024 * 1024)
                    .assured("the bounded row and IPC envelope fit two MiB"),
            },
        )
        .await
        .assured("the materialized input producer opens");
    let phase = BackupPhase::new(
        format!(
            "materialized input domain={}, initial_route={leader}, initial_producer={:?}, \
             round={round}, tenants={tenants}, row_kib={kibibytes}",
            world.domain,
            producer.id(),
        ),
        Duration::from_secs(120),
    );
    let deadline = phase.deadline();
    let domain = world.domain.clone();
    let producer = phase
        .observe(async move {
            for tenant in 0..tenants {
                nervix_primitives::task::consume_budget().await;
                let rows = RecordBatch::try_new(
                    StdArc::new(producer.arrow_schema()),
                    vec![
                        StdArc::new(StringArray::from(vec![format!("restore-tenant-{tenant}")])),
                        StdArc::new(StringArray::from(vec![value.as_str()])),
                        StdArc::new(Int64Array::from(vec![i64::from(round)])),
                        StdArc::new(
                            TimestampNanosecondArray::from(vec![i64::from(round) * 1_000_000_000])
                                .with_timezone("+00:00"),
                        ),
                    ],
                )
                .assured("the bounded row has the producer's exact input schema");
                let body = producer.batch(&rows).assured("the fixture row encodes");
                // Exercise an identical successful replay as well as possible partial failures. Every
                // relay must retain exactly one revision per tenant and round in the archive below.
                let completions_required = if round == 2 && tenant == 0 { 2 } else { 1 };
                let mut completions = 0;
                let mut attempts = 0_u32;
                loop {
                    nervix_primitives::task::consume_budget().await;
                    assert!(
                        !deadline.has_passed(),
                        "materialized input round expired: domain={domain}, round={round}, \
                         tenant={tenant}"
                    );
                    attempts += 1;
                    append_cucumber_log_line(&format!(
                        "materialized input submitting: domain={domain}, round={round}, tenant={tenant}, \
                         attempt={attempts}, producer={:?}, generation={}, initial_attachment={}, \
                         connection={:?}, admission={:?}, remaining={:?}",
                        producer.id(),
                        producer.description().generation,
                        producer.description().attachment,
                        producer.connection(),
                        producer.admission(),
                        deadline.remaining(),
                    ));
                    let submission = producer
                        .submit(body.clone())
                        .await
                        .assured("the input producer takes the bounded row");
                    append_cucumber_log_line(&format!(
                        "materialized input awaiting ACK: domain={domain}, round={round}, tenant={tenant}, \
                         attempt={attempts}, producer={:?}, submission={submission}",
                        producer.id(),
                    ));
                    let outcome = producer.rejoin(submission);
                    let mut outcome = std::pin::pin!(outcome);
                    let outcome = loop {
                        nervix_primitives::task::consume_budget().await;
                        nervix_primitives::select! {
                            result = &mut outcome => {
                                break result.assured("the acknowledged input exchange succeeds");
                            }
                            _ = nervix_primitives::time::sleep(Duration::from_secs(5)) => {
                                append_cucumber_log_line(&format!(
                                    "materialized input still awaiting ACK: domain={domain}, round={round}, \
                                     tenant={tenant}, attempt={attempts}, producer={:?}, \
                                     submission={submission}, connection={:?}, admission={:?}, end={:?}, \
                                     pending={:?}, remaining={:?}",
                                    producer.id(),
                                    producer.connection(),
                                    producer.admission(),
                                    producer.end(),
                                    producer.pending_submissions(),
                                    deadline.remaining(),
                                ));
                            }
                        }
                    };
                    append_cucumber_log_line(&format!(
                        "materialized input outcome: domain={domain}, round={round}, tenant={tenant}, \
                         attempt={attempts}, producer={:?}, submission={submission}, outcome={outcome:?}",
                        producer.id(),
                    ));
                    match outcome {
                        ProducerOutcome::Completed => {
                            println!(
                                "materialized input round {round}, tenant {tenant}: completed after \
                                 {attempts} attempts"
                            );
                            completions += 1;
                            if completions == completions_required {
                                break;
                            }
                        }
                        ProducerOutcome::ProcessingFailed { failure, message } => {
                            // The fixture retains each row's explicit event timestamp on replay, so a
                            // partially applied attempt does not create another materialized revision.
                            // Replaying a reported failed attempt preserves its complete value oracle;
                            // an unknown outcome or a final admission refusal remains a setup failure.
                            println!(
                                "materialized input round {round}, tenant {tenant}, attempt {attempts}: \
                                 replaying reported processing failure {failure:?}: {message}"
                            );
                            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
                        }
                        outcome => panic!(
                            "materialized input round {round}, tenant {tenant}: {outcome:?}"
                        ),
                    }
                }
            }
            producer
        })
        .await;
    producer.close().await.assured("the input producer closes");
}

#[then(
    expr = "within {string} the materialized generators report round {int}, {int} KiB, {int} \
            relays and {int} tenants"
)]
async fn then_materialized_generators_report_every_row(
    world: &mut ScenarioWorld,
    duration: String,
    round: u32,
    kibibytes: usize,
    relays: usize,
    tenants: usize,
) {
    let duration = parse_duration_text(&duration).assured("deadline is valid");
    let deadline = Instant::now() + duration;
    let mut pending = (0..relays)
        .flat_map(|relay| (0..tenants).map(move |tenant| (relay, tenant)))
        .collect::<BTreeSet<_>>();
    let length = kibibytes
        .checked_mul(1024)
        .assured("bounded row length fits");
    let mut session = world
        .active_session
        .take()
        .assured("the summary subscription exists");
    let (session, pending) =
        crate::common::runner_observation::observe_off_the_runner(async move {
            while !pending.is_empty() {
                nervix_primitives::task::consume_budget().await;
                let now = Instant::now();
                let event = if now < deadline {
                    session
                        .try_next_subscription(deadline.saturating_duration_since(now))
                        .await
                        .assured("subscription remains connected")
                } else {
                    None
                };
                let Some(event) = event else {
                    break;
                };
                if !event.payload.contains(&format!("\"round\":{round}")) {
                    continue;
                }
                assert!(
                    event.payload.contains(&format!("\"length\":{length}")),
                    "{}",
                    event.payload
                );
                if let Some(found) = pending
                    .iter()
                    .find(|(relay, tenant)| {
                        event
                            .payload
                            .contains(&format!("\"relay\":\"state_{relay}\""))
                            && event.payload.contains(&format!(
                                "key={{\"tenant\":\"restore-tenant-{tenant}\"}}"
                            ))
                    })
                    .copied()
                {
                    assert!(
                        event
                            .payload
                            .contains(&format!("\"tenant\":\"restore-tenant-{}\"", found.1)),
                        "payload and typed branch agree"
                    );
                    pending.remove(&found);
                }
            }
            (session, pending)
        })
        .await;
    world.active_session = Some(session);
    if !pending.is_empty() {
        println!(
            "missing materialized generator rows at the observation deadline: {pending:?}; \
             domain={}, round={round}, value_bytes={length}, subscription_node={:?}",
            world.domain, world.active_session_node
        );
        capture_missing_materialized_state(world, &pending).await;
        panic!("missing materialized generator rows: {pending:?}");
    }
}

#[then(
    expr = "node {string} reuses its remote materialized generation across five complete \
            observations of round {int} with {int} KiB rows"
)]
async fn then_materialized_generation_is_reused(
    world: &mut ScenarioWorld,
    node: String,
    round: u32,
    row_kib: usize,
) {
    let labels = [
        "class=\"bulk\"".to_string(),
        "direction=\"received\"".to_string(),
    ];
    let before = world
        .cluster()
        .read_observability_metric(&node, "nervix_interconnect_bulk_bytes_total", &labels)
        .await
        .assured("the remote reader exposes its transferred bulk bytes");
    for _ in 0..5 {
        then_materialized_generators_report_every_row(
            world,
            "30s".to_string(),
            round,
            row_kib,
            1,
            2,
        )
        .await;
    }
    let after = world
        .cluster()
        .read_observability_metric(&node, "nervix_interconnect_bulk_bytes_total", &labels)
        .await
        .assured("the remote reader still exposes its transferred bulk bytes");
    assert_eq!(
        after, before,
        "five complete observations of unchanged rows must reuse the verified generation"
    );
}

/// Observe the addressed owner's and replica's public state before failed-scenario teardown.
async fn capture_missing_materialized_state(
    world: &mut ScenarioWorld,
    pending: &BTreeSet<(usize, usize)>,
) {
    let relays = pending
        .iter()
        .map(|(relay, _)| *relay)
        .collect::<BTreeSet<_>>();
    let nodes = world
        .cluster()
        .node_ids()
        .into_iter()
        .map(|name| ClusterNodeName::parse(&name).assured("the fixture node name is valid"))
        .collect::<Vec<_>>();
    let cluster = world.cluster();
    let domain = &world.domain;
    let observations = crate::common::materialized_state_diagnostics::observe(
        &nodes,
        relays.into_iter(),
        |node, relay| {
            let node = node.clone();
            let query = format!("SHOW RELAY state_{relay} MATERIALIZED STATE;");
            async move { cluster.run_command(node.as_str(), domain, &query).await }
        },
    )
    .await;
    for observation in observations {
        println!("{observation}");
    }
}

/// Holds the node's next materialized snapshot fetch once its first received chunk is charged to
/// the node's bulk memory, until the node's restore checkpoint maintenance next asks bulk memory for
/// its working set, so the maintenance meets a fetch that holds part of the free room.
#[when(
    expr = "node {string} holds its next materialized snapshot fetch on its first chunk until \
            restore maintenance asks for bulk memory"
)]
async fn when_node_holds_its_next_materialized_snapshot_fetch(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .hold_materialized_snapshot_fetch_on(crate::common::cluster::node_name(&node_id));
}

#[then(
    expr = "within {string} node {string} resumes its held materialized snapshot fetch after \
            restore maintenance asked for bulk memory"
)]
async fn then_held_materialized_snapshot_fetch_resumes(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
) {
    let duration = parse_duration_text(&duration).assured("the wait is a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let node_name = crate::common::cluster::node_name(&node_id);
    nervix_primitives::time::timeout(
        duration,
        world
            .fault_injection
            .wait_for_materialized_snapshot_fetch_resumption(&node_name),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "node '{node_id}' never resumed its held materialized snapshot fetch within \
             {duration:?}: its restore maintenance did not ask for bulk memory while the fetch \
             held its first chunk"
        )
    });
}

#[when(expr = "the materialized summary subscription {string} is closed")]
async fn when_materialized_summary_subscription_is_closed(world: &mut ScenarioWorld, name: String) {
    let mut session = world
        .active_session
        .take()
        .assured("the materialized summary subscription is open");
    session
        .run_command(&format!("DELETE SUBSCRIPTION {name};"))
        .await
        .assured("the summary subscription closes before the backup cut");
    world.active_session_node = None;
    world.active_session_has_subscription = false;
}

#[then(
    expr = "backup {string} contains {int} materialized relays with {int} records each and more \
            than 32 MiB of columns"
)]
async fn then_large_materialized_archive_is_complete(
    world: &mut ScenarioWorld,
    file: String,
    relays: usize,
    records: usize,
) {
    let archive = described_archive(archive_path(world, &file)).await;
    let domain = scenario_domain(world, &world.domain);
    let entry = archive
        .domains
        .iter()
        .find(|entry| entry.record.domain == domain)
        .assured("domain exists");
    let mut columns = 0_u64;
    let mut count = 0_usize;
    let schema = entry
        .state
        .iter()
        .find_map(|state| match state {
            DescribedRuntimeState::Materialized { descriptor, .. } => Some(descriptor.schema),
            _ => None,
        })
        .assured("the graph has a materialized relay");
    for state in &entry.state {
        if let DescribedRuntimeState::Materialized {
            descriptor, groups, ..
        } = state
        {
            assert_eq!(
                descriptor.record_count,
                u64::try_from(records).verified("fixture count fits")
            );
            assert_eq!(
                descriptor.branch_generation,
                u64::try_from(records).verified("each tenant creates one branch")
            );
            assert_eq!(
                descriptor.revision,
                u64::try_from(records.checked_mul(2).assured("two bounded rounds fit"))
                    .verified("fixture count fits")
            );
            assert_eq!(
                descriptor.schema, schema,
                "all relays bind the same exact schema"
            );
            for group in groups {
                assert!(group.columns.length <= nervix_backup::MATERIALIZED_COLUMNS_BYTES);
                assert!(group.identities.length <= nervix_backup::MATERIALIZED_IDENTITIES_BYTES);
                columns = columns
                    .checked_add(group.columns.length)
                    .assured("fixture column length fits");
            }
            count += 1;
        }
    }
    assert_eq!(count, relays);
    assert!(
        columns > 32 * 1024 * 1024,
        "column sections total {columns} bytes"
    );
}
