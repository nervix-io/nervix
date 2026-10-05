//! Public materialized restore workloads with bounded input rows and small observable outputs.
//!
//! Layer: test harness.
//! - **Owns.** NSPL graphs, HTTP inputs, archive inventory assertions and generator subscriptions
//!   that qualify many containers and a single container larger than the bulk memory budget.
//! - **Depends on.** Public commands, HTTP intake, Row subscriptions and the public archive reader.
//! - **Must not know.** Runtime maps, checkpoint storage keys or private restore installation.

use nervix_backup::DescribedRuntimeState;

use super::*;

#[when(expr = "a materialized restore workload with {int} relays is created")]
async fn when_materialized_workload_is_created(world: &mut ScenarioWorld, relays: usize) {
    assert!((1..=40).contains(&relays));
    let domain = &world.domain;
    let host = expand_placeholders(world, "backup-large-{{test_id}}.example.com");
    let mut commands = format!(
        "CREATE UNPACED DOMAIN {domain};
         CREATE SCHEMA event ( tenant STRING, value STRING, round I64 );
         CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value string, round \
         integer );
         CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
         CREATE SCHEMA tenant_key ( tenant STRING );
         CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
         CREATE SCHEMA summary ( tenant STRING, relay STRING, length I64, round I64 );
         CREATE RELAY summaries SCHEMA summary BRANCHED BY by_tenant;
         CREATE VHOST edge {host};
         CREATE ENDPOINT ingress ON edge PATH '/large' TYPE HTTP;"
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
        "CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER \
         MAX SIZE 2MiB DECODE USING event_codec ",
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
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .assured("the workload command recovers through a leader change");
        assert!(
            outcome.succeeded(),
            "the public materialized workload is valid: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message);
    }
    let leader = current_leader_node(world).await;
    let session = crate::execute_nspl_commands_on_node(
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

#[when(expr = "round {int} of {int} KiB materialized rows is posted for {int} tenants")]
async fn when_large_materialized_round_is_posted(
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
    let host = expand_placeholders(world, "backup-large-{{test_id}}.example.com");
    let node = world
        .cluster()
        .node_ids()
        .first()
        .assured("one node is live")
        .clone();
    for tenant in 0..tenants {
        let payload = serde_json::json!({"tenant": format!("restore-tenant-{tenant}"), "value": &value, "round": round}).to_string();
        world
            .cluster()
            .publish_http(&node, &host, "/large", &payload)
            .await
            .assured("the bounded input row is accepted");
    }
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
    let session = world
        .active_session
        .as_mut()
        .assured("the summary subscription exists");
    while !pending.is_empty() {
        let now = Instant::now();
        assert!(
            now < deadline,
            "missing materialized generator rows: {pending:?}"
        );
        let event = session
            .try_next_subscription(deadline.saturating_duration_since(now))
            .await
            .assured("subscription remains connected")
            .unwrap_or_else(|| panic!("missing materialized generator rows: {pending:?}"));
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
                    && event
                        .payload
                        .contains(&format!("key={{\"tenant\":\"restore-tenant-{tenant}\"}}"))
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
fn then_large_materialized_archive_is_complete(
    world: &mut ScenarioWorld,
    file: String,
    relays: usize,
    records: usize,
) {
    let archive = nervix_backup::describe_archive(
        std::fs::File::open(archive_path(world, &file)).assured("archive opens"),
    )
    .assured("archive verifies");
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
