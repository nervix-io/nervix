//! Public deduplicator and window workloads whose single-branch state exceeds the bulk budget.
//!
//! Layer: test harness.
//! - **Owns.** The NSPL graph, the HTTP inputs that build one large keyspace and window beside many
//!   small branches, the summaries a subscription reports for them, and the archive inventory of
//!   their sections.
//! - **Depends on.** Public commands, HTTP intake, Row subscriptions and the public archive reader.
//! - **Must not know.** Native checkpoint encodings, restore conversion or runtime persistence.

use nervix_backup::DescribedRuntimeState;

use super::*;

/// The virtual host the workload's endpoints serve.
const HOST: &str = "backup-large-branch-{{test_id}}.example.com";

/// Every small tenant's name begins with this.
const SMALL_TENANT: &str = "small";

/// Each small tenant's one window row reports this latency.
const SMALL_LATENCY: i64 = 1;

/// The archived keyspace and window of the large tenant must each exceed the default bulk budget.
const BULK_BUDGET_BYTES: u64 = 32 * 1024 * 1024;

/// Every large key's transaction identifier and every large window row's note are padded to this.
const LARGE_VALUE_BYTES: usize = 1024 * 1024;

#[when(expr = "a large branch state restore workload is created")]
async fn when_large_branch_state_workload_is_created(world: &mut ScenarioWorld) {
    let domain = world.domain.clone();
    let host = expand_placeholders(world, HOST);
    let commands = format!(
        "CREATE UNPACED DOMAIN {domain};
         CREATE SCHEMA payment ( tenant STRING, transaction_id STRING, amount I64 );
         CREATE WIRE JSON SCHEMA payment_wire MODE STRICT ( tenant string, transaction_id string, \
         amount integer );
         CREATE CODEC payment_codec FROM WIRE JSON SCHEMA payment_wire TO SCHEMA payment;
         CREATE SCHEMA metric ( tenant STRING, latency I64, note STRING );
         CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( tenant string, latency integer, note \
         string );
         CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
         CREATE SCHEMA key_summary ( tenant STRING, amount I64, key_bytes I64 );
         CREATE SCHEMA metric_summary ( tenant STRING, samples I64, total I64 OPTIONAL, \
         first_latency I64 OPTIONAL, last_latency I64 OPTIONAL, smallest I64 OPTIONAL, largest \
         I64 OPTIONAL, latency_p0 F64 OPTIONAL, note_bytes I64 OPTIONAL );
         CREATE SCHEMA tenant_key ( tenant STRING );
         CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
         CREATE RELAY payments SCHEMA payment BRANCHED BY by_tenant;
         CREATE RELAY unique_keys SCHEMA key_summary BRANCHED BY by_tenant;
         CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant;
         CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant;
         CREATE VHOST edge {host};
         CREATE ENDPOINT payment_ingress ON edge PATH '/payments' TYPE HTTP;
         CREATE ENDPOINT metric_ingress ON edge PATH '/metrics' TYPE HTTP;
         CREATE INGESTOR payment_source FROM ENDPOINT payment_ingress MODE NO_ACK SEQUENTIAL ON \
         QUIESCE BUFFER MAX SIZE 2MiB DECODE USING payment_codec TO payments INHERIT ALL BRANCHED \
         BY by_tenant SET tenant = message.tenant FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL \
         ERROR LOG;
         CREATE INGESTOR metric_source FROM ENDPOINT metric_ingress MODE NO_ACK SEQUENTIAL ON \
         QUIESCE BUFFER MAX SIZE 2MiB DECODE USING metric_codec TO metrics INHERIT ALL BRANCHED \
         BY by_tenant SET tenant = message.tenant FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL \
         ERROR LOG;
         CREATE DEDUPLICATOR unique_key_filter FROM payments DEDUPLICATE ON input.transaction_id, \
         input.amount MAX TIME 30m BRANCHED BY by_tenant TO unique_keys SET tenant = \
         input.tenant, amount = input.amount, key_bytes = octet_length(input.transaction_id) \
         FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
         CREATE WINDOW PROCESSOR latency_window FROM metrics WIDTH 41 MESSAGES STEP 1 MESSAGES \
         BRANCHED BY by_tenant TO metric_summaries SET tenant = FIRST(input.tenant), samples = \
         COUNT(input.latency), total = SUM(input.latency), first_latency = FIRST(input.latency), \
         last_latency = LAST(input.latency), smallest = MIN(input.latency), largest = \
         MAX(input.latency), latency_p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, \
         100, '1h'), note_bytes = SUM(octet_length(input.note)) ON MESSAGE ERROR LOG;
         START;"
    );
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
            "the public large branch state workload is valid: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message);
    }
    when_large_branch_state_subscription_is_opened(world).await;
}

#[when(expr = "the large branch state subscription is opened on the leader node")]
async fn when_large_branch_state_subscription_is_opened(world: &mut ScenarioWorld) {
    let leader = current_leader_node(world).await;
    let session = crate::execute_nspl_commands_on_node(
        world,
        &leader,
        "CREATE SUBSCRIPTION large_keys TO unique_keys;
         CREATE SUBSCRIPTION large_summaries TO metric_summaries;",
    )
    .await
    .assured("the large branch state subscriptions open");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = true;
}

#[when(expr = "the large branch state subscription is closed")]
async fn when_large_branch_state_subscription_is_closed(world: &mut ScenarioWorld) {
    let mut session = world
        .active_session
        .take()
        .assured("the large branch state subscription is open");
    for name in ["large_keys", "large_summaries"] {
        session
            .run_command(&format!("DELETE SUBSCRIPTION {name};"))
            .await
            .assured("the summary subscription closes");
    }
    world.active_session_node = None;
    world.active_session_has_subscription = false;
}

/// One deduplicator key of a round: its transaction identifier, padded for a large tenant, and the
/// amount that tells the keys of a round apart.
struct LargeKey {
    transaction_id: String,
    amount: i64,
}

impl LargeKey {
    fn new(tenant: &str, round: u32, index: usize) -> Self {
        let transaction_id = format!(
            "{tenant}-r{round}-k{index:02}-{}",
            "k".repeat(LARGE_VALUE_BYTES)
        );
        let index = i64::try_from(index).assured("a bounded key index fits");
        let base = i64::from(round)
            .checked_mul(1000)
            .assured("a scenario round is far below i64::MAX / 1000");
        let amount = base
            .checked_add(index)
            .assured("a bounded key index fits beside its round");
        Self {
            transaction_id,
            amount,
        }
    }

    /// The fragments of the summary the deduplicator emits when it admits this key.
    fn summary(&self, tenant: &str) -> Vec<String> {
        vec![
            format!("key={{\"tenant\":\"{tenant}\"}}"),
            format!("\"amount\":{}", self.amount),
            format!("\"key_bytes\":{}", self.transaction_id.len()),
        ]
    }
}

/// The node the workload posts its inputs to.
fn posting_node(world: &ScenarioWorld) -> String {
    world
        .cluster()
        .node_ids()
        .first()
        .assured("one node is live")
        .clone()
}

#[when(expr = "round {int} of {int} large deduplicator keys is posted for tenant {string}")]
async fn when_large_keys_are_posted(
    world: &mut ScenarioWorld,
    round: u32,
    keys: usize,
    tenant: String,
) {
    assert!((1..=64).contains(&keys));
    let host = expand_placeholders(world, HOST);
    let node = posting_node(world);
    for index in 0..keys {
        nervix_primitives::task::consume_budget().await;
        let key = LargeKey::new(&tenant, round, index);
        let payload = serde_json::json!({
            "tenant": &tenant,
            "transaction_id": key.transaction_id,
            "amount": key.amount,
        })
        .to_string();
        world
            .cluster()
            .publish_http(&node, &host, "/payments", &payload)
            .await
            .assured("the bounded payment is accepted");
    }
}

#[when(expr = "{int} large window rows with latencies from {int} are posted for tenant {string}")]
async fn when_large_window_rows_are_posted(
    world: &mut ScenarioWorld,
    rows: usize,
    first_latency: i64,
    tenant: String,
) {
    assert!((1..=64).contains(&rows));
    let note = "n".repeat(LARGE_VALUE_BYTES);
    let host = expand_placeholders(world, HOST);
    let node = posting_node(world);
    for index in 0..rows {
        nervix_primitives::task::consume_budget().await;
        let offset = i64::try_from(index).assured("a bounded row index fits");
        let latency = first_latency
            .checked_add(offset)
            .assured("a bounded latency fits");
        let payload = serde_json::json!({
            "tenant": &tenant,
            "latency": latency,
            "note": &note,
        })
        .to_string();
        world
            .cluster()
            .publish_http(&node, &host, "/metrics", &payload)
            .await
            .assured("the bounded metric is accepted");
    }
}

/// The name of small tenant `index`.
fn small_tenant(index: usize) -> String {
    format!("{SMALL_TENANT}-{index:02}")
}

/// The one deduplicator key of small tenant `index`.
fn small_key(index: usize) -> LargeKey {
    let tenant = small_tenant(index);
    LargeKey {
        transaction_id: format!("{tenant}-key"),
        amount: i64::try_from(index).assured("a bounded tenant index fits"),
    }
}

#[when(expr = "one deduplicator key and one window row are posted for each of {int} small tenants")]
async fn when_small_branches_are_posted(world: &mut ScenarioWorld, tenants: usize) {
    assert!((1..=64).contains(&tenants));
    let host = expand_placeholders(world, HOST);
    let node = posting_node(world);
    for index in 0..tenants {
        nervix_primitives::task::consume_budget().await;
        let tenant = small_tenant(index);
        let key = small_key(index);
        let payment = serde_json::json!({
            "tenant": &tenant,
            "transaction_id": key.transaction_id,
            "amount": key.amount,
        })
        .to_string();
        world
            .cluster()
            .publish_http(&node, &host, "/payments", &payment)
            .await
            .assured("the small payment is accepted");
        let metric = serde_json::json!({
            "tenant": &tenant,
            "latency": SMALL_LATENCY,
            "note": "small",
        })
        .to_string();
        world
            .cluster()
            .publish_http(&node, &host, "/metrics", &metric)
            .await
            .assured("the small metric is accepted");
    }
}

/// The summaries one check expects beside its docstring fragment sets.
struct ExpectedSummaries<'a> {
    round: u32,
    keys: usize,
    tenant: &'a str,
    small_tenants: usize,
}

impl ExpectedSummaries<'_> {
    /// Waits for exactly the summaries of round `round`'s `keys` large keys, one key of each of
    /// `small_tenants` small tenants, and every docstring fragment set, failing on any other
    /// payload.
    async fn receive(&self, world: &mut ScenarioWorld, duration: &str, step: &Step) {
        let mut expected = crate::docstring_fragment_sets(world, step);
        for index in 0..self.keys {
            let key = LargeKey::new(self.tenant, self.round, index);
            expected.push(key.summary(self.tenant));
        }
        for index in 0..self.small_tenants {
            expected.push(small_key(index).summary(&small_tenant(index)));
        }
        crate::receive_expected_fragment_sets(
            world,
            duration,
            expected,
            crate::UnmatchedPayloads::Fail,
        )
        .await;
    }
}

#[then(
    expr = "within {string} the large branch state subscription reports round {int} of {int} keys \
            for tenant {string}, a key for each of {int} small tenants, and each fragment set"
)]
async fn then_large_and_small_summaries_are_reported(
    world: &mut ScenarioWorld,
    duration: String,
    round: u32,
    keys: usize,
    tenant: String,
    small_tenants: usize,
    #[step] step: &Step,
) {
    let expected = ExpectedSummaries {
        round,
        keys,
        tenant: &tenant,
        small_tenants,
    };
    expected.receive(world, &duration, step).await;
}

#[then(
    expr = "within {string} the large branch state subscription reports round {int} of {int} keys \
            for tenant {string} and each fragment set"
)]
async fn then_large_summaries_are_reported(
    world: &mut ScenarioWorld,
    duration: String,
    round: u32,
    keys: usize,
    tenant: String,
    #[step] step: &Step,
) {
    let expected = ExpectedSummaries {
        round,
        keys,
        tenant: &tenant,
        small_tenants: 0,
    };
    expected.receive(world, &duration, step).await;
}

/// The tenant a deduplicator or window descriptor's branch key names.
fn branch_tenant(branch: Option<&Vec<nervix_backup::StateField>>) -> String {
    let fields = branch.assured("the workload's branch state is branched");
    let field = fields
        .iter()
        .find(|field| field.name == "tenant")
        .assured("the branch key names its tenant");
    let nervix_backup::StateValue::String(tenant) = &field.value else {
        panic!("the tenant branch field is a string: {:?}", field.value);
    };
    tenant.clone()
}

#[then(
    expr = "backup {string} holds a deduplicator keyspace and a window of tenant {string} above \
            32 MiB beside {int} small branches of each"
)]
fn then_large_branch_state_archive_is_complete(
    world: &mut ScenarioWorld,
    file: String,
    tenant: String,
    small_tenants: usize,
) {
    let archive = nervix_backup::describe_archive(
        std::fs::File::open(archive_path(world, &file)).assured("the archive opens"),
    )
    .assured("the archive verifies");
    let domain = scenario_domain(world, &world.domain);
    let entry = archive
        .domains
        .iter()
        .find(|entry| entry.record.domain == domain)
        .assured("the workload domain is archived");
    let mut large_keyspace = None;
    let mut large_window = None;
    let mut small_keyspaces = BTreeSet::new();
    let mut small_windows = BTreeSet::new();
    for state in &entry.state {
        match state {
            DescribedRuntimeState::Deduplicator {
                descriptor, groups, ..
            } => {
                let branch = branch_tenant(descriptor.branch.as_ref());
                if branch == tenant {
                    let mut bytes = 0_u64;
                    for group in groups {
                        assert!(group.length <= nervix_backup::BRANCH_STATE_GROUP_BYTES);
                        bytes = bytes
                            .checked_add(group.length)
                            .assured("the archived key bytes fit");
                    }
                    large_keyspace = Some((descriptor.keys, bytes));
                } else if branch.starts_with(SMALL_TENANT) {
                    assert_eq!(descriptor.keys, 1, "{branch} holds its one key");
                    small_keyspaces.insert(branch);
                }
            }
            DescribedRuntimeState::Window {
                descriptor, groups, ..
            } => {
                let branch = branch_tenant(descriptor.branch.as_ref());
                let rows = u64::try_from(descriptor.rows.len()).assured("rows fit 64 bits");
                if branch == tenant {
                    let mut bytes = 0_u64;
                    for group in groups {
                        assert!(group.input.length <= nervix_backup::BRANCH_STATE_GROUP_BYTES);
                        assert!(group.arguments.length <= nervix_backup::BRANCH_STATE_GROUP_BYTES);
                        bytes = bytes
                            .checked_add(group.input.length)
                            .assured("the archived row bytes fit");
                    }
                    let mut delayed = 0_usize;
                    for accumulator in &descriptor.accumulators {
                        if let nervix_backup::WindowAccumulatorRecord::LinearHistogram {
                            delayed_removals,
                        } = accumulator
                        {
                            delayed = delayed
                                .checked_add(delayed_removals.len())
                                .assured("bounded removals fit");
                        }
                    }
                    large_window = Some((rows, bytes, delayed));
                } else if branch.starts_with(SMALL_TENANT) {
                    assert_eq!(rows, 1, "{branch} retains its one row");
                    small_windows.insert(branch);
                }
            }
            _ => {}
        }
    }
    let (keys, key_bytes) = large_keyspace.assured("the large keyspace is archived");
    assert_eq!(keys, 40, "the large keyspace holds every posted key");
    assert!(
        key_bytes > BULK_BUDGET_BYTES,
        "the large keyspace's key groups total {key_bytes} bytes"
    );
    let (rows, row_bytes, delayed) = large_window.assured("the large window is archived");
    assert_eq!(
        rows, 40,
        "the large window retains every row it did not step past"
    );
    assert!(
        row_bytes > BULK_BUDGET_BYTES,
        "the large window's input groups total {row_bytes} bytes"
    );
    assert_eq!(
        delayed, 1,
        "the stepped row remains a pending histogram removal"
    );
    assert_eq!(small_keyspaces.len(), small_tenants);
    assert_eq!(small_windows.len(), small_tenants);
}
