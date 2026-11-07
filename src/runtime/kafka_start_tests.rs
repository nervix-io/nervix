//! Kafka startup through the production runtime coordinator and native durable state.
//!
//! Layer: test harness.
//! - **Owns.** A delayed unit's canonical positions, competing offers and generation resumption.
//! - **Depends on.** Current Plexus fixtures, installed plans, a loopback node and native storage.
//! - **Must not know.** Kafka driver internals or a copied startup algorithm.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the production Kafka start boundary")
)]

use nervix_models::{DomainStartPoint, DomainStatus, DomainTimeRate, Statement};

use super::*;

struct Fixture {
    runtime: Runtime,
    states: Vec<KafkaOffsetStateOriginator>,
    domain: DomainName,
    _directory: tempfile::TempDir,
}

impl Fixture {
    async fn new(instances: u64) -> Self {
        let directory = tempfile::tempdir().assured("the test owns an empty database directory");
        let database = Database::builder(directory.path())
            .open()
            .assured("the empty database opens");
        let runtime = Runtime::with_persistence(Some(database), Duration::from_secs(30))
            .assured("current runtime storage initializes");
        let node = ClusterNodeName::parse("node-1").assured("the loopback node has a valid name");
        let incarnation = attach_loopback_cluster(&runtime, &node).await;
        let dispatcher = runtime
            .inner
            .remote_dispatcher
            .load_full()
            .assured("the loopback dispatcher is installed");
        assert!(
            dispatcher.is_live_node_identity(&ClusterNodeIdentity::new(node.clone(), incarnation,)),
            "the current local run is known before gossip publishes membership"
        );
        let domain = DomainName::parse("starts").assured("the fixture domain name is valid");
        let mut lifecycle = unpaced_domain_state(domain.as_str());
        lifecycle.status = DomainStatus::Stopped;
        lifecycle.start_version = 7;
        lifecycle.last_start = DomainStartPoint::At {
            timestamp: "2100-01-01T00:00:00Z"
                .parse()
                .assured("the future start is a timestamp"),
            time_rate: DomainTimeRate::ONE,
        };
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), lifecycle.clone())]));
        let mut nodes = Vec::new();
        let source_statement = format!(
            "CREATE KAFKA TOPIC INGESTOR source FROM broker TOPIC 'events' OFFSET BY DOMAIN \
             INSTANCES {instances} MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec TO \
             events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR \
             LOG"
        );
        for source in [
            "CREATE SCHEMA event (value I64)",
            "CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value integer)",
            "CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event",
            "CREATE RELAY events SCHEMA event UNBRANCHED",
            "CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' }",
            &source_statement,
        ] {
            let Statement::Create(statement) = nervix_plexus::statement::parse_statement(source)
                .assured("the fixture uses complete current statements")
            else {
                panic!("the fixture only creates graph Models");
            };
            let model: nervix_models::Model = (*statement.body)
                .try_map_resource_versions(|_, _| -> Result<u64, std::convert::Infallible> {
                    unreachable!("these literal statements bind no resources")
                })
                .assured("models without resource references need no resolution");
            nodes.push(scheduled_model(model));
        }
        let schedule = nervix_models::DomainSchedule::new(domain.clone(), nodes, Vec::new());
        runtime
            .rebuild_domain_from_schedule(&node, &domain, Some(schedule), false)
            .await
            .assured("the stopped graph installs native ownership without opening consumers");
        lifecycle.status = DomainStatus::Running;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), lifecycle)]));
        let mut states = Vec::new();
        for index in 0..instances {
            let placement = RuntimeStatePlacement {
                domain: domain.clone(),
                state: RuntimeState::KafkaOffset,
                kind: ModelKind::Ingestor,
                identifier: ModelName::parse("source").assured("the source name is valid"),
                unit: Unit::try_instance(index)
                    .assured("the fixture index fits the execution-unit range"),
                branch_key: None,
            };
            states.push(
                runtime
                    .replicated_kafka_offset_state(
                        placement,
                        Some(node.clone()),
                        Vec::new(),
                        0,
                        Some(&node),
                    )
                    .assured("the assigned unit's native state installs")
                    .originator
                    .assured("the primary receives its current originator"),
            );
        }
        Self {
            runtime,
            states,
            domain,
            _directory: directory,
        }
    }

    fn persisted(&self, index: usize) -> KafkaOffsetStateRead {
        let placement = self.states[index].placement().clone();
        let checkpoint = self
            .runtime
            .stored_runtime_state_snapshot(&placement)
            .assured("the native checkpoint reads")
            .assured("initialization persists before confirming adoption");
        let state = Arc::new(
            ReplicatedKafkaOffsetState::new(placement, Some(checkpoint))
                .assured("the persisted current checkpoint loads"),
        );
        ReplicatedKafkaOffsetState::read(&state)
    }
}

#[nervix_primitives::test]
async fn a_single_unit_selects_and_persists_its_canonical_start_directly() {
    let fixture = Fixture::new(1).await;
    for offset in [4, 99] {
        fixture
            .runtime
            .start_single_kafka_unit(
                &fixture.states[0],
                7,
                vec![KafkaOffsetPosition {
                    topic: "events".to_owned(),
                    partition: 0,
                    offset,
                }],
            )
            .await
            .assured("the single native owner initializes durably");
        let persisted = fixture.persisted(0);
        assert_eq!(persisted.applied_start_generation(), Some(7));
        assert_eq!(persisted.next_offset("events", 0), Some(4));
    }
    fixture.runtime.shutdown(Duration::from_secs(5)).await;
}

#[nervix_primitives::test]
async fn a_late_unit_adopts_the_canonical_cut_after_a_plain_resume() {
    let fixture = Fixture::new(3).await;
    let starts = [4, 7, 9]
        .into_iter()
        .enumerate()
        .map(|(partition, offset)| KafkaOffsetPosition {
            topic: "events".to_owned(),
            partition: i32::try_from(partition).assured("the fixture has three partitions"),
            offset,
        })
        .collect::<Vec<_>>();
    fixture
        .runtime
        .propose_kafka_start(&fixture.states[1], 7, starts.clone())
        .await
        .assured("a nonzero instance can select the first cut before unit zero opens");
    assert_eq!(fixture.persisted(1).next_offset("events", 1), Some(7));
    assert_eq!(fixture.persisted(0).applied_start_generation(), Some(7));
    assert_eq!(fixture.states[2].read().applied_start_generation(), None);
    fixture
        .runtime
        .commit_domain_kafka_offset(
            &fixture.states[1],
            KafkaOffsetPosition {
                topic: "events".to_owned(),
                partition: 1,
                offset: 41,
            },
        )
        .await
        .assured("the initialized unit commits through its own owner");
    let competing = starts
        .into_iter()
        .map(|position| KafkaOffsetPosition {
            offset: 99,
            ..position
        })
        .collect();
    fixture
        .runtime
        .propose_kafka_start(&fixture.states[0], 7, competing)
        .await
        .assured("a late independently resolved candidate uses the accepted cut");
    assert_eq!(fixture.states[0].read().next_offset("events", 0), Some(4));
    assert_eq!(fixture.states[1].read().next_offset("events", 1), Some(41));
    assert!(
        fixture.states[0]
            .read()
            .start_coordination()
            .offer
            .load_full()
            .is_none()
    );
    let mut lifecycle = unpaced_domain_state(fixture.domain.as_str());
    lifecycle.status = DomainStatus::Stopped;
    lifecycle.start_version = 7;
    fixture.runtime.sync_domains(&BTreeMap::from([(
        fixture.domain.clone(),
        lifecycle.clone(),
    )]));
    assert!(
        fixture
            .runtime
            .synchronize_kafka_start(&fixture.states[2], 7)
            .await
            .is_err()
    );
    lifecycle.status = DomainStatus::Running;
    lifecycle.start_version = 8;
    lifecycle.last_start = DomainStartPoint::Resume;
    fixture
        .runtime
        .sync_domains(&BTreeMap::from([(fixture.domain.clone(), lifecycle)]));
    let clock = fixture
        .runtime
        .domain_clock_lifecycle(&fixture.domain)
        .assured("the resumed domain owns its lifecycle")
        .bind()
        .assured("the resumed lifecycle is installed");
    {
        let mut execution = fixture
            .runtime
            .inner
            .executions
            .get_mut(&fixture.domain)
            .assured("the typed graph remains installed");
        execution.start_version = 8;
        execution.domain_clock = clock;
    }
    fixture
        .runtime
        .synchronize_kafka_start(&fixture.states[2], 8)
        .await
        .assured("a delayed unit adopts pending explicit-start seeds during a plain resume");
    assert_eq!(fixture.states[2].read().next_offset("events", 2), Some(9));
    assert_eq!(fixture.persisted(2).next_offset("events", 2), Some(9));
    assert_eq!(fixture.persisted(2).applied_start_generation(), Some(7));
    assert_eq!(fixture.states[1].read().next_offset("events", 1), Some(41));
    fixture.runtime.shutdown(Duration::from_secs(5)).await;
}
