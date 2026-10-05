//! Layer: test harness.
//! Owns: materialized owner selection, publication lifetimes and complete snapshot properties.
//! May depend on: the production materialized owner, Arrow carriers and shared model harness.
//! Must not know: control-plane transactions or publication backend internals.

use ahash::HashMap;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    CreateSchema, DomainName, FieldName, ModelKind, ModelName, ParseAsType, SchemaField,
    SchemaFingerprint, SchemaName, Timestamp,
};

use super::*;
use crate::{
    runtime::{RuntimeState, materialized_snapshot::SealedSource},
    runtime_schema::{RuntimeRecordMetadata, RuntimeValue, compile_schema},
};

fn placement() -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: DomainName::parse("publication").assured("valid test domain"),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: ModelName::parse("profiles").assured("valid test relay"),
        branch_key: None,
    }
}

fn key(tenant: &str) -> Option<BranchKey> {
    Some(
        BranchKey::from_fields([(
            FieldName::parse("tenant").assured("valid test field"),
            RuntimeValue::String(tenant.to_string()),
        )])
        .assured("one typed branch field is valid"),
    )
}

fn row(value: i64, low: i64, high: i64) -> RuntimeRow {
    let schema = compile_schema(&CreateSchema {
        name: SchemaName::parse("profiles").assured("valid fixture schema"),
        fields: [
            ("value", ParseAsType::I64, false, false),
            ("note", ParseAsType::String, true, true),
            ("enabled", ParseAsType::Bool, false, false),
        ]
        .into_iter()
        .map(|(name, ty, optional, sensitive)| SchemaField {
            name: FieldName::parse(name).assured("valid fixture field"),
            ty,
            optional,
            sensitive,
        })
        .collect(),
    });
    let mut fields = vec![
        ("value".to_string(), RuntimeValue::I64(value)),
        ("enabled".to_string(), RuntimeValue::Bool(value % 2 == 0)),
    ];
    if value % 3 != 0 {
        fields.push((
            "note".to_string(),
            RuntimeValue::String(format!("row-{value}")),
        ));
    }
    schema
        .batch_from_test_rows([fields])
        .assured("valid typed fixture columns")
        .runtime_row(
            0,
            RuntimeRecordMetadata::from_ingested_at_watermarks(
                Timestamp::from_unix_nanos(low),
                Timestamp::from_unix_nanos(high),
            ),
        )
        .assured("one fixture row exists")
}

fn owner(state: &Arc<ReplicatedMaterializedRelayState>) -> MaterializedRelayStateOriginator {
    ReplicatedMaterializedRelayState::bind(state, StateReplicationRoles::owned_by(None), None)
        .originator
        .assured("an unassigned concrete branch originates locally")
}

fn assert_row(actual: &RuntimeRow, expected: &RuntimeRow) {
    assert_eq!(actual.arrow_schema(), expected.arrow_schema());
    assert_eq!(
        actual.metadata().to_remote(),
        expected.metadata().to_remote()
    );
    for index in 0..expected.arrow_schema().fields().len() {
        assert_eq!(
            actual.value_at(index).assured("valid restored column"),
            expected.value_at(index).assured("valid original column")
        );
    }
    let sensitivity = nervix_vm::SchemaSensitivity::from_sensitive_fields(["note".to_string()]);
    assert_eq!(
        actual
            .to_json_string_masking(&sensitivity)
            .assured("valid sensitive projection"),
        expected
            .to_json_string_masking(&sensitivity)
            .assured("valid original projection")
    );
}

fn assert_records(
    actual: &[MaterializedGenerationRecord],
    expected: &HashMap<Option<BranchKey>, RuntimeRow>,
) {
    assert_eq!(actual.len(), expected.len());
    for record in actual {
        assert_row(
            &record.row,
            expected
                .get(&record.branch)
                .assured("exact branch identity"),
        );
    }
}

#[test]
fn established_updates_retain_membership_and_arrow_payloads_and_keep_timestamp_ties() {
    let first = row(1, -2, 4);
    let state = Arc::new(ReplicatedMaterializedRelayState::new(
        placement(),
        first.arrow_schema(),
    ));
    let mut originator = owner(&state);
    let branch = key("acme");
    assert_eq!(
        originator
            .update_last_by_timestamp(&branch, first.clone())
            .assured("current assignment"),
        Some(1)
    );
    let membership = state.entries.load_full();
    let published = membership.get(&branch).assured("installed branch").clone();
    let captured = originator.read().capture();
    for rejected in [row(2, -1, 3), row(3, -3, 4), row(4, -2, 4)] {
        assert_eq!(
            originator
                .update_last_by_timestamp(&branch, rejected)
                .assured("current assignment"),
            None
        );
        assert_eq!(originator.read().current_lsm(), 1);
        assert_row(
            &originator.read().record(&branch).assured("live row").row,
            &first,
        );
    }
    let second = row(5, -1, 4);
    assert_eq!(
        originator
            .update_last_by_timestamp(&branch, second.clone())
            .assured("current assignment"),
        Some(2)
    );
    assert!(
        StdArc::ptr_eq(&membership, &state.entries.load_full()),
        "replacement never republishes branch membership"
    );
    assert_eq!(state.branch_generation.load(Ordering::SeqCst), 1);
    let current = published.load_full().assured("published replacement");
    assert_row(&current, &second);
    assert!(
        Arc::ptr_eq(current.batch(), second.batch()),
        "publication shares the Arrow carrier"
    );
    assert_row(&captured.records()[0].row, &first);
    assert!(Arc::ptr_eq(
        captured.records()[0].row.batch(),
        first.batch()
    ));
    let third = row(6, -8, 5);
    assert_eq!(
        originator
            .update_last_by_timestamp(&branch, third.clone())
            .assured("current assignment"),
        Some(3)
    );
    assert_row(
        &originator.read().record(&branch).assured("live row").row,
        &third,
    );
}

#[nervix_primitives::test]
async fn materialized_branch_eviction_withdraws_its_record_before_recreation() {
    use crate::runtime::{Runtime, install_unpaced_test_domain, junction_branch_template};

    let runtime = Runtime::default();
    let placed = placement();
    install_unpaced_test_domain(&runtime, &placed.domain);
    let branch_key = key("acme");
    let mut branch = junction_branch_template("joining", "incoming")
        .instantiate(&runtime, &placed.domain, branch_key.clone(), 1)
        .await
        .assured("the branch installs")
        .into_inner();
    let state = Arc::new(ReplicatedMaterializedRelayState::new(
        placed.clone(),
        row(1, 1, 1).arrow_schema(),
    ));
    let mut originator = owner(&state);
    originator
        .update_last_by_timestamp(&branch_key, row(1, 1, 1))
        .assured("current assignment");
    let read = originator.read().clone();
    branch.materialized_states.insert(
        nervix_models::RelayName::from(&placed.identifier),
        originator,
    );
    branch.evict().await;
    assert!(
        read.record(&branch_key).is_none(),
        "branch eviction must withdraw its materialized publication"
    );
    let recreated = owner(&state);
    assert!(recreated.read().record(&branch_key).is_none());
}

#[test]
fn eviction_ends_retained_publications_and_recreation_gets_a_fresh_lifetime() {
    let first = row(1, 1, 1);
    let state = Arc::new(ReplicatedMaterializedRelayState::new(
        placement(),
        first.arrow_schema(),
    ));
    let mut originator = owner(&state);
    let (acme, beta) = (key("acme"), key("beta"));
    originator
        .update_last_by_timestamp(&acme, first.clone())
        .assured("current assignment");
    originator
        .update_last_by_timestamp(&beta, row(2, 2, 2))
        .assured("current assignment");
    let membership = state.entries.load_full();
    let retained = membership.get(&acme).assured("installed branch").clone();
    let captured = originator.read().capture();
    assert_eq!(
        originator.remove_key(&acme).assured("current assignment"),
        Some(3)
    );
    assert!(retained.load_full().is_none());
    assert!(originator.read().record(&acme).is_none());
    assert!(originator.read().record(&beta).is_some());
    assert_eq!(
        originator.remove_key(&acme).assured("current assignment"),
        None
    );
    assert_eq!(state.branch_generation.load(Ordering::SeqCst), 3);
    let recreated = row(3, 0, 0);
    assert_eq!(
        originator
            .update_last_by_timestamp(&acme, recreated.clone())
            .assured("current assignment"),
        Some(4)
    );
    let current = state.entries.load_full();
    assert!(!Arc::ptr_eq(
        &retained,
        current.get(&acme).assured("recreated branch")
    ));
    assert!(
        retained.load_full().is_none(),
        "an ended publication never attaches to a replacement"
    );
    assert_row(
        &originator.read().record(&acme).assured("recreated row").row,
        &recreated,
    );
    assert_row(
        &captured
            .records()
            .iter()
            .find(|record| record.branch == acme)
            .assured("captured branch")
            .row,
        &first,
    );
}

#[nervix_primitives::test]
async fn each_materialized_relay_installs_its_branch_owner_in_the_same_routing_epoch() {
    use crate::runtime::*;

    let runtime = Runtime::default();
    let placed = placement();
    install_unpaced_test_domain(&runtime, &placed.domain);
    let mut branch = junction_branch_template("joining", "incoming")
        .instantiate(&runtime, &placed.domain, key("acme"), 1)
        .await
        .assured("the branch installs")
        .into_inner();
    let mut specs = HashMap::default();
    for name in ["profiles", "rules"] {
        let relay = RelayName::parse(name).assured("valid relay");
        publish_state_identity(
            &runtime,
            &placed.domain,
            ModelKind::Relay,
            ModelName::from(&relay),
        );
        specs.insert(
            relay,
            RuntimeMaterializedRelaySpec::new(
                row(1, 1, 1).arrow_schema(),
                VmSchemaSensitivity::default(),
                ResolvedBranching::unbranched(),
            ),
        );
    }
    // Reconciliation reads the routing the runtime publishes, as a schedule leaves it, so the test
    // publishes the routing it gives the branch.
    let published = StdArc::new(DomainRoutingSnapshot {
        materialized_stream_reads: runtime.materialized_relay_publications(&placed.domain, &specs),
        materialized_stream_specs: specs,
        ..DomainRoutingSnapshot::default()
    });
    let routing = runtime
        .inner
        .domain_routings
        .get(&placed.domain)
        .verified("the test domain publishes its routing")
        .clone();
    routing.store(StdArc::clone(&published));
    branch.routing_snapshot = Some(published);
    for name in ["profiles", "rules"] {
        let relay = RelayName::parse(name).assured("valid relay");
        branch.reconcile_materialized_state_membership(&relay).await;
        assert!(
            branch.materialized_states.contains_key(&relay),
            "every relay must install its own originating branch capability"
        );
    }
    branch
        .reconcile_materialized_state_membership(
            &RelayName::parse("incoming").assured("valid relay"),
        )
        .await;
    assert_eq!(branch.materialized_states.len(), 2);
}

#[nervix_primitives::test]
async fn rebinding_reseals_the_current_revision_under_its_current_fence() {
    let executor = Executor::new(nervix_execution::ExecutionConfig::default())
        .assured("valid executor budgets");
    let runtime = super::super::Runtime::new();
    let first = row(1, 1, 1);
    let state = Arc::new(ReplicatedMaterializedRelayState::new(
        placement(),
        first.arrow_schema(),
    ));
    let mut predecessor = owner(&state);
    predecessor
        .update_last_by_timestamp(&None, first.clone())
        .assured("current assignment");
    let sealed = predecessor
        .read()
        .seal_after(&executor, &runtime.inner.snapshot_staging, None)
        .await
        .assured("seal succeeds")
        .assured("one revision exists");
    let mut successor = owner(&state);
    assert!(
        predecessor
            .update_last_by_timestamp(&None, row(2, 2, 2))
            .is_err()
    );
    assert!(predecessor.remove_key(&None).is_err());
    let current = successor
        .read()
        .seal_after(&executor, &runtime.inner.snapshot_staging, None)
        .await
        .assured("seal succeeds")
        .assured("one revision exists");
    assert_eq!(current.descriptor.revision, sealed.descriptor.revision);
    assert!(
        current.descriptor.fence > sealed.descriptor.fence,
        "a cached snapshot must describe the current assignment"
    );
    assert_eq!(current.descriptor.fence, successor.read().capture().fence());
    successor
        .update_last_by_timestamp(&None, row(3, 3, 3))
        .assured("successor originates");
}

#[test]
fn bolero_materialized_sequences_preserve_selection_lifetimes_and_complete_snapshots() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .assured("a bounded test runtime builds");
            let executor = Executor::new(nervix_execution::ExecutionConfig::default())
                .assured("valid executor budgets");
            let snapshot_runtime = super::super::Runtime::new();
            let first = row(0, 0, 0);
            let state = Arc::new(ReplicatedMaterializedRelayState::new(
                placement(),
                first.arrow_schema(),
            ));
            let mut originator = owner(&state);
            // One relay is either unbranched or has concrete branch identities throughout its
            // lifecycle. Exercise both layouts while preserving that public container contract.
            let keys = if bytes.first().is_some_and(|byte| byte & 1 == 0) {
                vec![key("acme"), key("beta"), key("gamma")]
            } else {
                vec![None]
            };
            let mut expected = HashMap::<Option<BranchKey>, RuntimeRow>::default();
            let mut revision = 0;
            let mut branch_generation = 0;
            let mut captures = Vec::new();
            for (index, byte) in bytes.iter().enumerate() {
                let branch = &keys[usize::from(byte / 5) % keys.len()];
                match byte % 5 {
                    0 | 1 => {
                        let watermark = i64::from(byte / 16) - 8;
                        let record = row(
                            i64::try_from(index).assured("bounded input"),
                            watermark - i64::from(byte % 2),
                            watermark,
                        );
                        let changes = expected.get(branch).is_none_or(|previous| {
                            (
                                record.metadata().ingested_at_high_watermark(),
                                record.metadata().ingested_at_low_watermark(),
                            ) > (
                                previous.metadata().ingested_at_high_watermark(),
                                previous.metadata().ingested_at_low_watermark(),
                            )
                        });
                        if changes {
                            revision += 1;
                            if !expected.contains_key(branch) {
                                branch_generation += 1;
                            }
                            expected.insert(branch.clone(), record.clone());
                        }
                        assert_eq!(
                            originator
                                .update_last_by_timestamp(branch, record)
                                .assured("current assignment"),
                            changes.then_some(revision)
                        );
                    }
                    2 => {
                        let changed = expected.remove(branch).is_some();
                        if changed {
                            revision += 1;
                            branch_generation += 1;
                        }
                        assert_eq!(
                            originator.remove_key(branch).assured("current assignment"),
                            changed.then_some(revision)
                        );
                    }
                    3 => {
                        captures.push((
                            originator.read().capture(),
                            expected.clone(),
                            branch_generation,
                        ));
                    }
                    _ => {
                        let mut successor = owner(&state);
                        assert!(
                            originator
                                .update_last_by_timestamp(branch, first.clone())
                                .is_err()
                        );
                        assert!(originator.remove_key(branch).is_err());
                        std::mem::swap(&mut originator, &mut successor);
                    }
                }
                assert_eq!(originator.read().current_lsm(), revision);
                assert_eq!(
                    state.branch_generation.load(Ordering::SeqCst),
                    branch_generation
                );
                assert_records(originator.read().capture().records(), &expected);
            }
            captures.push((originator.read().capture(), expected, branch_generation));
            for (captured, expected, branch_generation) in captures {
                assert_records(captured.records(), &expected);
                let sealed = runtime
                    .block_on(captured.seal(&executor, &snapshot_runtime.inner.snapshot_staging))
                    .assured("a current generation seals");
                assert_eq!(sealed.descriptor.revision, captured.revision());
                assert_eq!(sealed.descriptor.fence, captured.fence());
                assert_eq!(sealed.descriptor.branch_generation, branch_generation);
                let restored = runtime
                    .block_on(async {
                        RestoredMaterializedSnapshot::open_relay(
                            &executor,
                            &first.arrow_schema(),
                            SealedSource::artifact(sealed.artifact)
                                .await
                                .assured("the retained sealed artifact opens"),
                        )
                        .await
                    })
                    .assured("a sealed generation opens");
                assert_eq!(restored.revision, captured.revision());
                assert_eq!(restored.fence, captured.fence());
                assert_eq!(restored.branch_generation, branch_generation);
                assert_eq!(restored.records.len(), expected.len());
                for record in &restored.records {
                    assert_row(
                        &record.row,
                        expected
                            .get(&record.branch)
                            .assured("restored branch identity"),
                    );
                }
                let recovered = Arc::new(ReplicatedMaterializedRelayState::restored(
                    placement(),
                    first.arrow_schema(),
                    Some(restored),
                ));
                let recovered_owner = owner(&recovered);
                assert_eq!(recovered_owner.read().current_lsm(), captured.revision());
                assert_records(recovered_owner.read().capture().records(), &expected);
            }
        });
}

#[cfg(feature = "deloxide")]
#[test]
fn deloxide_materialized_publications() {
    let directory =
        std::env::var_os("NERVIX_DEADLOCK_EVIDENCE").map(nervix_deadlock::EvidenceDirectory::new);
    nervix_deadlock::DiagnosticRun::start(directory)
        .assured("the materialized diagnostic process starts its detector once");
    established_updates_retain_membership_and_arrow_payloads_and_keep_timestamp_ties();
    eviction_ends_retained_publications_and_recreation_gets_a_fresh_lifetime();
    rebinding_reseals_the_current_revision_under_its_current_fence();
    materialized_branch_eviction_withdraws_its_record_before_recreation();
    each_materialized_relay_installs_its_branch_owner_in_the_same_routing_epoch();
    bolero_materialized_sequences_preserve_selection_lifetimes_and_complete_snapshots();
}

#[cfg(feature = "shuttle")]
mod shuttle_checks {
    use nervix_model_harness::shuttle::check_interleavings;
    use nervix_primitives::{task, thread};

    use super::*;
    use crate::runtime::*;

    fn capture_update_eviction() {
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement(),
            row(1, 1, 1).arrow_schema(),
        ));
        let mut originator = owner(&state);
        let (acme, beta) = (key("acme"), key("beta"));
        originator
            .update_last_by_timestamp(&acme, row(1, 1, 1))
            .assured("current assignment");
        originator
            .update_last_by_timestamp(&beta, row(4, 4, 4))
            .assured("current assignment");
        let retained = state
            .entries
            .load_full()
            .get(&acme)
            .assured("installed branch")
            .clone();
        let updater = thread::spawn({
            let acme = acme.clone();
            move || {
                originator
                    .update_last_by_timestamp(&acme, row(2, 2, 2))
                    .assured("current assignment");
                originator.remove_key(&acme).assured("current assignment");
                originator
                    .update_last_by_timestamp(&acme, row(3, 0, 0))
                    .assured("current assignment");
            }
        });
        let read = ReplicatedMaterializedRelayState::read(&state);
        let mut captures = Vec::new();
        for _ in 0..2 {
            let captured = read.capture();
            for record in captured.records() {
                if record.branch == acme {
                    let value = record
                        .row
                        .value_at(0)
                        .assured("valid column")
                        .assured("required column");
                    let expected = match value {
                        RuntimeValue::I64(1) => row(1, 1, 1),
                        RuntimeValue::I64(2) => row(2, 2, 2),
                        RuntimeValue::I64(3) => row(3, 0, 0),
                        _ => panic!("capture contains an unpublished value"),
                    };
                    assert_row(&record.row, &expected);
                } else {
                    assert_eq!(record.branch, beta);
                    assert_row(&record.row, &row(4, 4, 4));
                }
            }
            assert!(
                captured
                    .records()
                    .iter()
                    .any(|record| record.branch == beta)
            );
            let expected = captured
                .records()
                .iter()
                .map(|record| (record.branch.clone(), record.row.clone()))
                .collect();
            captures.push((captured, expected));
        }
        updater.join().assured("the branch owner completes");
        assert!(
            retained.load_full().is_none(),
            "eviction ends the exact published lifetime"
        );
        assert_row(
            &read.record(&acme).assured("recreated branch").row,
            &row(3, 0, 0),
        );
        for (captured, expected) in captures {
            assert_records(captured.records(), &expected);
        }
    }

    #[test]
    fn shuttle_materialized_captures_and_eviction_preserve_complete_row_lifetimes() {
        check_interleavings(capture_update_eviction);
    }

    fn assignment_swap() {
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement(),
            row(1, 1, 1).arrow_schema(),
        ));
        let mut predecessor = owner(&state);
        predecessor
            .update_last_by_timestamp(&None, row(1, 1, 1))
            .assured("current assignment");
        let retiring = thread::spawn(move || {
            let result = predecessor.update_last_by_timestamp(&None, row(2, 2, 2));
            assert!(
                matches!(result, Ok(Some(2)) | Err(_)),
                "only the predecessor's own generation authorizes its replacement"
            );
        });
        let mut successor = owner(&state);
        successor
            .update_last_by_timestamp(&None, row(3, 3, 3))
            .assured("successor assignment");
        let captured = successor.read().capture();
        assert_eq!(captured.fence(), state.assignment.current_binding().fence());
        assert_row(&captured.records()[0].row, &row(3, 3, 3));
        retiring
            .join()
            .assured("the predecessor's admitted operation finishes");
        assert_row(
            &successor.read().record(&None).assured("current row").row,
            &row(3, 3, 3),
        );
    }

    #[test]
    fn shuttle_materialized_rebinding_fences_the_previous_writer() {
        check_interleavings(assignment_swap);
    }

    fn required_wait_registration() {
        shuttle::future::block_on(async {
            let runtime = Runtime::default();
            let placed = placement();
            let relay = RelayName::parse("profiles").assured("valid relay");
            publish_state_identity(
                &runtime,
                &placed.domain,
                ModelKind::Relay,
                placed.identifier.clone(),
            );
            let state_schema = row(1, 1, 1).arrow_schema();
            let mut assignment = runtime
                .replicated_materialized_stream_state(
                    placed.clone(),
                    state_schema.clone(),
                    None,
                    Vec::new(),
                    None,
                )
                .assured("empty state installs");
            let mut originator = assignment.originator.take().assured("local origination");
            let specs = HashMap::from_iter([(
                relay.clone(),
                RuntimeMaterializedRelaySpec::new(
                    state_schema,
                    VmSchemaSensitivity::default(),
                    ResolvedBranching::unbranched(),
                ),
            )]);
            let routing = DomainRoutingSnapshot {
                materialized_stream_reads: runtime
                    .materialized_relay_publications(&placed.domain, &specs),
                materialized_stream_specs: specs,
                ..DomainRoutingSnapshot::default()
            };
            let publisher = task::spawn({
                let runtime = runtime.clone();
                async move {
                    runtime
                        .apply_materialized_stream_records(&mut originator, &None, [row(7, 7, 7)])
                        .await
                        .assured("current writer publishes");
                }
            });
            loop {
                let (resolution, changed) = runtime
                    .observe_materialized_dependencies(
                        &routing,
                        &placed.domain,
                        &None,
                        &[nervix_models::MaterializedStateDependency {
                            relay: relay.clone(),
                            policy: nervix_models::MaterializedStatePolicy::RequiredWait,
                        }],
                        Timestamp::from_unix_nanos(8),
                    )
                    .await
                    .assured("the required observation succeeds");
                match resolution {
                    MaterializedDependencyResolution::Ready(values) => {
                        assert_eq!(
                            values.get("relay_state.profiles.value"),
                            Some(&RuntimeValue::I64(7))
                        );
                        break;
                    }
                    MaterializedDependencyResolution::Wait => changed.await,
                    MaterializedDependencyResolution::Skip => {
                        panic!("REQUIRED WAIT must retain its input")
                    }
                }
            }
            publisher
                .await
                .assured("publication finishes without a polling timer");
        });
    }

    #[test]
    fn shuttle_materialized_required_wait_registers_before_reading_publications() {
        check_interleavings(required_wait_registration);
    }
}
