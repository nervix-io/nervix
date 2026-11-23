//! Same-host cost of inspecting materialized relay state near the session reply bound while the
//! owner keeps applying records.
//!
//! Layer: test harness.
//! - **Owns.** The overlap measurement of record application and owner-side page captures.
//! - **Depends on.** The production materialized owner, its owner-side inspection page and
//!   monotonic instants.
//! - **Must not know.** Sessions, the interconnect or publication backend internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use ahash::HashMap;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    FieldName, ModelKind, RelayName, ResolvedBranching, SchemaFingerprint, SessionBounds,
    StateInspectionBranchSelection, StateInspectionSettings, Timestamp, Unit,
};
use nervix_primitives::{
    sync::{
        StdArc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use super::{MaterializedPageBounds, MaterializedPageQuery};
use crate::{
    runtime::{
        BranchKey, DomainRoutingSnapshot, Runtime, RuntimeMaterializedRelaySpec, RuntimeState,
        RuntimeStatePlacement, VmSchemaSensitivity, domain, install_unpaced_test_domain, named,
        publish_state_identity, test_schema,
    },
    runtime_schema::{RuntimeValue, batch_payload_bytes, test_runtime_row},
};

/// Branches the relay holds, one row each. At the default row bound a page selects every branch.
const BRANCHES: i64 = 1024;
/// The payload of each row: 1024 rows of 63 KiB come to 63 MiB, just under the session's 64 MiB
/// transfer bound, which bounds an inspection reply.
const PAYLOAD_BYTES: usize = 63 * 1024;
/// Record applications measured in each sample.
const UPDATES: usize = 4096;
/// Samples of each configuration.
const SAMPLES: usize = 5;

#[test]
#[ignore = "same-host materialized inspection cost beside the owner's record application"]
fn state_inspection_cost_materialized_relay() {
    // The measured owner runs on the calling thread and the spawned reader on a worker, so the
    // reader's polling never takes the measured thread.
    let executor = nervix_primitives::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .assured("the measurement runtime starts");
    executor.block_on(measure());
}

async fn measure() {
    let runtime = Runtime::default();
    let domain = domain("inspection_cost");
    let relay = named::<RelayName>("profiles");
    install_unpaced_test_domain(&runtime, &domain);
    publish_state_identity(&runtime, &domain, ModelKind::Relay, relay.clone());
    let schema = test_schema(&[("payload", nervix_models::ParseAsType::String)]);
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: relay.clone().into(),
        unit: Unit::SINGLE,
        branch_key: None,
    };
    let mut assignment = runtime.replicated_materialized_stream_state(
        placement,
        schema.arrow_schema(),
        None,
        Vec::new(),
        None,
    );
    let mut state = assignment
        .originator
        .take()
        .assured("a local assignment originates its state");
    let keys = (0..BRANCHES)
        .map(|index| {
            Some(
                BranchKey::from_fields([(
                    FieldName::parse("tenant").assured("a valid field name"),
                    RuntimeValue::I64(index),
                )])
                .assured("one typed key field"),
            )
        })
        .collect::<Vec<_>>();
    // Every application replaces a branch's row with a new view of one payload, as the owner keeps
    // the batch a record arrived in, so the measurement times the application, not a payload copy.
    let base = test_runtime_row([(
        "payload".to_string(),
        RuntimeValue::String("p".repeat(PAYLOAD_BYTES)),
    )]);
    let mut sequence = 1_i64;
    let record = |sequence: i64| {
        base.clone()
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(sequence))
    };
    for key in &keys {
        runtime
            .apply_materialized_stream_records(&mut state, key, [record(sequence)])
            .await
            .assured("the owner applies the initial row of every branch");
        sequence += 1;
    }
    let specs = [(
        relay.clone(),
        RuntimeMaterializedRelaySpec::new(
            schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            ResolvedBranching::unbranched(),
        ),
    )]
    .into_iter()
    .collect();
    let routing = DomainRoutingSnapshot {
        materialized_stream_reads: runtime.materialized_relay_publications(
            &domain,
            &specs,
            &HashMap::default(),
        ),
        materialized_stream_specs: specs,
        ..DomainRoutingSnapshot::default()
    };
    runtime
        .inner
        .domain_routings
        .get(&domain)
        .assured("the fixture installed the domain's routing")
        .store(StdArc::new(routing));
    let rows = StateInspectionSettings::default().max_rows.value().get();
    let bytes = SessionBounds::default().transfer_bytes().get();
    let query = move || MaterializedPageQuery {
        unit: Unit::SINGLE,
        selection: StateInspectionBranchSelection::All,
        after: None,
        bounds: MaterializedPageBounds { rows, bytes },
        catalog_requested: false,
        filter: None,
        reply: crate::runtime::MaterializedPageReply::Local,
    };

    // A page the reader holds keeps the generation it captured however far the owner moves on.
    let held = runtime
        .capture_materialized_state_page(&domain, &relay, query())
        .await
        .assured("the owner answers a page")
        .page;
    let held_revision = held.generation.revision();
    let held_bytes: u64 = held
        .generation
        .records()
        .iter()
        .map(|record| {
            batch_payload_bytes(&record.row.batch().batch().slice(record.row.row_index(), 1))
        })
        .sum();
    assert_eq!(
        held.generation.records().len(),
        keys.len(),
        "the page selects every branch at the default row bound"
    );
    assert!(
        held_bytes <= bytes && held_bytes >= bytes / 10 * 9,
        "the page holds {held_bytes} bytes, within a tenth of the {bytes}-byte transfer bound"
    );

    // Each sample measures both configurations back to back, so a change in the host's load
    // between samples reaches both alike.
    for sample in 0..SAMPLES {
        for inspected in [false, true] {
            let stop = StdArc::new(AtomicBool::new(false));
            let reader = inspected.then(|| {
                nervix_primitives::task::spawn({
                    let runtime = runtime.clone();
                    let domain = domain.clone();
                    let relay = relay.clone();
                    let stop = stop.clone();
                    async move {
                        let mut pages = 0_u64;
                        let mut page_rows = 0_usize;
                        while !stop.load(Ordering::SeqCst) {
                            let page = runtime
                                .capture_materialized_state_page(&domain, &relay, query())
                                .await
                                .assured("the owner answers every page")
                                .page;
                            page_rows += page.generation.records().len();
                            pages += 1;
                        }
                        (pages, page_rows)
                    }
                })
            });
            let mut timings = Vec::with_capacity(UPDATES);
            let whole = Instant::now();
            for update in 0..UPDATES {
                let started = Instant::now();
                runtime
                    .apply_materialized_stream_records(
                        &mut state,
                        &keys[update % keys.len()],
                        [record(sequence)],
                    )
                    .await
                    .assured("the owner applies every measured row");
                sequence += 1;
                timings.push(started.elapsed().as_nanos());
            }
            let elapsed = whole.elapsed();
            stop.store(true, Ordering::SeqCst);
            let (pages, page_rows) = match reader {
                Some(reader) => reader.await.assured("the reader ends with its pages"),
                None => (0, 0),
            };
            timings.sort_unstable();
            println!(
                "state-inspection-materialized inspected={inspected} sample={sample} \
                 updates={UPDATES} ns_per_update={} p50_ns={} p99_ns={} pages={pages} \
                 page_rows={page_rows} page_bytes={held_bytes} transfer_bound={bytes}",
                elapsed.as_nanos() / u128::try_from(UPDATES).assured("a fixed count fits u128"),
                timings[timings.len() / 2],
                timings[timings.len() * 99 / 100]
            );
            if inspected {
                assert!(
                    pages > 0,
                    "the reader captured pages while the owner applied records"
                );
            }
        }
    }
    assert_eq!(
        held.generation.revision(),
        held_revision,
        "a captured page never sees the owner's later applications"
    );
    let latest = runtime
        .capture_materialized_state_page(&domain, &relay, query())
        .await
        .assured("the owner answers after the measurement")
        .page;
    assert!(
        latest.generation.revision() > held_revision,
        "the owner published past the generation the reader held, without waiting for it"
    );
}
