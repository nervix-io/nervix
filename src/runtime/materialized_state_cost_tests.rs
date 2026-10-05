//! Layer: test harness.
//! Owns: same-host materialized row/batch cost and retained carrier measurements.
//! May depend on: the production materialized owner, jemalloc counters and monotonic instants.
//! Must not know: model-checker scheduling or publication backend internals.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{DomainName, FieldName, ModelKind, ModelName, SchemaFingerprint, Timestamp};
use nervix_primitives::{sync::blocking::Barrier, thread, time::Instant};

use super::*;
use crate::{
    runtime::RuntimeState,
    runtime_schema::{RuntimeValue, test_runtime_row},
};

#[test]
#[ignore = "same-host materialized row/batch allocation and captured-generation measurement"]
fn materialized_owner_cost() {
    let allocated =
        tikv_jemalloc_ctl::thread::allocatedp::read().assured("the server uses jemalloc");
    let deallocated =
        tikv_jemalloc_ctl::thread::deallocatedp::read().assured("the server uses jemalloc");
    let record = test_runtime_row([(
        "payload".to_string(),
        RuntimeValue::String("p".repeat(65_536)),
    )]);
    for branches in [1, 256, 4096] {
        let keys = (0..branches)
            .map(|index| {
                Some(
                    BranchKey::from_fields([(
                        FieldName::parse("tenant").assured("valid field"),
                        RuntimeValue::I64(index),
                    )])
                    .assured("one typed field"),
                )
            })
            .collect::<Vec<_>>();
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            RuntimeStatePlacement {
                domain: DomainName::parse("measurement").assured("valid domain"),
                state: RuntimeState::MaterializedRelay {
                    schema: SchemaFingerprint::from_digest([7; 32]),
                },
                kind: ModelKind::Relay,
                identifier: ModelName::parse("profiles").assured("valid relay"),
                branch_key: None,
            },
            record.arrow_schema(),
        ));
        let mut originator = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        )
        .originator
        .assured("local originator");
        for key in &keys {
            originator
                .update_last_by_timestamp(key, record.clone())
                .assured("current assignment");
        }
        let capture = originator.read().capture();
        let retained_before = allocated.get() - deallocated.get();
        let mut sequence = 1_i64;
        for batch_size in [1, 128] {
            for sample in 0..5 {
                let mut timings = Vec::with_capacity(10_000);
                let before = allocated.get();
                let start = Instant::now();
                for batch in 0_usize..10_000 / batch_size {
                    let batch_start = Instant::now();
                    for offset in 0..batch_size {
                        let selected = &keys[(batch * batch_size + offset) % keys.len()];
                        let replacement = record
                            .clone()
                            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(sequence));
                        sequence += 1;
                        std::hint::black_box(
                            originator
                                .update_last_by_timestamp(selected, replacement)
                                .assured("current assignment"),
                        );
                    }
                    timings.push(batch_start.elapsed().as_nanos());
                }
                let elapsed = start.elapsed();
                let bytes = allocated.get() - before;
                timings.sort_unstable();
                let operations = timings.len() * batch_size;
                println!(
                    "materialized-owner branches={branches} batch_size={batch_size} \
                     sample={sample} rows={operations} ns_per_row={} p50_batch_ns={} \
                     p95_batch_ns={} p99_batch_ns={} allocated_bytes={bytes}",
                    elapsed.as_nanos() / u128::try_from(operations).assured("bounded operations"),
                    timings[timings.len() / 2],
                    timings[timings.len() * 95 / 100],
                    timings[timings.len() * 99 / 100]
                );
                assert!(
                    bytes <= u64::try_from(operations).assured("bounded operations") * 128,
                    "publication allocates only a bounded row view, never the 64 KiB Arrow payload"
                );
            }
        }
        let rendezvous = Arc::new(Barrier::new(2));
        let reader = thread::spawn({
            let read = originator.read().clone();
            let rendezvous = rendezvous.clone();
            move || {
                rendezvous.wait();
                for _ in 0..16 {
                    std::hint::black_box(read.capture());
                }
            }
        });
        rendezvous.wait();
        let overlap = Instant::now();
        for index in 0..10_000 {
            sequence += 1;
            originator
                .update_last_by_timestamp(
                    &keys[index % keys.len()],
                    record
                        .clone()
                        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(sequence)),
                )
                .assured("current assignment");
        }
        let update_elapsed = overlap.elapsed();
        reader
            .join()
            .assured("the bounded capture participant finishes");
        println!(
            "materialized-overlap branches={branches} rows=10000 captures=16 ns_per_row={} \
             combined_ns={}",
            update_elapsed.as_nanos() / 10_000,
            overlap.elapsed().as_nanos()
        );
        assert_eq!(capture.records().len(), keys.len());
        for captured in capture.records() {
            assert!(Arc::ptr_eq(captured.row.batch(), record.batch()));
        }
        assert_eq!(
            Arc::strong_count(record.batch()),
            keys.len() * 2 + 1,
            "only the current row and the retained capture keep one carrier view per branch"
        );
        println!(
            "materialized-owner branches={branches} retained_delta_bytes={} captured_rows={} \
             carrier_references={}",
            (allocated.get() - deallocated.get()).abs_diff(retained_before),
            capture.records().len(),
            Arc::strong_count(record.batch())
        );
    }
}
