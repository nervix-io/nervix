//! The cost of building and evaluating the input batch for a literal node tag selector.
//!
//! Layer: test harness.
//! - **Owns.** Measuring the production node predicate over fixed populations of records.
//! - **Depends on.** The VM selector, Plexus expression parser and vocabulary record fixtures.
//! - **Must not know.** Live cluster membership or scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "measures node tag selection over fixed fixtures")
)]

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, Throughput, black_box};
use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{ClusterNodeIdentity, ClusterNodeName, CommittedNodeRecord, NodeTags};
use nervix_vm::NodePredicate;

fn records(count: u64) -> BTreeMap<ClusterNodeName, CommittedNodeRecord> {
    let fixture = Arbitrary::new(&[], Domain::Vocabulary).committed_node_record();
    (0..count)
        .map(|index| {
            let name = ClusterNodeName::parse(&format!("node-{index:03}"))
                .assured("the fixture uses a canonical node name");
            let mut record = fixture.clone();
            record.record.claim.identity =
                ClusterNodeIdentity::new(name.clone(), record.record.identity().incarnation());
            let tags = match index % 3 {
                0 => Some("zone=eu"),
                1 => Some("zone=ap"),
                _ => None,
            };
            record.record.tags = NodeTags::from_assignments(tags)
                .assured("the benchmark uses valid tag assignments");
            (name, record)
        })
        .collect()
}

pub(super) fn node_tag_benches(criterion: &mut Criterion) {
    let source = nervix_plexus::parse_expression("tag('zone') = 'eu'")
        .assured("the benchmark uses a valid tag expression");
    let predicate =
        NodePredicate::compile(&source).assured("the benchmark uses a literal tag selector");
    let mut group = criterion.benchmark_group("node_selectors");
    for count in [1, 3, 64] {
        let records = records(count);
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(
            BenchmarkId::new("tag", count),
            &records,
            |bench, records| {
                bench.iter(|| {
                    black_box(
                        predicate
                            .evaluate(black_box(records))
                            .assured("the tag selector evaluates"),
                    )
                });
            },
        );
    }
    group.finish();
}
