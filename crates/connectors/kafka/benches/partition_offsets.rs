//! Criterion cost of folding acknowledged and rejected Kafka positions per partition.
//!
//! A source folds every acknowledgement batch to each partition's latest offset, and every
//! rejection to each partition's earliest message. Positions spread over one partition per topic
//! mostly repeat a partition; positions spread over as many partitions as there are positions each
//! name a new one. Both topics repeat throughout.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::ResultExt as _;
use nervix_connector_kafka::{KafkaOffsetPosition, earliest_message_offsets, latest_positions};

/// The positions each measurement folds, as one acknowledgement batch carries them.
const POSITIONS: usize = 1_024;

/// `POSITIONS` positions over two topics and `partitions` partitions, with increasing offsets.
fn positions(partitions: usize) -> Vec<KafkaOffsetPosition> {
    let mut positions = Vec::with_capacity(POSITIONS);
    for index in 0..POSITIONS {
        positions.push(KafkaOffsetPosition {
            topic: format!("topic-{}", index % 2),
            partition: i32::try_from(index % partitions)
                .assured("the benchmark uses at most 1,024 partitions"),
            offset: i64::try_from(index + 1).assured("the benchmark uses at most 1,024 offsets"),
        });
    }
    positions
}

fn partition_offsets(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("kafka_partition_offsets");
    group.throughput(Throughput::Elements(
        u64::try_from(POSITIONS).assured("the position count fits u64"),
    ));
    for partitions in [1, 32, POSITIONS] {
        let positions = positions(partitions);
        group.bench_with_input(
            BenchmarkId::new("latest", partitions),
            &positions,
            |bench, positions| bench.iter(|| black_box(latest_positions(black_box(positions)))),
        );
        group.bench_with_input(
            BenchmarkId::new("earliest", partitions),
            &positions,
            |bench, positions| {
                bench.iter(|| black_box(earliest_message_offsets(black_box(positions))))
            },
        );
    }
    group.finish();
}

criterion_group!(benches, partition_offsets);
criterion_main!(benches);
