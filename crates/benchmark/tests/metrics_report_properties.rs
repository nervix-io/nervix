//! Complete current benchmark metric reports through their production file codec.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** Bounded report cases, bit-exact round trips and oracle weakening controls.
//! - **Depends on.** The public report writer/reader, vocabulary units and bounded test entropy.
//! - **Must not know.** Running benchmarks, live metrics, placement or connector implementations.

use meticulous::ResultExt as _;
use nervix_arbitrary::Entropy;
use nervix_benchmark::{BatchTargetMetrics, NervixMetricsReport, RelayBufferMetrics};
use nervix_models::{ClusterNodeName, Unit};

// TOML's integer representation is signed, including the payload of a Unit::Instance.
const MAX_TOML_INTEGER: u64 = i64::MAX.cast_unsigned();

struct Reports<'a> {
    entropy: Entropy<'a>,
}

impl<'a> Reports<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            entropy: Entropy::new(bytes),
        }
    }

    fn text(&mut self) -> String {
        let length = self.entropy.up_to(12);
        let mut text = String::new();
        for _ in 0..length {
            let byte = self.entropy.byte();
            text.push(match byte % 8 {
                0 => '"',
                1 => '\\',
                2 => '\n',
                3 => '\r',
                4 => '\t',
                5 => 'λ',
                6 => '🦀',
                _ => char::from(byte),
            });
        }
        text
    }

    fn physical_node(&mut self) -> Option<ClusterNodeName> {
        if !self.entropy.flag() {
            return None;
        }
        let index = self.entropy.up_to(u64::from(u32::MAX));
        Some(
            ClusterNodeName::parse(&format!("node-{index}"))
                .assured("node- followed by decimal digits is a valid cluster node name"),
        )
    }

    fn unit(&mut self, index: usize) -> Unit {
        match index % 3 {
            0 => Unit::SINGLE,
            1 => {
                let partition = self.entropy.boundary_biased(0..=u64::from(u32::MAX));
                Unit::partition(
                    u32::try_from(partition).assured("the generated partition is at most u32::MAX"),
                )
            }
            _ => {
                const { assert!(MAX_TOML_INTEGER <= Unit::MAX_INSTANCE_INDEX) };
                let instance = self.entropy.boundary_biased(0..=MAX_TOML_INTEGER);
                Unit::try_instance(instance)
                    .assured("every nonnegative TOML integer fits the instance identity range")
            }
        }
    }

    fn percentile(&mut self) -> f64 {
        match self.entropy.byte() % 10 {
            0 => -0.0,
            1 => 0.0,
            2 => f64::from_bits(1),
            3 => f64::from_bits(0x000f_ffff_ffff_ffff), // Largest IEEE 754 subnormal.
            4 => f64::MIN_POSITIVE,
            5 => 1.0 + f64::EPSILON,
            6 => f64::MAX,
            _ => f64::from_bits(self.entropy.up_to(f64::MAX.to_bits())),
        }
    }

    fn percentiles(&mut self) -> [f64; 3] {
        let mut values = [self.percentile(), self.percentile(), self.percentile()];
        values.sort_by(f64::total_cmp);
        values
    }

    fn report(&mut self) -> NervixMetricsReport {
        let domain = self.text();
        let target = self.text();
        let relay = self.text();
        let mut batch_targets = Vec::new();
        let target_count = self.entropy.between(3..=6);
        for index in 0..target_count {
            let unit =
                self.unit(usize::try_from(index).assured("at most six series are generated"));
            let [p50, p90, p99] = self.percentiles();
            batch_targets.push(BatchTargetMetrics {
                domain: domain.clone(),
                target_kind: self.text(),
                target: target.clone(),
                physical_node_id: self.physical_node(),
                unit,
                direction: self.text(),
                relay: relay.clone(),
                messages_total: self.entropy.boundary_biased(0..=MAX_TOML_INTEGER),
                batches_total: self.entropy.boundary_biased(1..=MAX_TOML_INTEGER),
                p50,
                p90,
                p99,
            });
        }
        let mut relay_buffers = Vec::new();
        let relay_count = self.entropy.between(3..=6);
        for index in 0..relay_count {
            let unit =
                self.unit(usize::try_from(index).assured("at most six series are generated"));
            let [p50, p90, p99] = self.percentiles();
            relay_buffers.push(RelayBufferMetrics {
                domain: domain.clone(),
                relay: relay.clone(),
                physical_node_id: self.physical_node(),
                unit,
                direction: self.text(),
                observations: self.entropy.boundary_biased(1..=MAX_TOML_INTEGER),
                p50,
                p90,
                p99,
            });
        }
        NervixMetricsReport {
            batch_targets,
            relay_buffers,
        }
    }
}

fn assert_complete_report(actual: &NervixMetricsReport, expected: &NervixMetricsReport) {
    // Derived equality includes every current field and both series' lengths and order. It does
    // not distinguish signed zeros, so compare all six persisted percentile fields by bits too.
    assert_eq!(actual, expected, "complete metric report");
    for (actual, expected) in actual.batch_targets.iter().zip(&expected.batch_targets) {
        assert_eq!(
            [actual.p50, actual.p90, actual.p99].map(f64::to_bits),
            [expected.p50, expected.p90, expected.p99].map(f64::to_bits),
            "batch percentile bits"
        );
    }
    for (actual, expected) in actual.relay_buffers.iter().zip(&expected.relay_buffers) {
        assert_eq!(
            [actual.p50, actual.p90, actual.p99].map(f64::to_bits),
            [expected.p50, expected.p90, expected.p99].map(f64::to_bits),
            "relay percentile bits"
        );
    }
}

fn round_trip(report: &NervixMetricsReport) {
    let directory = tempfile::tempdir().assured("the test can create its report directory");
    let path = directory.path().join("nervix-metrics.toml");
    report
        .write(&path)
        .assured("the generated current report is valid and TOML-representable");
    let decoded = NervixMetricsReport::read(&path)
        .assured("the production reader accepts the production writer's complete report");
    assert_complete_report(&decoded, report);
}

#[test]
fn bolero_metrics_report_round_trip() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| round_trip(&Reports::new(bytes).report()));
}

#[test]
fn corpus_boundaries_round_trip() {
    let seeds: &[&[u8]] = &[
        include_bytes!("__fuzz__/bolero_metrics_report_round_trip/corpus/minimal"),
        include_bytes!("__fuzz__/bolero_metrics_report_round_trip/corpus/integer-boundaries"),
        include_bytes!("__fuzz__/bolero_metrics_report_round_trip/corpus/extreme-quantiles"),
        include_bytes!("__fuzz__/bolero_metrics_report_round_trip/corpus/mixed-bits-and-labels"),
    ];
    for seed in seeds {
        round_trip(&Reports::new(seed).report());
    }
}

#[test]
fn exact_integer_float_and_identity_boundaries_round_trip() {
    let mut report = Reports::new(&[]).report();
    let percentiles = [
        [-0.0, 0.0, f64::from_bits(1)],
        [
            f64::from_bits(0x000f_ffff_ffff_ffff),
            f64::MIN_POSITIVE,
            1.0 + f64::EPSILON,
        ],
        [9_007_199_254_740_991.0, 9_007_199_254_740_992.0, f64::MAX],
    ];
    let units = [
        Unit::SINGLE,
        Unit::partition(u32::MAX),
        Unit::try_instance(MAX_TOML_INTEGER)
            .assured("the largest nonnegative TOML integer fits an instance identity"),
    ];
    for (index, target) in report.batch_targets.iter_mut().enumerate() {
        target.domain = "domain-λ\n\"\\".into();
        target.target_kind = "INGESTOR".into();
        target.target = "shared-target".into();
        target.direction = "sent".into();
        target.relay = "shared-relay".into();
        target.physical_node_id = Some(
            ClusterNodeName::parse(&format!("node-{index}"))
                .assured("node- followed by decimal digits is a valid cluster node name"),
        );
        target.unit = units[index];
        target.messages_total = MAX_TOML_INTEGER;
        target.batches_total = MAX_TOML_INTEGER;
        [target.p50, target.p90, target.p99] = percentiles[index];
    }
    // Keep two otherwise identical logical series distinct solely by their execution unit.
    report.batch_targets[2].physical_node_id = report.batch_targets[1].physical_node_id.clone();
    report.batch_targets[0].messages_total = 0;
    report.batch_targets[0].batches_total = 1;
    for (index, relay) in report.relay_buffers.iter_mut().enumerate() {
        relay.unit = units[index];
        relay.observations = MAX_TOML_INTEGER;
        [relay.p50, relay.p90, relay.p99] = percentiles[index];
    }
    round_trip(&report);
}

#[test]
#[should_panic(expected = "complete metric report")]
fn oracle_detects_lost_batch_partition() {
    let expected = Reports::new(&[]).report();
    let mut actual = expected.clone();
    actual.batch_targets[1].unit = Unit::SINGLE;
    assert_complete_report(&actual, &expected);
}

#[test]
#[should_panic(expected = "complete metric report")]
fn oracle_detects_lost_relay_instance() {
    let expected = Reports::new(&[]).report();
    let mut actual = expected.clone();
    actual.relay_buffers[2].unit = Unit::SINGLE;
    assert_complete_report(&actual, &expected);
}

#[test]
#[should_panic(expected = "complete metric report")]
fn oracle_detects_lost_physical_node() {
    let mut expected = Reports::new(&[]).report();
    expected.batch_targets[0].physical_node_id =
        Some(ClusterNodeName::parse("node-1").assured("node-1 is a valid cluster node name"));
    let mut actual = expected.clone();
    actual.batch_targets[0].physical_node_id = None;
    assert_complete_report(&actual, &expected);
}

#[test]
#[should_panic(expected = "batch percentile bits")]
fn oracle_detects_lost_signed_zero() {
    let expected = Reports::new(&[]).report();
    let mut actual = expected.clone();
    actual.batch_targets[0].p50 = 0.0;
    assert_complete_report(&actual, &expected);
}
