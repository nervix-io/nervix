//! Bounded mergeable sketches owned by one branch's epoch-aligned window panes.
//!
//! Layer: data plane.
//!
//! - **Owns.** HLL registers, bounded quantile centroids, and bounded frequency candidates for
//!   retained pane rows, and deterministic merges when a window emits.
//! - **Depends on.** Validated sketch plans and typed argument columns.
//! - **Must not know.** Relays, placement, or how a branch snapshot is stored.

use std::{cmp::Ordering, collections::BTreeMap};

use arrow_array::{Float64Array, Int64Array};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use error_stack::ResultExt as _;
use ordered_float::OrderedFloat;
use sorted_vec::SortedVec;

use super::*;

#[derive(Debug, Clone)]
pub(in crate::runtime) struct PaneSketches {
    config: WindowSketchConfig,
    layout: WindowPaneLayout,
    panes: BTreeMap<i64, WindowSketch>,
}

#[derive(Debug, Clone)]
enum WindowSketch {
    Distinct(HyperLogLog),
    Quantile(BoundedTDigest),
    TopK(FrequencyCandidates),
}

impl WindowSketch {
    fn new(config: WindowSketchConfig) -> Self {
        match config {
            WindowSketchConfig::Distinct { precision } => {
                Self::Distinct(HyperLogLog::new(precision))
            }
            WindowSketchConfig::Quantile { capacity } => {
                Self::Quantile(BoundedTDigest::new(capacity.get()))
            }
            WindowSketchConfig::TopK { capacity, .. } => {
                Self::TopK(FrequencyCandidates::new(capacity.get()))
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        let merged = match (self, other) {
            (Self::Distinct(left), Self::Distinct(right)) => {
                left.merge(right);
                Some(())
            }
            (Self::Quantile(left), Self::Quantile(right)) => {
                left.merge(right);
                Some(())
            }
            (Self::TopK(left), Self::TopK(right)) => {
                left.merge(right);
                Some(())
            }
            _ => None,
        };
        merged.verified("a single demand has one validated sketch config");
    }
}

impl PaneSketches {
    pub(super) fn new(config: WindowSketchConfig, layout: WindowPaneLayout) -> Self {
        Self {
            config,
            layout,
            panes: BTreeMap::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.panes.clear();
    }

    pub(super) fn admit<R: RetainedWindowRows>(
        &mut self,
        demand: usize,
        rows: &R,
        positions: Range<usize>,
    ) {
        for position in positions {
            let retained = rows.retained_row(position);
            let argument = retained.arguments.demand(demand).first();
            if !argument.is_present(retained.row) {
                continue;
            }
            let pane = self.layout.pane_of(retained.timestamp.unix_nanos());
            let sketch = self
                .panes
                .entry(pane)
                .or_insert_with(|| WindowSketch::new(self.config));
            match sketch {
                WindowSketch::Distinct(hll) => {
                    let key = argument
                        .sketch_key(retained.row)
                        .verified("distinct accepts only scalar typed arguments");
                    hll.admit(&key);
                }
                WindowSketch::Quantile(quantile) => {
                    let value = argument
                        .number_at(retained.row)
                        .verified("quantile accepts only present numeric arguments");
                    quantile.admit(value);
                }
                WindowSketch::TopK(top) => {
                    let key = argument
                        .sketch_key(retained.row)
                        .verified("top-k accepts only scalar typed arguments");
                    top.admit(key);
                }
            }
        }
    }

    /// A sketch cannot subtract a row. Rebuild from survivors, which also discards expired panes.
    pub(super) fn retain_after<R: RetainedWindowRows>(
        &mut self,
        demand: usize,
        rows: &R,
        removed: usize,
    ) {
        if removed == 0 {
            return;
        }
        self.clear();
        self.admit(demand, rows, removed..rows.retained());
    }

    pub(super) fn evaluate<R: RetainedWindowRows>(
        &self,
        demand: usize,
        invocation: &WindowAggregateInvocation,
        output_type: &ArrowDataType,
        rows: &R,
    ) -> error_stack::Result<ArrayRef, WindowProcessorError> {
        let mut merged = WindowSketch::new(self.config);
        for pane in self.panes.values() {
            merged.merge(pane);
        }
        match merged {
            WindowSketch::Distinct(hll) => Ok(StdArc::new(Int64Array::from(vec![hll.estimate()?]))),
            WindowSketch::Quantile(centroids) => {
                let value = centroids.quantile(
                    invocation
                        .percentile
                        .verified("quantile invocations carry a percentage"),
                );
                if let Some(value) = value
                    && !value.is_finite()
                {
                    return Err(Report::new(WindowProcessorError::StatisticNotFinite {
                        function: WindowAggregateFunction::ApproxQuantile,
                    }));
                }
                Ok(StdArc::new(Float64Array::from(vec![value])))
            }
            WindowSketch::TopK(candidates) => {
                let k = match self.config {
                    WindowSketchConfig::TopK { k, .. } => Some(k),
                    _ => None,
                }
                .verified("a top-k sketch is built only from its top-k config");
                let mut ranked = candidates.counts.into_iter().collect::<Vec<_>>();
                ranked.sort_by(|(left_key, left_count), (right_key, right_count)| {
                    right_count
                        .cmp(left_count)
                        .then_with(|| left_key.cmp(right_key))
                });
                ranked.truncate(k.get());
                let mut pending = BTreeMap::new();
                for (index, (key, _)) in ranked.iter().enumerate() {
                    pending.insert(key.as_slice(), index);
                }
                let mut found = vec![None; ranked.len()];
                for position in 0..rows.retained() {
                    let retained = rows.retained_row(position);
                    let column = retained.arguments.demand(demand).first();
                    if let Some(key) = column.sketch_key(retained.row)
                        && let Some(index) = pending.remove(key.as_slice())
                    {
                        // Keep candidate values columnar by slicing their original Arrow column.
                        found[index] = Some(column.slice(retained.row));
                        if pending.is_empty() {
                            break;
                        }
                    }
                }
                let arrays = found
                    .into_iter()
                    .map(|array| {
                        array.verified(
                            "every frequency candidate came from a retained row of this demand",
                        )
                    })
                    .collect::<Vec<ArrayRef>>();
                let item = match output_type {
                    ArrowDataType::List(item) => Some(item),
                    _ => None,
                }
                .verified("top-k compiles to a list of its argument type");
                let values = if arrays.is_empty() {
                    new_empty_array(item.data_type())
                } else {
                    let refs = arrays
                        .iter()
                        .map(|array| array.as_ref())
                        .collect::<Vec<_>>();
                    concat_arrow_arrays(&refs)
                        .change_context(WindowProcessorError::BuildAggregateOutput)?
                };
                let length = i32::try_from(values.len()).assured("top-k capacity is at most 4096");
                Ok(StdArc::new(
                    ListArray::try_new(
                        item.clone(),
                        OffsetBuffer::new(ScalarBuffer::from(vec![0, length])),
                        values,
                        None,
                    )
                    .change_context(WindowProcessorError::BuildAggregateOutput)?,
                ))
            }
        }
    }
}

#[derive(Debug, Clone)]
struct HyperLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl HyperLogLog {
    fn new(precision: u8) -> Self {
        Self {
            precision,
            registers: vec![0; 1_usize << precision],
        }
    }
    fn admit(&mut self, key: &[u8]) {
        let digest = blake3::hash(key);
        let hash = u64::from_le_bytes(
            digest.as_bytes()[..8]
                .try_into()
                .assured("a BLAKE3 digest has eight initial bytes"),
        );
        let suffix_bits = 64_u8
            .checked_sub(self.precision)
            .assured("validated HLL precision is between 4 and 16");
        let index =
            usize::try_from(hash >> suffix_bits).assured("HLL precision is at most 16 bits");
        let remaining = hash << self.precision;
        let rank = (remaining.leading_zeros() + 1).min(u32::from(suffix_bits) + 1);
        self.registers[index] =
            self.registers[index].max(u8::try_from(rank).assured("HLL rank is at most 65"));
    }
    fn merge(&mut self, other: &Self) {
        for (left, right) in self.registers.iter_mut().zip(&other.registers) {
            *left = (*left).max(*right);
        }
    }
    fn estimate(&self) -> error_stack::Result<i64, WindowProcessorError> {
        let m: f64 = self.registers.len().approx_into();
        let zeros = self.registers.iter().filter(|rank| **rank == 0).count();
        let inverse_sum: f64 = self
            .registers
            .iter()
            .map(|rank| 2_f64.powi(-i32::from(*rank)))
            .sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / inverse_sum;
        let corrected = if raw <= 2.5 * m && zeros > 0 {
            let zero_count: f64 = zeros.approx_into();
            m * (m / zero_count).ln()
        } else {
            raw
        };
        corrected
            .round()
            .checked_approx_into()
            .ok_or_else(|| Report::new(WindowProcessorError::SketchEstimateOverflow))
    }
}

#[derive(Debug, Clone)]
struct BoundedTDigest {
    capacity: usize,
    centroids: SortedVec<Centroid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Centroid {
    mean: OrderedFloat<f64>,
    weight: u64,
}

impl BoundedTDigest {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            centroids: SortedVec::with_capacity(
                capacity
                    .checked_add(1)
                    .assured("validated centroid capacity is at most 4096"),
            ),
        }
    }
    fn admit(&mut self, value: f64) {
        self.centroids.insert(Centroid {
            mean: OrderedFloat(value),
            weight: 1,
        });
        self.compress();
    }
    fn merge(&mut self, other: &Self) {
        for centroid in other.centroids.iter() {
            self.centroids.insert(*centroid);
            self.compress();
        }
    }
    fn compress(&mut self) {
        while self.centroids.len() > self.capacity {
            let total = self.centroids.iter().fold(0_u64, |count, centroid| {
                count
                    .checked_add(centroid.weight)
                    .assured("a branch cannot admit 2^64 rows")
            });
            let total: f64 = total.approx_into();
            let capacity: f64 = self.capacity.approx_into();
            let mut before = 0_u64;
            let mut best: Option<(usize, f64)> = None;
            for (index, pair) in self.centroids.windows(2).enumerate() {
                let combined = pair[0]
                    .weight
                    .checked_add(pair[1].weight)
                    .assured("a branch cannot admit 2^64 rows");
                let midpoint = before.approx_into::<f64>() + combined.approx_into::<f64>() / 2.0;
                let quantile = midpoint / total;
                // The t-digest scale permits larger centroids near the median and preserves
                // smaller ones at the tails. The capacity cap is enforced even for an adversarial
                // distribution by merging the least costly adjacent pair.
                let allowance = (4.0 * total * quantile * (1.0 - quantile) / capacity).max(1.0);
                let gap = (pair[1].mean.0 * 0.5 - pair[0].mean.0 * 0.5).abs();
                let pressure = (combined.approx_into::<f64>() / allowance).max(1.0);
                let cost = gap * pressure / allowance;
                if best
                    .as_ref()
                    .is_none_or(|(_, score)| cost.total_cmp(score) == Ordering::Less)
                {
                    best = Some((index, cost));
                }
                before = before
                    .checked_add(pair[0].weight)
                    .assured("a branch cannot admit 2^64 rows");
            }
            let pair = best
                .map(|(index, _)| index)
                .assured("more than one centroid exists above the minimum capacity of 32");
            let right = self.centroids.remove_index(pair + 1);
            let left = self.centroids.remove_index(pair);
            let weight = left
                .weight
                .checked_add(right.weight)
                .assured("a branch cannot admit 2^64 rows");
            let total_weight: f64 = weight.approx_into();
            let left_fraction: f64 = left.weight.approx_into::<f64>() / total_weight;
            let right_fraction: f64 = right.weight.approx_into::<f64>() / total_weight;
            self.centroids.insert(Centroid {
                mean: OrderedFloat(left.mean.0 * left_fraction + right.mean.0 * right_fraction),
                weight,
            });
        }
    }
    fn quantile(&self, percentage: f64) -> Option<f64> {
        let total = self.centroids.iter().fold(0_u64, |count, centroid| {
            count
                .checked_add(centroid.weight)
                .assured("a branch cannot admit 2^64 rows")
        });
        if total == 0 {
            return None;
        }
        let maximum_rank = total.checked_sub(1).verified("the centroids are nonempty");
        let rank = (percentage / 100.0) * maximum_rank.approx_into::<f64>();
        let lower_rank = rank
            .floor()
            .checked_approx_into::<u64>()
            .verified("a retained window's percentile rank fits U64");
        let upper_rank = rank
            .ceil()
            .checked_approx_into::<u64>()
            .verified("a retained window's percentile rank fits U64");
        let lower = self
            .value_at_rank(lower_rank)
            .verified("the lower percentile rank is inside the retained centroids");
        if lower_rank == upper_rank {
            return Some(lower);
        }
        let upper = self
            .value_at_rank(upper_rank)
            .verified("the upper percentile rank is inside the retained centroids");
        let fraction = rank.fract();
        Some(lower * (1.0 - fraction) + upper * fraction)
    }

    fn value_at_rank(&self, rank: u64) -> Option<f64> {
        let mut before = 0_u64;
        // The validated capacity bounds this ordered rank scan to at most 4096 centroids.
        for centroid in self.centroids.iter() {
            let after = before
                .checked_add(centroid.weight)
                .assured("a branch cannot admit 2^64 rows");
            if rank < after {
                return Some(centroid.mean.0);
            }
            before = after;
        }
        None
    }
}

#[derive(Debug, Clone)]
struct FrequencyCandidates {
    capacity: usize,
    counts: BTreeMap<Vec<u8>, u64>,
}

impl FrequencyCandidates {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            counts: BTreeMap::new(),
        }
    }
    fn admit(&mut self, key: Vec<u8>) {
        self.add(key, 1);
    }
    fn add(&mut self, key: Vec<u8>, weight: u64) {
        if let Some(count) = self.counts.get_mut(&key) {
            *count = count
                .checked_add(weight)
                .assured("a branch cannot admit 2^64 rows");
            return;
        }
        if self.counts.len() < self.capacity {
            self.counts.insert(key, weight);
            return;
        }
        let minimum = self
            .counts
            .values()
            .copied()
            .min()
            .assured("a full frequency sketch has candidates");
        let decrement = minimum.min(weight);
        self.counts.retain(|_, count| {
            *count = count
                .checked_sub(decrement)
                .verified("the minimum count bounds every candidate decrement");
            *count > 0
        });
        if weight > decrement {
            self.add(key, weight - decrement);
        }
    }
    fn merge(&mut self, other: &Self) {
        for (key, count) in &other.counts {
            self.add(key.clone(), *count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_merges_registers_and_tracks_distinct_count_with_bounded_error() {
        let mut whole = HyperLogLog::new(12);
        let mut left = HyperLogLog::new(12);
        let mut right = HyperLogLog::new(12);
        for value in 0_u64..10_000 {
            let key = value.to_le_bytes();
            whole.admit(&key);
            if value % 2 == 0 {
                left.admit(&key);
            } else {
                right.admit(&key);
            }
        }
        left.merge(&right);
        assert_eq!(left.registers, whole.registers);
        let estimate = whole
            .estimate()
            .verified("ten thousand distinct keys fit I64");
        assert!((estimate - 10_000).abs() < 500);
        assert_eq!(HyperLogLog::new(12).estimate().verified("zero fits I64"), 0);
    }

    #[test]
    fn bounded_centroids_merge_and_approximate_the_median() {
        let mut left = BoundedTDigest::new(256);
        let mut right = BoundedTDigest::new(256);
        for value in 0_u64..10_000 {
            if value % 2 == 0 {
                left.admit(value.approx_into());
            } else {
                right.admit(value.approx_into());
            }
        }
        left.merge(&right);
        assert!(left.centroids.len() <= 256);
        let median = left.quantile(50.0).verified("the test admitted values");
        assert!((median - 4_999.5).abs() < 200.0, "median {median}");
        assert_eq!(BoundedTDigest::new(256).quantile(50.0), None);
    }

    #[test]
    fn t_digest_preserves_duplicate_heavy_median_and_adversarial_upper_tail() {
        let mut digest = BoundedTDigest::new(128);
        for _ in 0..9_000 {
            digest.admit(0.0);
        }
        for offset in 0..1_000_u64 {
            digest.admit(1_000_000_000.0 + offset.approx_into::<f64>());
        }
        assert!(digest.centroids.len() <= 128);
        assert_eq!(digest.quantile(50.0), Some(0.0));
        let upper = digest
            .quantile(95.0)
            .verified("the test admitted ten thousand values");
        assert!(
            (upper - 1_000_000_499.5).abs() < 150.0,
            "upper tail {upper}"
        );
    }

    #[test]
    fn bounded_frequency_candidates_keep_heavy_hitters_after_merge() {
        let mut left = FrequencyCandidates::new(16);
        let mut right = FrequencyCandidates::new(16);
        for _ in 0..100 {
            left.admit(b"hot".to_vec());
        }
        for _ in 0..80 {
            right.admit(b"warm".to_vec());
        }
        for value in 0_u16..200 {
            let key = value.to_le_bytes().to_vec();
            if value % 2 == 0 {
                left.admit(key);
            } else {
                right.admit(key);
            }
        }
        left.merge(&right);
        assert!(left.counts.len() <= 16);
        let hot = left
            .counts
            .get(b"hot".as_slice())
            .verified("a 100/380 item exceeds the 1/17 heavy-hitter threshold");
        let warm = left
            .counts
            .get(b"warm".as_slice())
            .verified("an 80/380 item exceeds the 1/17 heavy-hitter threshold");
        let hot_error = 100_u64
            .checked_sub(*hot)
            .verified("Misra-Gries counts do not overestimate");
        let warm_error = 80_u64
            .checked_sub(*warm)
            .verified("Misra-Gries counts do not overestimate");
        assert!(hot_error <= 23);
        assert!(warm_error <= 23);
    }
}
