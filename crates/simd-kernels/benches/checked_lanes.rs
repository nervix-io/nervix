//! Criterion throughput of checked integer sums and products over one 1,024-lane run.
//!
//! Each case measures the SIMD kernel beside the lane loop the VM ran before it: one
//! `overflowing_*` operation per lane storing the lane's value and a failure byte, and one call
//! packing the run's bytes into words. The compiler vectorizes that loop where it can, as it did in
//! the VM, which it does for unsigned sums. Both run in the same process on the same operands, so a
//! round compares them under the same conditions.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::ResultExt as _;
use nervix_simd_kernels::{
    CheckedArithmetic, CheckedLane, CheckedLanes, FlagPacker, LaneOperands, WidenedLane,
};

/// The lanes of one run, the block the VM packs with one call.
const LANES: usize = 1_024;

/// The lane loop: `lane` computes each lane's value and whether it failed, the loop stores a
/// failure byte per lane, and one call packs the bytes into words.
fn lane_loop<N: Copy + Default>(
    left: &[N],
    right: &[N],
    lane: impl Fn(N, N) -> (N, bool),
) -> CheckedLanes<N> {
    let mut values = vec![N::default(); left.len()];
    let mut flags = [0_u8; LANES];
    let run_lanes = values
        .iter_mut()
        .zip(left.iter().zip(right))
        .zip(flags.iter_mut());
    for ((value, (left, right)), flag) in run_lanes {
        let (result, failed) = lane(*left, *right);
        *value = result;
        *flag = u8::from(failed);
    }
    let mut failed = Vec::with_capacity(LANES.div_ceil(64));
    FlagPacker::new().pack(&flags[..left.len()], &mut failed);
    CheckedLanes { values, failed }
}

/// Operands drawn from the whole range of a type, so some lanes fail in both paths. Neither path
/// branches on a failure, so the share of failed lanes does not change what they cost.
trait RandomLane: Copy + Default {
    fn random(random: &mut fastrand::Rng) -> Self;
}

macro_rules! random_lane {
    ($($native:ident),+ $(,)?) => {
        $(
            impl RandomLane for $native {
                fn random(random: &mut fastrand::Rng) -> Self {
                    random.$native(..)
                }
            }
        )+
    };
}

random_lane!(i8, u8, i16, u16, i32, u32, i64, u64);

/// The left and right operands of every lane of one run.
struct Operands<N> {
    left: Vec<N>,
    right: Vec<N>,
}

impl<N: RandomLane> Operands<N> {
    fn random(seed: u64) -> Self {
        let mut random = fastrand::Rng::with_seed(seed);
        let left = (0..LANES).map(|_| N::random(&mut random)).collect();
        let right = (0..LANES).map(|_| N::random(&mut random)).collect();
        Self { left, right }
    }
}

/// Measures one width's sums. `overflowing_add` is the function item itself, not a function
/// pointer, so the lane loop inlines it as the VM's loop did.
fn bench_sums<N: CheckedLane + RandomLane>(
    criterion: &mut Criterion,
    width: &str,
    overflowing_add: impl Fn(N, N) -> (N, bool) + Copy,
) {
    let Operands { left, right } = Operands::<N>::random(0x0007_5000);
    let kernel = CheckedArithmetic::new();
    let mut group = criterion.benchmark_group(format!("checked_lanes/{width}_sums"));
    group.throughput(Throughput::Elements(
        u64::try_from(LANES).assured("the run length fits u64"),
    ));
    group.bench_function(BenchmarkId::from_parameter("simd"), |benchmark| {
        benchmark.iter(|| {
            kernel.sums(LaneOperands::Runs {
                left: black_box(&left),
                right: black_box(&right),
            })
        })
    });
    group.bench_function(BenchmarkId::from_parameter("lane_loop"), |benchmark| {
        benchmark.iter(|| lane_loop(black_box(&left), black_box(&right), overflowing_add))
    });
    group.finish();
}

/// Measures one width's products, with `overflowing_mul` inlined into the lane loop as
/// [`bench_sums`] inlines its addition.
fn bench_products<N: WidenedLane + RandomLane>(
    criterion: &mut Criterion,
    width: &str,
    overflowing_mul: impl Fn(N, N) -> (N, bool) + Copy,
) {
    let Operands { left, right } = Operands::<N>::random(0x0007_4000);
    let kernel = CheckedArithmetic::new();
    let mut group = criterion.benchmark_group(format!("checked_lanes/{width}_products"));
    group.throughput(Throughput::Elements(
        u64::try_from(LANES).assured("the run length fits u64"),
    ));
    group.bench_function(BenchmarkId::from_parameter("simd"), |benchmark| {
        benchmark.iter(|| {
            kernel.products(LaneOperands::Runs {
                left: black_box(&left),
                right: black_box(&right),
            })
        })
    });
    group.bench_function(BenchmarkId::from_parameter("lane_loop"), |benchmark| {
        benchmark.iter(|| lane_loop(black_box(&left), black_box(&right), overflowing_mul))
    });
    group.finish();
}

fn checked_lanes(criterion: &mut Criterion) {
    bench_sums(criterion, "i8", i8::overflowing_add);
    bench_sums(criterion, "u8", u8::overflowing_add);
    bench_sums(criterion, "i16", i16::overflowing_add);
    bench_sums(criterion, "u16", u16::overflowing_add);
    bench_sums(criterion, "i32", i32::overflowing_add);
    bench_sums(criterion, "u32", u32::overflowing_add);
    bench_sums(criterion, "i64", i64::overflowing_add);
    bench_sums(criterion, "u64", u64::overflowing_add);
    bench_products(criterion, "i8", i8::overflowing_mul);
    bench_products(criterion, "u8", u8::overflowing_mul);
    bench_products(criterion, "i16", i16::overflowing_mul);
    bench_products(criterion, "u16", u16::overflowing_mul);
    bench_products(criterion, "i32", i32::overflowing_mul);
    bench_products(criterion, "u32", u32::overflowing_mul);
}

criterion_group!(benches, checked_lanes);
criterion_main!(benches);
