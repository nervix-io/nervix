//! Reciprocal SIMD, scalar reciprocal and checked lane-loop comparisons on identical runs.
//!
//! Layer: test harness.
//! - **Owns.** Same-process Criterion evidence for every integer width's constant division.
//! - **Depends on.** Production kernels and their checked integer reference.
//! - **Must not know.** Arrow, VM programs or graph execution.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_simd_kernels::{
    CheckedLanes, ConstantDivision, DivisionLane, FlagPacker, SignedDivisor, UnsignedDivisor,
};

const LANES: usize = 1_024;

trait Measured: DivisionLane {
    type Scalar: Copy;
    fn prepare_scalar(divisor: Self) -> Self::Scalar;
    fn scalar(value: Self, prepared: Self::Scalar, remainder: bool) -> Option<Self>;
    fn checked(value: Self, divisor: Self, remainder: bool) -> Option<Self>;
    fn sample(index: usize) -> Self;
    fn divisor() -> Self;
}

macro_rules! signed {
    ($($native:ty),+ $(,)?) => {$(
        impl Measured for $native {
            type Scalar = SignedDivisor;
            fn prepare_scalar(divisor: Self) -> Self::Scalar {
                SignedDivisor::new(i64::from(divisor)).assured("the measured divisor is seven")
            }
            fn scalar(value: Self, prepared: Self::Scalar, remainder: bool) -> Option<Self> {
                let value = if remainder { prepared.checked_rem(i64::from(value)).unwrap_or(0) } else { prepared.checked_div(i64::from(value))? };
                Self::try_from(value).ok()
            }
            fn checked(value: Self, divisor: Self, remainder: bool) -> Option<Self> {
                if remainder { value.checked_rem(divisor) } else { value.checked_div(divisor) }
            }
            fn sample(index: usize) -> Self {
                let magnitude = Self::try_from(index % 127).assured("every signed width holds 126");
                if index % 2 == 0 { magnitude } else { -magnitude }
            }
            fn divisor() -> Self { 7 }
        }
    )+};
}

macro_rules! unsigned {
    ($($native:ty),+ $(,)?) => {$(
        impl Measured for $native {
            type Scalar = UnsignedDivisor;
            fn prepare_scalar(divisor: Self) -> Self::Scalar {
                UnsignedDivisor::new(u64::from(divisor)).assured("the measured divisor is seven")
            }
            fn scalar(value: Self, prepared: Self::Scalar, remainder: bool) -> Option<Self> {
                let value = if remainder { prepared.remainder(u64::from(value)) } else { prepared.quotient(u64::from(value)) };
                Self::try_from(value).ok()
            }
            fn checked(value: Self, divisor: Self, remainder: bool) -> Option<Self> {
                if remainder { value.checked_rem(divisor) } else { value.checked_div(divisor) }
            }
            fn sample(index: usize) -> Self { Self::try_from(index % 251).assured("every unsigned width holds 250") }
            fn divisor() -> Self { 7 }
        }
    )+};
}

signed!(i8, i16, i32, i64);
unsigned!(u8, u16, u32, u64);

fn lane_loop<N: Copy + Default>(values: &[N], lane: impl Fn(N) -> Option<N>) -> CheckedLanes<N> {
    let mut output = vec![N::default(); values.len()];
    let mut flags = [0; LANES];
    for ((output, value), flag) in output.iter_mut().zip(values).zip(flags.iter_mut()) {
        match lane(*value) {
            Some(value) => *output = value,
            None => *flag = 1,
        }
    }
    let mut failed = Vec::with_capacity(values.len().div_ceil(64));
    FlagPacker::new().pack(&flags[..values.len()], &mut failed);
    CheckedLanes {
        values: output,
        failed,
    }
}

fn measure<N: Measured>(criterion: &mut Criterion, name: &str) {
    let values: Vec<N> = (0..LANES).map(N::sample).collect();
    let kernel = ConstantDivision::new();
    for remainder in [false, true] {
        let operation = if remainder { "remainder" } else { "quotient" };
        let mut group = criterion.benchmark_group(format!("constant_division/{name}_{operation}"));
        group.throughput(Throughput::Elements(1_024));
        group.bench_function("selected_kernel", |b| {
            b.iter(|| {
                let divisor = black_box(N::divisor());
                if remainder {
                    kernel.remainders(black_box(&values), divisor)
                } else {
                    kernel.quotients(black_box(&values), divisor)
                }
            })
        });
        group.bench_function("scalar_reciprocal", |b| {
            b.iter(|| {
                let prepared = N::prepare_scalar(black_box(N::divisor()));
                lane_loop(black_box(&values), |value| {
                    N::scalar(value, prepared, remainder)
                })
            })
        });
        group.bench_function("checked_lane_loop", |b| {
            b.iter(|| {
                let divisor = black_box(N::divisor());
                lane_loop(black_box(&values), |value| {
                    N::checked(value, divisor, remainder)
                })
            })
        });
        group.finish();
    }
}

fn division(criterion: &mut Criterion) {
    measure::<i8>(criterion, "i8");
    measure::<u8>(criterion, "u8");
    measure::<i16>(criterion, "i16");
    measure::<u16>(criterion, "u16");
    measure::<i32>(criterion, "i32");
    measure::<u32>(criterion, "u32");
    measure::<i64>(criterion, "i64");
    measure::<u64>(criterion, "u64");
    euclidean(criterion);
}

fn euclidean(criterion: &mut Criterion) {
    let values: Vec<i64> = (0..LANES)
        .map(|index| {
            (i64::try_from(index).assured("the lane index is below 1024") - 512) * 9_000_000_000_001
        })
        .collect();
    for (name, divisor) in [
        ("second", 1_000_000_000_i64),
        ("minute", 60_000_000_000),
        ("day", 86_400_000_000_000),
    ] {
        for remainder in [false, true] {
            let operation = if remainder { "remainder" } else { "quotient" };
            let mut group = criterion
                .benchmark_group(format!("constant_division/euclidean_{name}_{operation}"));
            group.throughput(Throughput::Elements(1_024));
            group.bench_function("scalar_reciprocal", |b| {
                b.iter(|| {
                    let prepared = SignedDivisor::new(black_box(divisor))
                        .assured("each measured fixed divisor is positive");
                    lane_loop(black_box(&values), |value| {
                        if remainder {
                            prepared.checked_rem_euclid(value)
                        } else {
                            prepared.checked_div_euclid(value)
                        }
                    })
                })
            });
            group.bench_function("checked_lane_loop", |b| {
                b.iter(|| {
                    let divisor = black_box(divisor);
                    lane_loop(black_box(&values), |value| {
                        if remainder {
                            value.checked_rem_euclid(divisor)
                        } else {
                            value.checked_div_euclid(divisor)
                        }
                    })
                })
            });
            group.finish();
        }
    }
}

criterion_group!(benches, division);
criterion_main!(benches);
