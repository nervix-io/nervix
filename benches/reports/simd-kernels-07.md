# SIMD kernels 07: checked integer arithmetic in explicit SIMD lanes

## Reproduce

The baseline is `b607538691ebda2666062ed02c06deec4cf179d2` (`origin/main` when this branch
started) with this change's Criterion programs added, so both revisions run the same cases; the
candidate is the revision containing this report. Measurements were made on 2026-09-30 UTC on an
AMD Ryzen AI 9 HX 370 (Zen 5 and Zen 5c cores, AVX-512) with 24 logical CPUs, x86-64 Linux,
rustc 1.98.1, LLVM 22.1.8, `fearless_simd` 1.0.0, Arrow 58.4.0, and Criterion 0.5.1. Release
builds use the repository's configured kache wrapper. The VM timing binaries use
`target-cpu=native`; the inspected binaries and the kernel timing binary use the Docker image's
`x86-64-v3` payload target.

```bash
just build-vm-bench-x86-64-v3
just build-simd-kernels-x86-64-v3
just bench-vm --list
taskset -c 3 <vm bench binary> --bench '<filter>' --warm-up-time 1 --measurement-time 3 --noplot
taskset -c 3 just bench-checked-lanes-x86-64-v3 --warm-up-time 1 --measurement-time 2
```

Both revisions' VM Criterion binaries were built before any timing and run directly, pinned to
logical CPU 3, one of the host's Zen 5 cores, in the order baseline, candidate, candidate,
baseline, baseline, candidate, candidate, baseline. The kernel benchmark ran three passes on the
same core. Each reported time is the minimum of its rounds' Criterion medians, because
interference only adds time. Other sessions' builds and scenario suites shared the host: the load
average ranged from 10 to 27 during the VM rounds and from 6 to 11 during the kernel passes.

## The kernels

`nervix-simd-kernels` computes checked `+` and `-` over every integer width and checked `*` over
the 8-, 16- and 32-bit widths, and returns each lane's value together with the run's failure
words, so the VM stores no failure byte and makes no packing call for them. A product widens into
lanes twice as wide, where it is exact, is compared against the narrow type's bounds, and narrows
back by truncation. A sum or a difference needs no wider lane: it wraps in the operands' own
lanes, and the signs of the operands and the wrapped result, or the carry out of unsigned lanes,
decide exactly which lanes overflowed. The first implementation widened sums and differences as
well; the kernel benchmark, run the same way on native builds of both forms, measured the
native-width rule faster at every width (widened `i8` sums took 161 ns per 1,024 lanes and the
native-width rule 93 ns; `i32` 247 ns against 171 ns), so sums and differences use it, and the
same rule covers the 64-bit widths. A 64-bit product has no wider lane to be exact in and stays on the VM's lane loop, with
division and remainder.

## Generated instructions

### Baseline

`objdump -d -C` of the baseline's `x86-64-v3` Criterion binary, classifying the innermost loops of
each `Arithmetic::evaluate_integers` instance:

| Operands | Checked `+` and `-` | Checked `*` |
|:--|:--|:--|
| `I8`, `I16`, `I32`, `I64` | scalar `add` or `sub` with `seto` per lane, in every operand shape | scalar `imul` with `seto` per lane |
| `U8`, `U16`, `U32`, `U64` | vector loops LLVM formed, such as `vpaddd`, `vpmaxud` and `vpcmpeqd` for the carry, and packs down to failure bytes, 14 instructions per 8 `U32` lanes; scalar `add` or `sub` with `setb` for the remaining lanes | scalar `mul` with `seto` per lane |

The ticket expected the `I64` sums and differences to be vectorized already. In this build every
one of their loops, over two columns or a column and a constant, is a scalar `add` or `sub`
followed by `seto`, unrolled four times. Only the unsigned sums and differences were vectorized.

### Candidate

The candidate's `evaluate_integers` instances contain no checked `+`, `-` or `*` loop of their
own; they call the kernels. The only flag-per-lane loops left for these operators are the 64-bit
products in `<i64 as CheckedInteger>::products` and `<u64 as CheckedInteger>::products`, 15 `imul`
or `mul` with `seto` sites each, as designed.

Each kernel's word loop, which computes 64 lanes, from the same `x86-64-v3` binary. The AVX2 arm is
inlined into the kernel function and runs on a CPU without AVX-512; the AVX-512 arm is its own
`vectorize_avx512` function and runs on this host. Neither contains a scalar overflow flag.

| Kernel | AVX2 instructions | AVX2 core | AVX-512 instructions | AVX-512 core |
|:--|--:|:--|--:|:--|
| `i8` sums, differences | 24 | `vpaddb`/`vpsubb`, `vpxor`, `vpand`, `vpmovmskb` | 13 | `vpaddb`/`vpsubb`, `vpternlogq`, `kmovq` |
| `u8` sums, differences | 21, 23 | `vpaddb`/`vpsubb`, `vpminub`, `vpcmpeqb`, `vpmovmskb` | 8, 9 | `vpcmpltub`, `kmovq` |
| `i16` sums, differences | 55, 54 | `vpaddw`/`vpsubw`, `vpxor`, `vpand`, `vpacksswb`, `vpmovmskb` | 21 | `vpternlogq`, `kunpckdq` |
| `u16` sums, differences | 48, 52 | `vpminuw`/`vpmaxuw`, `vpcmpeqw`, `vpacksswb`, `vpmovmskb` | 15, 17 | `vpcmpltuw` |
| `i32` sums, differences | 81 | `vpaddd`/`vpsubd`, `vpxor`, `vpand`, `vmovmskps` | 41 | `vpternlogd`, `vpmovd2m` |
| `u32` sums, differences | 70, 78 | `vpminud`/`vpmaxud`, `vpcmpeqd`, `vmovmskps` | 29, 33 | `vpcmpltud` |
| `i64` sums, differences | 150 | `vpaddq`/`vpsubq`, `vpxor`, `vpand`, `vmovmskpd` | 80 | `vpternlogq`, `vpmovq2m` |
| `u64` sums, differences | 118, 134 | `vpcmpgtq` on sign-flipped lanes, `vmovmskpd` | 56, 64 | `vpcmpltuq` |
| `i8` products | 55 | `vpmovsxbw`, `vpmullw`, range test, `vpacksswb`, `vpmovmskb` | 20 | `vpmullw`, `vpcmpltuw`, `vpmovwb` |
| `u8` products | 50 | `vpmovzxbw`, `vpmullw`, `vpmaxuw`, `vpmovmskb` | 18 | `vpmullw`, `vpcmpnleuw`, `vpmovwb` |
| `i16` products | 89 | `vpmovsxwd`, `vpmulld`, range test, `vpackssdw` | 35 | `vpmulld`, `vpcmpltud`, `vpmovdw` |
| `u16` products | 81 | `vpmovzxwd`, `vpmulld`, `vpmaxud`, `vpackssdw` | 31 | `vpmulld`, `vpcmpnleud`, `vpmovdw` |
| `i32` products | 23 per 8 lanes | `vpmovzxdq`, `vpmuldq`, `vpcmpgtq`, `vmovmskps` | 80 | `vpmuldq`, `vpcmpltuq`, `vpmovqd` |
| `u32` products | 136 | `vpmovzxdq`, `vpmuludq`, `vpcmpgtq`, `vmovmskps` | 72 | `vpmuludq`, `vpcmpnleuq`, `vpmovqd` |

`fearless_simd` 1.0 implements a 64-bit lane multiplication on AVX2 as four scalar
multiplications, and its 64-bit AVX2 comparisons as scalar comparisons. LLVM folds both back into
vector code here: the product of two sign-extended 32-bit lanes becomes one `vpmuldq`, of two
zero-extended lanes one `vpmuludq`, and the 64-bit comparisons `vpcmpgtq`. A 32-bit product
therefore stays in vector lanes on AVX2 as well as on AVX-512.

## Criterion observation

### `checked_lanes`, one 1,024-lane run

The kernel benchmark times each kernel beside the lane loop the VM ran before it, in the same
process on the same operands: one `overflowing_*` operation per lane storing its value and a
failure byte, and one packing call. The compiler vectorizes that loop where it can, as it did in
the VM, which it does for the unsigned sums. Both come from the `x86-64-v3` build, so the lane loop
compiles for AVX2 as the payload's did, while the kernels select AVX-512 on this host.

| Kernel | SIMD | Lane loop | Speedup |
|:--|--:|--:|--:|
| `i8` sums | 65 ns | 390 ns | 6.0× |
| `u8` sums | 66 ns | 70 ns | 1.06× |
| `i16` sums | 112 ns | 411 ns | 3.7× |
| `u16` sums | 115 ns | 123 ns | 1.07× |
| `i32` sums | 167 ns | 449 ns | 2.7× |
| `u32` sums | 141 ns | 182 ns | 1.3× |
| `i64` sums | 213 ns | 471 ns | 2.2× |
| `u64` sums | 201 ns | 363 ns | 1.8× |
| `i8` products | 105 ns | 399 ns | 3.8× |
| `u8` products | 100 ns | 415 ns | 4.1× |
| `i16` products | 181 ns | 446 ns | 2.5× |
| `u16` products | 166 ns | 627 ns | 3.8× |
| `i32` products | 300 ns | 416 ns | 1.4× |
| `u32` products | 254 ns | 409 ns | 1.6× |

Every pass ranked the kernel ahead of the lane loop for every signed sum and every product. The
unsigned 8- and 16-bit sums, which LLVM had already vectorized, are at parity: the kernel saves
the failure bytes and the packing call, and the vector work is the same. The benchmark does not
time differences separately; the table above shows their word loops at the size of the sums' or
up to 16 instructions larger.

### `numeric_kernels`, 1,024 rows

| Benchmark | Baseline | Candidate | Change |
|:--|--:|--:|--:|
| `i16_add_sub_mul/no_failures` | 3.01 µs | 2.32 µs | −23% |
| `i32_add_sub_mul/no_failures` | 3.03 µs | 2.51 µs | −17% |
| `i64_add_sub_mul/no_failures` | 3.18 µs | 3.26 µs | +2% |
| `i16_arithmetic/no_failures` | 8.08 µs | 7.75 µs | −4% |
| `i32_arithmetic/no_failures` | 6.66 µs | 7.47 µs | +12% |
| `i64_arithmetic/no_failures` | 8.07 µs | 7.96 µs | −1% |
| `f64_arithmetic/no_failures` (unchanged) | 9.96 µs | 10.78 µs | +8% |

The `add_sub_mul` programs compute only the operators this change moves to the kernels. Over the
eight rounds, 14 of the 16 pairs of an `i16` candidate round and a baseline round, and 12 of the 16
`i32` pairs, favored the candidate. The rest of the table is within the rounds' spread: the
unchanged `f64` program moved +8%, one benchmark's rounds differed by up to 2.3 times under this
load, and the five-operator programs spend most of their time in the scalar `idiv` of their
quotient and remainder, which this change does not touch. The failing-row variants, which spend
their time building row errors, moved between −8% and +38% from one set of rounds to the next,
with no consistent direction, so this report makes no claim about them. The kernel benchmark
above is the speedup evidence.

## Semantic and coverage checks

The kernel crate's tests compare every level the host supports, and the forced scalar fallback,
against `overflowing_add`, `overflowing_sub` and `overflowing_mul`, value and failure bit for
every lane and every failure word bit past the last lane: every `i8` and `u8` operand pair as two
runs and with each value shared on either side, the bounds and random operands of the 16-, 32-
and 64-bit widths, every run length through 193 lanes, and runs of unequal lengths. They ran from
the native build, whose x86 levels all dispatch to AVX-512 on this host, and from the `x86-64-v3`
build, which also exercises the AVX2 arm. The Bolero property `simd-checked-lanes` repeats the
comparison for generated cases of every width and operand shape; its bounded run and corpus
replay passed through `just test-bolero`, and 20 seconds of sanitizer-backed libFuzzer through
`just fuzz` found no failure. Six deliberate defects in the kernels — the sign rule, the borrow
comparison, both product bounds, the tail mask, and the register shift — each failed between two
and six of the eight tests.

The 571 VM unit tests include every narrow width's `+`, `-` and `*` through
`Arithmetic::evaluate_integers` against the same reference, with null lanes whose buffers hold
overflowing values, sliced columns, scalar operands on either side, and a null scalar. The
Cucumber outline “Narrow integer arithmetic fails only the messages whose exact results leave
their width” passed on one and three nodes, together with the other checked numeric and numeric
function outlines (20 scenarios). `just coverage-lib` for the kernel and VM packages covered
**715 of 715 changed executable Rust lines**.

```bash
just test-package-lib nervix-simd-kernels
just test-vm
just test-bolero simd-checked-lanes
just fuzz simd-checked-lanes 20
just test-scenarios --input 'tests/features/runtime/\{checked_numeric_execution,numeric_classification_math_bits\}.feature'
just coverage-lib target/simd07.lcov --package nervix-vm --package nervix-simd-kernels
```
