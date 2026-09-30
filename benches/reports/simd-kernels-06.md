# SIMD kernels 06: bitmap failure packing and selections

## Reproduce

The baseline is `d9c65a005ce8415131251dd35232d2084aadbc32` (`origin/main` after the branch
update); the candidate is the revision containing this report. The earlier baseline
`8803d2afd2a437104212fdea89f116db812a8312` compiles the inspected kernels to the same instruction
counts, since the update does not touch them. Measurements were made on
2026-09-29 UTC on an AMD Ryzen AI 9 HX 370 (Zen 5, AVX-512) with 24 logical CPUs, x86-64 Linux,
rustc 1.98.1, LLVM 22.1.8, Arrow 58.4.0, and Criterion 0.5.1. Release builds use the repository's
configured kache wrapper; the timed binaries use `target-cpu=native`, and the inspected binaries
also use the Docker image's `x86-64-v3` payload target.

```bash
taskset -c 3 just bench-vm numeric_kernels --save-baseline simd06-p1 --warm-up-time 1 --measurement-time 3
taskset -c 3 just bench-vm execute_program_batch_size --save-baseline simd06-p1 --warm-up-time 0.5 --measurement-time 1
just build-vm-bench-x86-64-v3
```

Both revisions' Criterion binaries were built before any timing. Every timed run was pinned to
logical CPU 3, one of the host's four Zen 5 cores: its eight Zen 5c cores top out at 3.29 GHz
against 5.16 GHz, and earlier unpinned rounds that migrated between the two core types gave
bimodal timings, so they were discarded. `numeric_kernels` ran three times per revision, in the
order baseline, candidate, candidate, baseline, baseline, candidate, and
`execute_program_batch_size` twice, in the order baseline, candidate, candidate, baseline. Each
benchmark's reported time is the minimum of its rounds' Criterion medians, because interference
only adds time. Other sessions' builds and scenario suites shared the host: the load average
ranged from 4 to 33 during the `numeric_kernels` rounds and from 2.6 to 4.2 during the sweep, so
the sweep is the steadier evidence, with a median round-to-round spread of 6.5% per benchmark.

## Generated instructions

`objdump -d -C` of both revisions' `x86-64-v3` Criterion binaries, counting the instructions that
put one lane's failure into a word, in every checked arithmetic and rounding kernel:

| Kernel | Baseline `shlx` | Baseline `vpsllvq` | Candidate `shlx` | Candidate `vpsllvq` |
|:--|--:|--:|--:|--:|
| `Arithmetic::evaluate_integers`, each signed width | 69 | 32 | 0 | 0 |
| `Arithmetic::evaluate_integers`, `U8` and `U16` | 67 | 44 | 0 | 0 |
| `Arithmetic::evaluate_integers`, `U32` and `U64` | 67 | 48 | 0 | 0 |
| `Arithmetic::evaluate_floats`, `F32` and `F64` | 61 | 60 | 0 | 0 |
| `Rounding::evaluate_floats`, `F32` and `F64` | 18 | 20 | 0 | 0 |

The baseline shifted each lane's failure flag into the word, as scalar `shlx` and `or` or as a
vector `vpsllvq` by the lane index followed by an `or` reduction. In the candidate a vectorized
lane loop stores its failure bytes from the compare results with `vpackssdw`, `vpacksswb`, and
one `vmovd` per four 64-bit lanes, and a scalar lane loop, such as checked `I64` addition or
multiplication, writes each byte with `seto` into memory. One `FlagPacker::pack` call then packs
a block of up to 1,024 lanes: its AVX2 arm is a loop of `vpcmpeqb` and `vpmovmskb` over 32-byte
registers, and its AVX-512 arm uses `vptestmb` and `kmovq` on 64-byte registers. The `vpextrb`
that remain in the candidate's integer kernels feed the scalar `idiv` of division and remainder:
lane predicates for the checked quotient, and the 8-bit dividend and divisor bytes. Division by a
column stays scalar under this epic's rule, so those extractions are the lane operation, not the
packing.

The `*_valid` variants in the baseline updated the failure word in memory for every valid lane
(`shlx` followed by `or %rax,(%rdx,%rcx,8)` behind a bounds check). The candidate's contain no
`or` to memory: a fully valid word runs the ordinary lane loop, a mixed word clears its failure
bytes and walks its valid lanes with `tzcnt` and `blsr`, and the words' bytes are packed by one
`pack` call per block, as in the other variants.

The checked `I64` addition and subtraction loops over two columns are scalar in both revisions
of this build: the baseline followed each `add` and `seto` with a `shlx` and an `or`, and the
candidate stores the `seto` byte and packs the block afterwards.

The native (AVX-512) binaries show the same shape: the baseline's checked kernels held 121 to 131
`shlx` and 46 to 65 `vpsllvq` each, and the candidate's hold none.

## Criterion observation

### `numeric_kernels`, 1,024 rows

| Benchmark | Baseline | Candidate | Change |
|:--|--:|--:|--:|
| `i64_arithmetic/no_failures` | 8.14 µs | 7.19 µs | −12% |
| `i32_arithmetic/no_failures` | 7.63 µs | 6.45 µs | −16% |
| `f64_arithmetic/no_failures` | 10.33 µs | 8.90 µs | −14% |
| `numeric_unary/no_failures` | 6.92 µs | 4.90 µs | −29% |
| `integer_shifts/no_failures` | 8.99 µs | 7.14 µs | −21% |
| `precision_rounding/no_failures` | 45.30 µs | 33.05 µs | −27% |
| `float_classification/no_failures` | 3.78 µs | 3.26 µs | −14% |
| `transcendental/no_nulls` | 66.47 µs | 62.37 µs | −6% |
| `transcendental/half_nulls` | 65.88 µs | 69.72 µs | +6% |
| `transcendental/most_nulls` | 26.60 µs | 18.26 µs | −31% |
| `i64_arithmetic/sparse_failures` | 20.52 µs | 11.92 µs | −42% |
| `f64_arithmetic/dense_failures` | 49.04 µs | 40.88 µs | −17% |
| `comparison/nan_and_nulls` | 3.45 µs | 3.57 µs | +4% |

The candidate is faster in 27 of the 32 benchmarks, with a geometric mean change of −17.8%. The
rounds of one benchmark still varied by up to a factor of two, because other processes could run
on the pinned core, so a single row is diagnostic rather than a speedup claim. The comparison
kernel, which this change does not touch, moved +4%.

### `execute_program_batch_size`

| Program | Rows | Baseline | Candidate | Change |
|:--|--:|--:|--:|--:|
| `arithmetic_filter` | 1 | 3.20 µs | 3.25 µs | +2% |
| `arithmetic_filter` | 1,024 | 9.26 µs | 8.11 µs | −12% |
| `arithmetic_filter` | 65,536 | 465.08 µs | 423.05 µs | −9% |
| `numeric_compare` | 1,024 | 7.68 µs | 7.00 µs | −9% |
| `numeric_compare` | 65,536 | 405.11 µs | 377.42 µs | −7% |
| `float_arithmetic` | 1,024 | 7.64 µs | 7.25 µs | −5% |
| `float_arithmetic` | 65,536 | 635.20 µs | 554.53 µs | −13% |
| `correlate_where` | 1,024 | 6.40 µs | 5.88 µs | −8% |
| `correlate_where` | 65,536 | 235.53 µs | 203.59 µs | −14% |
| `window_aggregate_input` | 65,536 | 25.96 µs | 18.74 µs | −28% |
| `string_builtins` | 1,024 | 27.39 µs | 27.25 µs | −1% |
| `string_builtins` | 65,536 | 1,452.70 µs | 1,483.94 µs | +2% |

Over the 16 programs, the geometric mean change is −0.5% at 1 row, +0.3% at 8, −2.2% at 64,
−2.6% at 256, −4.3% at 1,024, −2.9% at 4,096, −5.2% at 16,384, and −6.6% at 65,536 rows. Programs
with a `WHERE` or checked kernels gain as batches grow, while the string, regular-expression,
text, and list programs this change does not touch stay within about 3%. The fixed costs a small
batch pays, the zeroed flag block and one packing call per kernel, stay inside the round-to-round
spread at one and eight rows.

## Semantic and coverage checks

The Cucumber outline “FILTER WHERE keeps sparse, dense and no rows of interleaved branches, with
and without row errors” in `tests/features/runtime/filter_selection.feature` passed on one and
three nodes, before and after the branch update and under coverage instrumentation. An ingestor
`FILTER WHERE` selects dense, sparse and empty sets from batches that interleave two branches, one
batch fails a row in each branch, and a junction's branch-local `FILTER WHERE` selects again. A
strict subscription step requires every kept row and every routed failure exactly once, and a
three-second window afterwards requires that nothing else arrives.

The 26 kernel unit tests compare packing with per-lane shifts at every supported level for every
run length through 193 lanes. They ran from both the native and the `x86-64-v3` builds, because on
this AVX-512 host the native build dispatches every x86 level to the AVX-512 arm. The 568 VM unit
tests include packed failures against per-lane shifts for every lane count through 193 and on both
sides of the 1,024-lane block, sliced validity at three offsets, valid-lane-only computation, and
a `WHERE` over three bitmap words with null predicates and failed rows. Under coverage
instrumentation 1,500 of the server's 1,501 unit tests passed; the timing-based
`reingestor_propagates_attached_ack_into_branched_entrypoint` failed there once at a load average
near 60 and passed three reruns on the ordinary build. LCOV from `just coverage-lib` for the kernel, VM and server packages and from
`just coverage-scenarios` for the outline covered **533 of 540 changed executable Rust lines
(98.7%)** before the branch update.

```bash
just coverage-lib target/simd06-vm-kernels.lcov --package nervix-vm --package nervix-simd-kernels
ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" \
  just coverage-lib target/simd06-server.lcov --package nervix-server --features testing
just coverage-scenarios target/simd06-scenario.lcov --input tests/features/runtime/filter_selection.feature
```
