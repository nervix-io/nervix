# SIMD kernels 08: division by per-call constants

## Reproduce

The production VM baseline is `54dbfec52d72c8995955e11f7c1206e9749ac318`, with the candidate's
three numeric constant-division benchmark programs added to the harness. The candidate is the
revision containing this report. Both run the same 1,024-row programs. Measurements were made
on 2026-09-30 UTC on an Intel Core i9-14900HX, 32 logical CPUs, x86-64 Linux, rustc 1.99.0,
LLVM 22.1.8, Arrow 58.4.0, `fearless_simd` 1.0.0 and Criterion 0.5.1. The host supports AVX2.
Builds retain the repository's configured kache wrapper. Native builds use `target-cpu=native`;
the additional production-target measurements and inspection use `x86-64-v3`.

```bash
# Baseline VM source plus the same harness cases, then the candidate:
taskset -c 3 just bench-vm no_failures --warm-up-time 1 --measurement-time 2 --sample-size 30 --save-baseline division08 --noplot
taskset -c 3 just bench-vm "'(numeric_kernels/.*constant_division|datetime_kernels/.*)/no_failures'" --warm-up-time 1 --measurement-time 2 --sample-size 30 --baseline division08 --noplot

# All selected paths and scalar references in one process:
taskset -c 3 just bench-constant-division --warm-up-time 0.5 --measurement-time 1 --sample-size 20 --noplot
taskset -c 3 just bench-constant-division "'(i64|u64)_'" --warm-up-time 1 --measurement-time 2 --sample-size 40 --noplot
taskset -c 3 just bench-constant-division-x86-64-v3 --warm-up-time 0.5 --measurement-time 1 --sample-size 20 --noplot
taskset -c 3 just bench-constant-division-x86-64-v3 --test
```

The tables use Criterion's central reported time estimates. The six smaller widths and the
Euclidean cases use 20 samples; the selected 64-bit cases use a later 40-sample run. Divisor
preparation, output allocation and failure words are included. The reference performs checked
integer operations directly in an inlined lane closure, stores failure bytes and packs them,
without a function pointer per lane. Divisors are black-boxed at call entry, preventing compile-time
strength reduction of the checked reference. Other builds and scenarios share this host.
Timing variation is visible in the controls below; these observations establish kernel and VM
costs, and contain no end-to-end throughput measurement.

## Arithmetic and retained paths

`UnsignedDivisor` prepares `m = floor(2^64 / d)` once for a non-power-of-two nonzero divisor.
For `n < 2^64`, `high(n*m)` is either the exact quotient or one below it. Subtracting
`q*d` and comparing that remainder against `d` gives the single correction. Narrower lanes
derive `floor(2^w / d)` by discarding the reciprocal's lower `64-w` bits. Powers of two use
a shift and mask. `SignedDivisor` divides unsigned magnitudes and restores the sign, so
`i64::MIN` remains representable. Its checked truncating and Euclidean operations match Rust,
including the overflow of `MIN / -1`.

The numeric caller prepares a divisor only for a shared right operand. Division by a column
and scalar-left/column-right execution keep their checked lane loops. A zero divisor fails
valid lanes; `MIN / -1` fails its lane; `MIN % -1` is the successful zero. Null lanes mask
failures, and a null scalar short-circuits to an all-null result.

SIMD is retained for quotient and remainder below 64 bits, and for `I64` quotient. The
`U64` operations and `I64` remainder use scalar reciprocal loops. The measured vector
experiments informed that selection:

| Production-target experiment, 1,024 lanes | Vector | Scalar reciprocal |
| --- | ---: | ---: |
| `i64` quotient | 1302 ns | 1745 ns |
| `u64` quotient | 856 ns | 788 ns |
| `u64` remainder | 877 ns | 797 ns |
| `i64` remainder, repeated with 40 samples | 1685 ns | 1534 ns |

Fixed-unit truncation and binning normalize values and origins through the prepared signed
remainder. Their phase difference lies in `(-stride, stride)`, so adding one stride when it
is negative replaces another modulo. Subtracting the resulting distance retains the checked
bin-start boundary, including timestamps before the epoch and fixed-offset week alignment.
`to_unix` uses Euclidean quotient. Fixed-unit `date_diff` uses truncating division when its
`i64` timestamp subtraction fits; otherwise its exact `i128` subtraction and division check
the final `i64` result. Calendar and zoned calculations retain their calendar rules.
`date_add` and `from_unix` retain checked `i128` multiplication and addition;
`from_unix` has no division to reduce.

## Generated instructions

`objdump -d -C --no-show-raw-insn` of the kernel benchmark and VM benchmark identifies the
selected reciprocal loops. No selected constant-division body contains `div` or `idiv`.
Preparation may call the compiler's wide division helper once at entry.

| Selected operation | Generated instruction families on AVX2 |
| --- | --- |
| 8-bit quotient and remainder | Widened `vpmullw`, `vpsrlw`, byte comparisons and `vpsubb` |
| 16-bit quotient and remainder | Widened `vpmulld`, `vpsrld`, word comparisons and `vpsubw` |
| 32-bit quotient and remainder | Widened `vpmuludq`, narrowing and dword comparison/correction |
| `i64` quotient | Four 32-by-32 partial products lowered to `vpmuludq`, `vpsrlq`, `vpaddq`, `vpsubq`, sign and overflow masks |
| `i64` remainder and `u64` operations | Scalar `mul`/`mulx` for the high product and correction; power-of-two loops may auto-vectorize shifts |

The VM's datetime `Lanes::binary` bodies for binning and elapsed-unit counting, and its
`to_unix` iterator loop, use `mul`/`mulx` and shifts for the prepared divisor. The
wide elapsed-difference fallback preserves wide division. The runtime chooses the process's
SIMD level once; the checks run every host-supported level and the forced scalar fallback.
AVX-512 and aarch64 timings were not measured on this host.

## Same-process kernel observations

One native 1,024-lane run, quotient or remainder by seven:

| Integer and operation | Selected kernel | Scalar reciprocal | Checked lane loop | Checked/selected |
| --- | ---: | ---: | ---: | ---: |
| `i8` quotient | 265 ns | 1463 ns | 1595 ns | 6.01× |
| `i8` remainder | 149 ns | 1325 ns | 1845 ns | 12.39× |
| `u8` quotient | 102 ns | 849 ns | 1595 ns | 15.58× |
| `u8` remainder | 112 ns | 934 ns | 1753 ns | 15.69× |
| `i16` quotient | 419 ns | 1529 ns | 5202 ns | 12.40× |
| `i16` remainder | 284 ns | 1052 ns | 4370 ns | 15.39× |
| `u16` quotient | 195 ns | 762 ns | 1673 ns | 8.56× |
| `u16` remainder | 218 ns | 869 ns | 1793 ns | 8.21× |
| `i32` quotient | 482 ns | 1419 ns | 1822 ns | 3.78× |
| `i32` remainder | 478 ns | 1162 ns | 1758 ns | 3.68× |
| `u32` quotient | 267 ns | 913 ns | 3121 ns | 11.69× |
| `u32` remainder | 585 ns | 1527 ns | 1909 ns | 3.26× |
| `i64` quotient | 1324 ns | 1904 ns | 2584 ns | 1.95× |
| `i64` remainder | 1571 ns | 1594 ns | 2587 ns | 1.65× |
| `u64` quotient | 1191 ns | 1081 ns | 2207 ns | 1.85× |
| `u64` remainder | 1141 ns | 1051 ns | 1961 ns | 1.72× |

Euclidean division of mixed-sign full-width values by fixed-unit strides, also 1,024 lanes:

| Unit and operation | Scalar reciprocal | Checked lane loop | Checked/reciprocal |
| --- | ---: | ---: | ---: |
| second, quotient | 1.737 µs | 2.978 µs | 1.72× |
| second, remainder | 1.424 µs | 2.910 µs | 2.04× |
| minute, quotient | 1.944 µs | 2.984 µs | 1.54× |
| minute, remainder | 1.477 µs | 3.055 µs | 2.07× |
| day, quotient | 1.876 µs | 2.998 µs | 1.60× |
| day, remainder | 1.412 µs | 2.993 µs | 2.12× |

## VM observations

No-failure 1,024-row programs, including unchanged date-part controls. The constant-division
programs each compute a quotient and a remainder. The `unix_conversion` program includes
checked `from_unix` multiplication as well as `to_unix`.

| Program | Baseline | Candidate | Time change |
| --- | ---: | ---: | ---: |
| `numeric_kernels/i16_constant_division/no_failures` | 10.410 µs | 3.314 µs | -68.2% |
| `numeric_kernels/i32_constant_division/no_failures` | 5.080 µs | 3.440 µs | -32.3% |
| `numeric_kernels/i64_constant_division/no_failures` | 5.690 µs | 4.182 µs | -26.5% |
| `datetime_kernels/date_part_time_of_day/no_failures` | 18.092 µs | 19.511 µs | 7.8% |
| `datetime_kernels/date_part_calendar/no_failures` | 38.338 µs | 36.906 µs | -3.7% |
| `datetime_kernels/truncate_and_bin/no_failures` | 20.005 µs | 14.348 µs | -28.3% |
| `datetime_kernels/add_and_diff/no_failures` | 9.546 µs | 7.329 µs | -23.2% |
| `datetime_kernels/unix_conversion/no_failures` | 6.756 µs | 6.747 µs | -0.1% |

The measured truncation/binning program is about 28% faster, elapsed arithmetic about 23%
faster, and the numeric constant programs about 26–68% faster. The combined Unix conversion
program's time is unchanged within its confidence interval. The shared-host controls vary,
so these timings do not establish an application throughput gain.

## Qualification

- All 39 kernel library tests and 577 VM library tests pass. Exhaustive byte division and
  remainder, full-width boundaries and randomized cases cover zero, all sign combinations,
  `MIN / -1`, `MIN % -1`, tails, null masks, sliced buffers and shared operands.
- The registered `simd-constant-division` Bolero property compares checked arithmetic and
  complete failure words at every supported level; fixed-unit and generated bin strides
  separately exercise the Euclidean scalar primitive. The ordinary randomized/corpus runner
  and a 20-second AddressSanitizer libFuzzer campaign pass.
- The 32 focused scenarios (262 steps) pass across datetime, calendar datetime, checked numeric
  execution and subscription options. Updated outlines include
  “Integer arithmetic fails only the messages whose operands fail in a batch”,
  “Datetime functions compute from event timestamps and the domain execution time”, and
  “Datetime results outside their types fail only their own messages”.
- The owning contracts are in `docs/src/vm-functions.md`; the function qualification ledger
  records the public scenarios and differential checks. Native benchmark smoke coverage
  executes every new benchmark body.
