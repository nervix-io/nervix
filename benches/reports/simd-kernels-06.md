# SIMD kernels 06: bitmap failure packing and selections

## Reproduce

The baseline is `8803d2afd2a437104212fdea89f116db812a8312` (`origin/main` when the measurements
started); the candidate is the revision containing this report. Measurements were made on
2026-09-29 UTC on an AMD Ryzen AI 9 HX 370 (Zen 5, AVX-512) with 24 logical CPUs, x86-64 Linux,
rustc 1.98.1, LLVM 22.1.8, Arrow 58.4.0, and Criterion 0.5.1. Release builds use the repository's
configured kache wrapper; the timed binaries use `target-cpu=native`, and the inspected binaries
also use the Docker image's `x86-64-v3` payload target.

```bash
just bench-vm numeric_kernels --save-baseline simd06-r1 --warm-up-time 1 --measurement-time 2
just bench-vm execute_program_batch_size --save-baseline simd06-r1 --warm-up-time 1 --measurement-time 2
just build-vm-bench-x86-64-v3
```

Both revisions' Criterion binaries were built before any timing. Each group then ran twice per
revision, in the order baseline, candidate, candidate, baseline, and each benchmark's reported
time is the median of its two Criterion medians. Other sessions' scenario suites kept the host's
load average between 30 and 46 throughout, so the timings are diagnostic: the interleaving lets
the load affect both revisions alike, but it does not make a small difference significant.

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
bytes and walks its valid lanes with `tzcnt` and `blsr`, and each word's bytes are packed by one
`pack` call.

The native (AVX-512) binaries show the same shape: the baseline's checked kernels held 121 to 131
`shlx` and 46 to 65 `vpsllvq` each, and the candidate's hold none.

## Criterion observation

Pending.

## Semantic and coverage checks

Pending.
