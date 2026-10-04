# SIMD Kernels

Nervix applies explicit SIMD to contiguous typed buffers where generated-instruction inspection
and measurement justify it. The optimization includes the caller: decoding into Arrow builders,
resolving a column once, retaining bitmaps, or folding a whole run removes the per-row structural
work that would otherwise hide a faster arithmetic loop.

This chapter owns the kernel boundary, dispatch, result guarantees, caller catalog, qualification,
and measured evidence. [Data Plane](./data-plane.md) owns batches, branches, persistence, and ACKs;
[VM Functions](./vm-functions.md) owns expression execution and window structures;
[Domain Clock](./domain-clock.md) owns time and admission;
[Data-Plane Concurrency](./data-plane-concurrency.md) owns synchronization and execution modes.
[Schemas And Codecs](./schemas-and-codecs.md), [Syslog](./syslog.md), and
[Connector Crates And The Connector Contract](./connector-contract.md) retain their wire contracts.

## Ownership And The Admission Rule

`nervix-simd-kernels`, in `crates/simd-kernels`, belongs to the **primitives** layer, below the
vocabulary. It owns portable operations on slices, offsets, validity bytes, and bitmask words.
It depends on `fearless_simd`, `simdutf8`, pointer-width conversions, and self-contained error
values. It must not know Arrow, Models, schemas, domain clocks, codecs, metric recorders,
connectors, branches, or execution graphs. A nanosecond buffer is an integer buffer here; the
caller gives those integers their meaning.

A new explicit kernel needs all three:

1. Inspection of the current caller's optimized code shows scalar or lane-extracting work.
   A source loop, a batch API, or an operation's name is insufficient evidence.
2. Every supported level and the forced fallback reproduce the complete scalar result,
   including failure bits, null masking at the caller, and unused tail bits. Integer, bitmap,
   and byte operations are exact. Window floating reductions follow the run contract below.
3. The caller supplies a typed contiguous slice or bitmap. Per-row boxing, field lookup, virtual
   type selection, or reconstruction from values already held in columns must be resolved first.

Kernel ownership does not grant clock, scheduling, synchronization, or allocation policy. The
caller retains its bounded-executor admission, cancellation between bounded units, memory charges,
and branch-local state. A kernel neither reads a clock nor starts a task. Its output is transient
values and masks, never a new persisted execution representation.

The repository's cast denial, checked arithmetic, typed errors, ownership headers, and debt
ratchets apply here. Source-owned compiler diagnostics and narrow reason-bearing expectations
govern architecture rules; a generated report or an approval count grants no exception.

## Dispatch And Build Targets

The crate caches one `fearless_simd::Level::new()` result in a process-wide `OnceLock` from
`nervix_primitives::unmodeled::sync`. The exact-file permission explains why CPU capability
detection may outlive individual model executions. It carries no protocol state or ordering that
a concurrency check relies on. Later calls reuse that level; short byte searches can return before
resolving it at all.

`dispatch!` enters the selected target-feature context. Small `#[inline(always)]` functions
generic over `S: Simd` perform the buffer loop there, using `simd.vectorize` where required.
The dispatch expression returns values or masks. Fallible validation and contextual errors stay
outside it; a dispatch body does not use `?` or return from its enclosing caller. Callers name
operations and types, and do not choose an instruction set or use `std::arch` themselves.

| Level | Where it is available | Selection contract |
| --- | --- | --- |
| SSE2 | x86 and x86-64 | The supported x86 baseline where no higher level is selected |
| SSE4.2 | x86 and x86-64 | Requires the level's complete feature set, including its auxiliary features |
| AVX2 | x86 and x86-64 | Includes the associated features required by `fearless_simd`, rather than testing AVX2 alone |
| AVX-512 | x86 and x86-64 | The complete Ice Lake class feature set; AVX-512F alone is insufficient |
| NEON | AArch64 | Selected from the target's NEON capability |
| WASM SIMD | wasm32 built with `simd128` | A build capability; browser feature detection does not dynamically enable it inside an already compiled module |
| Fallback | Targets without a supported SIMD level, and explicit tests | Portable scalar implementation; LLVM may still auto-vectorize it for the ambient build target |

The workspace enables `force_support_fallback` so tests can explicitly request
`Level::fallback()` even on SIMD hosts. This makes the fallback implementation reachable; it
does not make an AVX2 binary runnable on a CPU below that binary's baseline.

| Build | What the payload guarantees | What runtime selection adds |
| --- | --- | --- |
| Debian x86-64 release image | `Dockerfile.debian` defaults cargo-sonic payloads to `x86-64-v3`, including AVX2 | The same payload can select the Ice Lake class AVX-512 arm on a supporting host |
| AArch64 release image | The AArch64 target and its NEON capability | NEON execution; x86 levels do not exist in this build |
| Local `target-cpu=native` build | Features of the machine used to compile it | Selection cannot lower that ambient baseline; a native AVX-512 build can dispatch lower x86 tokens through its AVX-512 backend |
| Untuned local build | The compiler target's baseline | Host-supported higher levels through multiversion dispatch |
| WASM build | SIMD only when `simd128` is enabled | Selection within that module's compiled capabilities; no claim that the browser console invokes these server kernels |

The effective compiler flags matter. A native build's nominal SSE or AVX2 test token does not
prove that its lower backend executed. The production-target recipes build separate
`x86-64-v3` artifacts, allowing an AVX-512 host to exercise AVX2 as well as its higher runtime arm.
SSE and non-x86 qualification require builds and hosts that expose those backends. Current
instruction and timing evidence below is x86 evidence; it establishes no AArch64 or WASM timing.

## Results, Failures, And Bounds

| Result family | Required guarantee |
| --- | --- |
| Integer arithmetic and reductions | Exact values and failure classification; integer window sums accumulate in `i128` and check the declared result width at emission |
| Bitmaps | Bit zero names the first lane or byte; unused high bits in the last word or byte are clear |
| Byte classification and encoding | Exact member positions and masks, and byte-identical wire output under the owning codec's contract |
| Timestamp admission and latency | Exact inclusive admission and histogram bucket decisions over Arrow nanoseconds, including the full signed timestamp range |
| Window float sums and moments | Per-lane compensated sums and centered moments merged per run; run partitioning and addition order may affect rounding |
| VM float arithmetic | Existing scalar/compiler-vectorized arithmetic and non-finite rules; packing failure bytes does not change the arithmetic result |

Checked arithmetic returns the wrapped lane value together with its failure word, matching
`overflowing_*`. The VM intersects failures with operand validity, zeroes failed values, makes
them null, and constructs errors only for set bits. Null lanes cannot produce a message error,
including when their underlying value buffers contain overflowing operands. Scalar-null operands
produce an all-null result. Sliced arrays retain their value span and bitmap offset.

Integer sums, counts, comparisons, byte classes, and masks require exact scalar equality.
Floating window admission uses eight compensated accumulation lanes, two-pass centered moments,
and one Chan merge per run. This is a typed run algorithm, not a claim that every float loop has
an explicit `fearless_simd` backend. Different batch/run partitions can round differently under
the public window contract; changing VM float association is outside this boundary. Retraction
refolds survivors rather than subtracting floating statistics. Window ownership, snapshot restore,
and branch isolation remain defined by
[Window Aggregates And Sketches](./vm-functions.md#window-aggregates-and-sketches).

Output storage scales with the input run, or with a validated fixed histogram layout. Local
padding, lane reductions, and block masks have bounded sizes. Tails are masked before publication;
a padding value is never an extra admitted row or failure. Byte scanning uses overlapping loads
inside the existing buffer rather than allocating padded tails. Unsafe conversion is confined to
`XmlChars::into_string`: the spans accepted by `simdutf8` partition the complete buffer, so joining
them preserves UTF-8 and permits `String::from_utf8_unchecked`. Character rejection remains a
typed result, and invalid UTF-8 takes precedence over an excluded XML character.

## Kernel And Caller Catalog

The catalog includes library-dispatched operations and typed run algorithms so their contribution
is not mistaken for an explicit vector kernel. The callers retain schema validation, sensitivity,
route errors, ACK attribution, and branch ownership.

| Operation | Buffer API or library | Caller and retained work |
| --- | --- | --- |
| Live schemaful JSON decode | simd-json borrowed values, reusable `Buffers`, pre-resolved `KnownKey` | The open ingest group owns writable payload scratch and parser buffers; connector, endpoint, ingest-group, and lookup-file decoding append directly to typed Arrow builders |
| Columnar JSON encode | `JsonEscapeClassification` over string values and Arrow-style offsets | `nervix-columnar-json` prepares typed readers per batch and pre-escaped keys per schema; runtime wire codecs and ClickHouse `JSONEachRow` share the writer |
| Delivery latency and latest watermark | `ElapsedHistogram`, `ElapsedLayout`, `latest_instant`, `elapsed_nanos` over `&[i64]` | Relay delivery observations feed retained metrics handles at processor, reingestor, and emitter inputs; HDR bucket scatter and Prometheus local recording remain scalar |
| Paced-domain admission | `AdmissionKernel::admit` returns packed bytes | `DomainAdmissionWindow::admit_column` supplies reached centers and period; ingest groups select Arrow rows, timestamp metadata, and ACK sidecars together |
| Window admission and retraction | `RunValidity`, signed/unsigned sums, boolean counts, non-finite masks, compensated sums, moments, co-moments, bucket indexes, typed reverse visits | `ArgumentColumn` resolves type and validity once per run; branch-owned accumulators admit, retract, and rebuild retained rows; deque updates, scatter, and sketches remain irregular |
| VM failure packing | `FlagPacker::pack`, `WORD_LANES`, `lane_mask` | `numeric::Lanes` computes blocks of up to 1,024 lanes and packs one failure byte per lane; explicit arithmetic kernels already return failure words |
| Selection bitmaps | Arrow `BooleanBuffer` operations, `collect_bool`, `set_indices`, filter/take | VM predicates exclude null and failed rows; runtime selection and ingest branch masks retain packed form; required-output validation ORs inverted validity buffers |
| Checked integer arithmetic | `CheckedArithmetic`, `LaneOperands`, `CheckedLanes` | VM integer operators pass two runs or a shared operand; every signed/unsigned width has checked sums and differences, with widened products below 64 bits |
| Constant integer division | `UnsignedDivisor`, `SignedDivisor`, `ConstantDivision` | VM column-by-scalar `/` and `%` and fixed-unit datetime operations prepare the divisor once; measured 64-bit operations retain scalar reciprocal paths |
| SYSLOG header and structured-data scanning | `ByteClass::first_in`, `ByteScanner` | Compiled SYSLOG fields retain names and Arrow types; byte classes find header refusals and structured-data syntax before scalar parsing at those positions |
| Syslog TCP framing | memchr, retained read cursor, typed `FrameProgress` | The connector retains parsed octet counts and searches newly read bytes of split lines, with one compaction per read |
| SQS XML character admission | `XmlChars::into_string` and `XmlChars::admits`, with simdutf8 | The SQS sink validates bodies and string attributes; its integration owns size limits and reporting |
| OTEL trace/span identifiers | faster-hex | The OTEL connector decodes fixed-length hexadecimal IDs; malformed identifiers keep their connector outcome |

### JSON Boundaries

Decode copies a borrowed payload into reusable writable scratch because simd-json mutates its
input. It builds no intermediate serde JSON tree or materialized row map. Schema compilation
resolves field keys and Arrow types; datetime text is parsed once and base64 chunks append to the
binary builder. Exact integer shape/range, strict unknown-key rejection, nullability, invalid
UTF-8, and document limits remain codec decisions. Workspace `serde_json::preserve_order` remains
for JAQ's public object-member-order contract, independently of schemaful decoding.

Encode classifies quotes, backslashes, and controls below `0x20` over each string values buffer
in 64-byte blocks, then summarizes offset pairs into row masks. Non-ASCII UTF-8 remains verbatim.
Clean strings copy directly; marked rows enter escaping. Numbers use itoa/ryu; datetime and base64
text write without another escape scan. Sensitivity redaction, nested values, ClickHouse's widened
`F32` rendering, bounded-write refusal, and frozen retry bytes remain the owning encoder's contract.
The shared byte block classifier also implements the JSON escape class.

### Latency And Domain Admission

Relay metadata carries low/high ingestion watermarks as Arrow-compatible nanosecond buffers.
The delivery fold computes the latest watermark, excludes timestamps after the acceptance instant,
rounds elapsed time to milliseconds with halves up, clamps at 30 seconds, and counts HDR buckets.
The histogram layout validates the bounds under which floating lane division reproduces the
integer rounding exactly. Scalar bucket scatter is intentional. Each retained series merges the
fold into its four rolling windows under one lock with one wall-clock read, then flushes its local
Prometheus histogram once. The domain acceptance instant and wall-clock window maintenance remain
separate clock responsibilities.

Admission uses the first and last reached centers with inclusive skew. Ordered signed timestamp
differences become exact unsigned distances, including across the signed extrema. Interior rows
test their distance to the nearest period boundary. A window with no gaps needs no remainder;
powers of two use masks; other periods prepare a floor reciprocal and correct its estimate once.
The single-timestamp vocabulary check has the same semantics. A rejected or missing timestamp
affects its row alone, with the existing `validation`/`admit` error and ACK attribution.
The kernel does not establish which centers are eligible; [Admission Windows](./domain-clock.md#admission-windows)
defines that clock contract.

### Window Runs And Bitmap Work

Validity readers carry a bitmap and its bit offset and count present rows by popcount. Boolean
counts combine value and validity words. Signed/unsigned 64-bit sums accumulate low and high
32-bit halves in vector lanes, periodically reducing them to exact `i128` before lane overflow.
Smaller integer sums and generic min/max use typed scalar folds. Non-finite `F32`/`F64` inputs
produce column bitmaps. Bucket-index calculation and subsequent histogram scatter remain typed
scalar work. Sketch admission reuses a run key buffer; BLAKE3, t-digest insertion, and candidate
updates are not turned into lane-wise kernels.

Failure packing compares failure bytes with zero and extracts masks per register. Valid-lane-only
VM operations read validity by 64-lane words: empty words do no work, full words use the ordinary
loop, and mixed words evaluate only present lanes. One packing call covers the block, with clear
bits past its end. Explicit checked arithmetic bypasses the intermediate failure bytes entirely.
Selections use bitmap operations and Arrow filtering; required output columns with no nulls take
a fast path. These operations remove repeated per-row mask reconstruction, without changing
which row is selected or reported as failed.

### Checked Arithmetic And Constant Division

Sums and differences wrap at their own width. Signed operand/result signs determine overflow;
unsigned carry and borrow comparisons determine it. Products of 8-, 16-, and 32-bit operands widen
to twice the width, compare against the original bounds, and narrow by truncation. A 64-bit
checked product remains scalar because no wider exact lane is provided.

For a nonzero non-power-of-two divisor `d`, preparation computes `m = floor(2^64 / d)` once.
Discarding the low `64-w` bits yields `floor(2^w / d)` for width `w`. The high product gives a
quotient at most one below the exact quotient, and one remainder comparison corrects it.
Powers of two use a shift and mask. Signed division operates on unsigned magnitudes, including
`MIN`, before sign restoration. Truncating numeric division and Euclidean datetime division keep
their distinct rounding. Zero fails valid lanes; `MIN / -1` fails, while the numeric remainder
contract keeps `MIN % -1` equal to zero.

| Constant operation | Retained execution |
| --- | --- |
| Signed/unsigned 8-, 16-, 32-bit quotient and remainder | Explicit SIMD reciprocal lanes |
| `I64` quotient | Explicit SIMD multiply-high from four 32-bit partial products |
| `I64` remainder, `U64` quotient and remainder | Scalar multiply-high and correction, selected by measurement |
| Fixed-unit truncation/binning and `to_unix` | Prepared signed reciprocal in the datetime caller; scalar/compiler-vectorized loops |
| Fixed-unit `date_diff` | Checked `i64` subtraction and prepared truncating division; exact `i128` subtraction/division when the difference exceeds `i64`, followed by a checked `i64` result |
| `date_add`, `from_unix` | Checked `i128` multiplication/addition; `from_unix` has no division |
| Column divisor, including scalar-left/column-right | Existing checked scalar lane loop |

Binning normalizes the value and origin, corrects their phase difference by at most one stride,
and checks subtraction of the distance to the bin start. Calendar and zoned operations retain
their calendar rules. Divisor preparation may execute wide division once; selected reciprocal
lane bodies contain no integer division instruction. The exact wide `date_diff` fallback still
divides in `i128`.

### Byte Classes And Text Codecs

A `ByteClass` supplies an ordinary inclusive byte range, excluded bytes outside it, and listed
bytes inside it. A byte is a member when it lies outside the range without being excluded, or
is listed. Constants become splatted vector comparisons. Each excluded/listed list is bounded
by `MAX_NAMED_BYTES` at compile time; supported examples and a paired `compile_fail` doctest
exercise that current API restriction.

`first_in` stops at the first block holding a member. `ByteScanner` retains the current block's
mask for successive forward positions, classifying each block at most once. A final partial block
uses the last 64 bytes of a sufficiently long buffer and shifts away the overlap. Buffers shorter
than a block use 16-byte registers and an overlapping final load. `first_in` below 16 bytes uses the
scalar loop before resolving the level. No padded-tail copy is needed.

`XmlChars` checks the XML 1.0 `Char` ranges: tab, LF, CR, U+0020–U+D7FF, U+E000–U+FFFD, and
U+10000–U+10FFFF. The owned-byte path validates UTF-8 and classifies bounded 4 KiB spans,
carrying sequences across classification boundaries and adjusting validation boundaries to
character starts. Once a character is excluded it still validates the remaining bytes, because
invalid UTF-8 outranks character rejection. An already valid `&str` needs only character
classification. Syslog framing's memchr, UTF-8 validation's simdutf8, and OTEL's faster-hex choose
their own SIMD; the host adds no competing dispatch layer for their internals.

## Qualification

Tests compare every level available to their build/host, the baseline, and the forced fallback
with a complete scalar oracle. They cover empty runs, short inputs, every tail length, word/block
boundaries, sliced validity, nulls, shared operands, signed extrema, and unused bits. Byte
arithmetic and division include exhaustive operand pairs. Float-window tests check the documented
run algorithm and numerical bounds; they do not assert bit equality across different partitions.

Use the repository recipes and retain its kache compiler wrapper:

```bash
just test-simd-kernels
just test-vm
just test-bolero simd-
just build-simd-kernels-x86-64-v3
just build-vm-bench-x86-64-v3
```

Registered Bolero targets use the same production path and complete assertion in ordinary
randomized/corpus runs and sanitizer-backed libFuzzer. Current targets include
`simd-checked-lanes`, `simd-constant-division`, `simd-byte-classes`, and `simd-xml-chars`;
`syslog-stream-framing` and `syslog-structured-data` cover the connector/codec consumers.
Every applicable property must remain registered in `tests/bolero-targets.toml`. Ordinary runs
remain required on PRs; sanitizer CI runs only for PRs labeled `fuzz`. Scheduled,
`workflow_dispatch`, and unlabeled PR runs expect a sanitizer skip. A skip is neither execution
nor coverage evidence. Explicit qualification uses `just fuzz <target> <seconds>` or
`just fuzz-all <seconds>` and records the actual inventory executed. See
[Property Testing And Fuzzing](./property-testing-and-fuzzing.md).

Current Rust API restrictions use source-adjacent `compile_fail` doctests paired with compiling
examples of supported imports and signatures, run in normal tests and CI. Retain compiler-resolved
lint fixtures, feature/dependency/target checks, public scenarios, and model tests under their
owners. Use LLVM tools matching the producing compiler, as declared by its toolchain.

Public behavior retains Cucumber evidence on one and three nodes: exact JSON ingestion/emission,
batch latency metrics, reached admission-window edges, interleaved branch window aggregates,
sparse/dense/empty filters with row errors, checked numeric and datetime errors, split Syslog
TCP/TLS frames, SQS noncharacters, and OTEL ingestion. Kernel equivalence does not establish ACK,
branch, transport, or recovery semantics. Synchronization changes additionally need the evidence
required by [Data-Plane Concurrency](./data-plane-concurrency.md); a pure buffer kernel introduces
no claim about concurrent ordering.

## Measurement And Recorded Evidence

Inspect optimized instructions for the actual payload target, using `nm -C` and `objdump -d -C`
on the binaries the production-target recipes build. Check every operand shape and width,
including tails and fallback. A source abstraction's scalar implementation can be folded back
into vector instructions by LLVM; conversely, an early-exit loop can remain scalar even when
the target supports AVX2. Instruction evidence must name the revision, toolchain, flags, and host.

Criterion compares a kernel with the loop it replaces on identical operands in one process.
Include scalar/shared operands, null/error density, short inputs, and divisor preparation and
allocation where the caller pays them. Pass reference operations as inlinable closures: a
function pointer per lane measures indirect-call overhead. A byte scan's reference must preserve
its early exit; removing that exit can let LLVM vectorize a different algorithm.

```bash
just bench-json-encode
just bench-relay-interaction
just bench-window-admission
just bench-checked-lanes-x86-64-v3
just bench-constant-division-x86-64-v3
just bench-byte-classes-x86-64-v3
just bench-syslog-framing
just bench-vm numeric_kernels
just bench-vm datetime_kernels
just benchmark-ab <baseline-ref> 3 <workload>
```

An application-throughput claim requires serialized same-host `just benchmark-ab`, exact output
parity, and equivalent configuration. Pin timing runs to one core on mixed-core hosts and
interleave baseline/candidate rounds. Report intervals and interference; a bounded backlog rate
does not establish maximum sustainable throughput. Store new raw logs, benchmark outputs,
disassembly, and validation reports on the corresponding ClickUp task, rather than committing
transient artifacts. The summaries below are architectural evidence, not fresh measurements.

| Delivery | Recorded evidence | What it establishes and limits |
| --- | --- | --- |
| [01: live JSON decode](https://app.clickup.com/t/86bc85v80) | [Decode measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/simd-kernels-01.md), 2026-09-26, i9-14900HX/AVX2 | Direct builders and library dispatch; same-host Kafka means changed +0.0% and −0.8%, within spread and at backlog caps, so no throughput gain is established |
| [02: columnar JSON encode](https://app.clickup.com/t/86bc85v8c) | [Encode measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/simd-kernels-02.md), 2026-09-28, i9-14900HX | AVX2 `vpcmpeqb`/`vpmovmskb` and AVX-512 mask instructions; 1,024-row writer 77.8 µs versus a cheaper direct-serde reference 74.0 µs; bounded Kafka mean +3.9% does not isolate the writer's contribution |
| [03: batch latency](https://app.clickup.com/t/86bc85v94) | [Latency measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/simd-kernels-03.md), 2026-09-28 | Vector timestamp arithmetic with scalar bucket scatter; final 1,024-row spread-watermark recording 322.9 → 30.66 µs, shared-watermark 415.5 → 7.896 µs; bounded processor A/B −0.7% establishes no application gain |
| [04: domain admission](https://app.clickup.com/t/86bc85v9t) | [Admission measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/simd-kernels-04.md), 2026-09-29, i9-14900HX | Exact scalar/level equivalence and public paced-window edges; noisy +8.0% mean came from an unpaced workload, measuring surrounding metadata/selection changes rather than paced SIMD arithmetic |
| [05: window runs](https://app.clickup.com/t/86bc85vat) | [Window measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/simd-kernels-05.md), 2026-09-29 | AVX-512 integer sum adds/shifts; combined four-kernel 4,096-row fixture 30.696 µs, not isolated kernel throughput; bounded window A/B mean −0.04% is within spread |
| [06: packed failures and selections](https://app.clickup.com/t/86bc85vbf) | [Bitmap measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/simd-kernels-06.md), 2026-09-29, Ryzen AI 9 HX 370 | Packing's per-lane shifts removed; AVX2 compare/movemask and AVX-512 mask extraction; 65,536-row arithmetic-filter program 465.08 → 423.05 µs; native and production-target checks distinguish dispatch arms |
| [07: checked integers](https://app.clickup.com/t/86bc85vc3) | [Arithmetic measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/simd-kernels-07.md), 2026-09-30, Ryzen AI 9 HX 370 | Signed sums 2.2–6.0× and narrow products 1.4–4.1× faster in same-process kernel comparisons; unsigned 8/16-bit sums at parity; VM rounds varied by up to 2.3× under shared load |
| [08: constant division](https://app.clickup.com/t/86bc85vcf) | [Division measurements](https://github.com/nervix-io/nervix/blob/main/benches/reports/simd-kernels-08.md), 2026-09-30, i9-14900HX/AVX2 | Exact reciprocal loops without per-lane `idiv`; measured vector/scalar selection, kernel ratios, and VM timings are distinct observations, with no end-to-end throughput measurement |
| [09: text scanning](https://app.clickup.com/t/86bc85vdw) | Implementation evidence on that task, 2026-10-03, PR #600, rustc 1.99/LLVM 22, production target | Replaced early-exit loops were scalar; AVX2 byte compares/movemasks and AVX-512 masks, with no kernel tail `memset`/`memcpy`; short headers reach parity at 8 bytes, about 3× at 48 and 15× at 255; no Kafka A/B workload represents Syslog/SQS/OTEL |

The arithmetic inspection corrected the initial assumption about `I64`: every signed sum and
difference was a scalar flag-per-lane loop in the inspected `x86-64-v3` VM, while unsigned sums
and differences were already vectorized. Own-width overflow checks also measured faster than
widening sums (native `i8`: 93 versus 161 ns; `i32`: 171 versus 247 ns per 1,024 lanes).
Unsigned operations now share the checked-arithmetic owner, with parity or a measured improvement;
their presence is not evidence that LLVM previously left them scalar.

Constant-division production-target experiments measured `I64` quotient at 1,302 ns in vector
lanes versus 1,745 ns with scalar reciprocal, but `U64` quotient at 856 versus 788 ns and
`U64` remainder at 877 versus 797 ns. A repeated `I64` remainder comparison measured 1,685 versus
1,534 ns. These 1,024-lane comparisons explain the retained scalar operations. Separately, VM
truncation/binning measured 20.005 → 14.348 µs, fixed elapsed arithmetic 9.546 → 7.329 µs, and
numeric constant programs about 26–68% less time. Combined `to_unix`/`from_unix` remained unchanged;
the latter is multiplication. Unchanged controls also moved under shared-host load, so these are
kernel/VM observations, not application-throughput guarantees.

Text-scanning qualification found that a padded-tail copy compiled into `memset`/`memcpy` and
made an 8-byte header 4.5× slower. The shipped overlapping-load and below-16-byte scalar paths
remove that cost. This is why every byte-kernel benchmark includes short inputs. Framer A/B used
one MiB of frames read eight KiB at a time, with interleaved pinned rounds against the prior
decoder; it remains connector-level evidence rather than a Kafka pipeline measurement.

## Adding A Kernel And Scope Limits

1. Resolve the typed caller and ownership first. Reuse Arrow or an existing dispatched library
   when it supplies the exact semantics. Inspect the current optimized loop before adding code.
2. Write the full scalar contract, including bounds, signed arithmetic, failures, bitmap offsets,
   empty input, and tails. Establish a failing focused check; observable behavior also needs its
   public Cucumber scenario before product changes.
3. Put the operation at the primitives boundary. Validate fallible inputs before dispatch,
   resolve the cached level, and return values/masks from the generic buffer body. Keep allocation
   proportional and local padding bounded; keep irregular scatter in its owning caller.
4. Qualify every available backend and forced fallback, with appropriate baseline builds.
   Register differential properties in both Bolero modes and pair API-rejection doctests with
   supported examples. Record the limits of host/target coverage.
5. Measure the complete caller and inspect generated instructions. Keep a scalar operation where
   it wins. Any end-to-end claim additionally needs same-host A/B. Publish raw evidence on the
   task and update this chapter and the caller's authoritative chapter in the same change.
6. Run focused checks and `just validate`; lower ratchet baselines that fall. Keep public
   contracts and NSPL skill routing current when their surfaces change, then run `just book dev`.

Do not add explicit kernels for operations LLVM already vectorizes without a measured caller
benefit, or duplicate Arrow and library dispatch. Division by a column has no constant reciprocal
and stays scalar. Checked 64-bit multiplication lacks a wider exact lane. VM floating arithmetic
and platform-libm transcendental calls keep their current contracts. Branch keys, deduplicator and
reorderer keys, lookups, correlator keys, and subscription sampling still contain irregular
string/map/hash work: typed keys and a demonstrated buffer algorithm must precede SIMD there.
No kernel may combine state across concrete branches, weaken schema/sensitivity checks, change
clock authority, hide an error, or persist hot-path payloads.
