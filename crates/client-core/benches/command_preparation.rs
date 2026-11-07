//! Reproducible component costs of preparing a native client command, and of reading a completion
//! with the statement grammar.
//!
//! Outside the layer order: a benchmark harness. Product code must not name it.
//!
//! - **Owns.** The command and completion workloads, the raw timing, CPU and allocation samples of
//!   each stage a native client runs before it sends a command and of reading a completion with a
//!   held or a newly built grammar, and the memory a built grammar retains.
//! - **Depends on.** The public statement grammar, the execution identities and the wire request
//!   encoding.
//! - **Must not know.** Sessions, transports or servers: a command's round trip is measured by the
//!   client-wire scenario instruments.

use std::{collections::BTreeMap, fs, hint::black_box, num::NonZeroU64, path::PathBuf};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{ClientMessage, ClientRequest, CommandRequest, RequestId, SessionLimits};
use nervix_models::{CommandExecutionReference, DomainName, ResourceUploadIdentity};
use nervix_plexus::client_statement::{ClientStatementGrammar, parse_client_statement_sources};
use nervix_primitives::time::Instant;
use serde::Serialize;
use tikv_jemalloc_ctl::thread;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Rounds of every stage. A round runs each stage once, starting from a different stage each time,
/// so load the host adds over the run reaches every stage alike and their samples stay comparable.
const ROUNDS: usize = 400;
/// Untimed rounds before the measured ones, so first-use allocation does not enter the samples.
const WARMUP_ROUNDS: usize = 10;
const DOMAIN: &str = "wire_preparation";

/// One command a client prepares: the baseline instruments' command, and a batch of model
/// statements.
struct Workload {
    name: &'static str,
    query: &'static str,
}

const WORKLOADS: [Workload; 2] = [
    Workload {
        name: "describe_domain",
        query: "DESCRIBE DOMAIN;",
    },
    Workload {
        name: "model_batch",
        query: "CREATE SCHEMA event ( seq I64 ); CREATE RELAY events SCHEMA event UNBRANCHED; \
                CREATE RELAY copies SCHEMA event UNBRANCHED CAPACITY 4;",
    },
];

/// What a completion reads at the end of `input`: the grammar's expectations there, and the local
/// path being written there, as a server reads both for one completion request.
struct CompletionWorkload {
    name: &'static str,
    input: &'static str,
}

const COMPLETIONS: [CompletionWorkload; 2] = [
    CompletionWorkload {
        name: "relay_schema",
        input: "CREATE RELAY events SCHEMA ",
    },
    CompletionWorkload {
        name: "upload_path",
        input: "UPLOAD RESOURCE proto VERSION '/tmp/pro",
    },
];

#[derive(Serialize)]
struct Distribution {
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

impl Distribution {
    fn of(samples: &[u64]) -> Self {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Self {
            p50: quantile(&sorted, 50),
            p95: quantile(&sorted, 95),
            p99: quantile(&sorted, 99),
            max: sorted
                .last()
                .copied()
                .assured("every stage records samples"),
        }
    }
}

/// The nearest-rank quantile of `sorted`, which holds at least one sample.
fn quantile(sorted: &[u64], percent: usize) -> u64 {
    let rank = sorted
        .len()
        .checked_mul(percent)
        .assured("a sample count times a percentage fits usize")
        .div_ceil(100);
    let index = rank
        .checked_sub(1)
        .assured("a nonempty sample set ranks every quantile at one or more");
    sorted[index]
}

/// What this thread's scheduler statistics report, in nanoseconds.
#[derive(Clone, Copy)]
struct SchedulerTime {
    on_cpu: u64,
    run_queue_wait: u64,
}

impl SchedulerTime {
    /// Reads `/proc/thread-self/schedstat`, whose first two fields are the thread's time on a CPU
    /// and its time waiting on a run queue. The kernel advances them at scheduling events and
    /// ticks, so one short sample's share is coarse, while a stage's total over its samples is not.
    fn now() -> Self {
        let text = fs::read_to_string("/proc/thread-self/schedstat")
            .assured("the benchmark runs on Linux with scheduler statistics");
        let mut fields = text.split_whitespace();
        let on_cpu = fields
            .next()
            .assured("schedstat starts with the time on a CPU");
        let on_cpu = on_cpu
            .parse()
            .assured("schedstat counts the time on a CPU in whole nanoseconds");
        let run_queue_wait = fields
            .next()
            .assured("schedstat's second field is the run-queue wait");
        let run_queue_wait = run_queue_wait
            .parse()
            .assured("schedstat counts the run-queue wait in whole nanoseconds");
        Self {
            on_cpu,
            run_queue_wait,
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            on_cpu: self
                .on_cpu
                .checked_sub(earlier.on_cpu)
                .assured("a thread's CPU time never decreases"),
            run_queue_wait: self
                .run_queue_wait
                .checked_sub(earlier.run_queue_wait)
                .assured("a thread's run-queue wait never decreases"),
        }
    }
}

/// This thread's cumulative allocation counters.
struct ThreadAllocation {
    allocated: thread::ThreadLocal<u64>,
    deallocated: thread::ThreadLocal<u64>,
}

impl ThreadAllocation {
    fn new() -> Self {
        Self {
            allocated: thread::allocatedp::read()
                .assured("the benchmark uses jemalloc as its global allocator"),
            deallocated: thread::deallocatedp::read()
                .assured("the benchmark uses jemalloc as its global allocator"),
        }
    }

    fn allocated(&self) -> u64 {
        self.allocated.get()
    }

    fn deallocated(&self) -> u64 {
        self.deallocated.get()
    }
}

/// One run of a stage.
struct Sample {
    nanoseconds: u64,
    allocated_bytes: u64,
    scheduler: SchedulerTime,
}

/// Runs `operation` once and returns its sample with what it produced, which the caller drops
/// after the sample ends.
fn sampled<T>(allocation: &ThreadAllocation, operation: impl FnOnce() -> T) -> (Sample, T) {
    let scheduler_before = SchedulerTime::now();
    let allocated_before = allocation.allocated();
    let started = Instant::now();
    let output = black_box(operation());
    let elapsed = started.elapsed();
    let allocated_after = allocation.allocated();
    let scheduler = SchedulerTime::now().since(scheduler_before);
    let sample = Sample {
        nanoseconds: u64::try_from(elapsed.as_nanos())
            .assured("one benchmark sample lasts less than 584 years"),
        allocated_bytes: allocated_after
            .checked_sub(allocated_before)
            .assured("a thread's allocation counter never decreases"),
        scheduler,
    };
    (sample, output)
}

#[derive(Serialize)]
struct Stage {
    samples: usize,
    nanoseconds: Distribution,
    allocated_bytes: Distribution,
    /// How many runs of the stage a second of its measured time holds.
    operations_per_second: u64,
    /// Time this thread spent on a CPU over the stage's samples, divided by the samples.
    cpu_nanoseconds_per_sample: u64,
    /// Time this thread spent runnable but waiting for a CPU over the stage's samples, divided by
    /// the samples: how much other work on the host delayed them.
    run_queue_wait_nanoseconds_per_sample: u64,
    raw_nanoseconds: Vec<u64>,
    raw_allocated_bytes: Vec<u64>,
}

impl Stage {
    fn of(samples: &[Sample]) -> Self {
        let mut raw_nanoseconds = Vec::with_capacity(samples.len());
        let mut raw_allocated_bytes = Vec::with_capacity(samples.len());
        let mut measured = 0_u64;
        let mut on_cpu = 0_u64;
        let mut run_queue_wait = 0_u64;
        for sample in samples {
            measured = measured
                .checked_add(sample.nanoseconds)
                .assured("a stage's measured time fits u64 nanoseconds");
            raw_nanoseconds.push(sample.nanoseconds);
            raw_allocated_bytes.push(sample.allocated_bytes);
            on_cpu = on_cpu
                .checked_add(sample.scheduler.on_cpu)
                .assured("a stage's CPU time fits u64 nanoseconds");
            run_queue_wait = run_queue_wait
                .checked_add(sample.scheduler.run_queue_wait)
                .assured("a stage's run-queue wait fits u64 nanoseconds");
        }
        let count = u64::try_from(samples.len()).assured("a sample count fits u64");
        let operations_per_second = count
            .checked_mul(1_000_000_000)
            .assured("a sample count times a second in nanoseconds fits u64")
            .checked_div(measured)
            .assured("a stage's samples last at least a nanosecond");
        Self {
            samples: samples.len(),
            nanoseconds: Distribution::of(&raw_nanoseconds),
            allocated_bytes: Distribution::of(&raw_allocated_bytes),
            operations_per_second,
            cpu_nanoseconds_per_sample: on_cpu
                .checked_div(count)
                .assured("every stage records samples"),
            run_queue_wait_nanoseconds_per_sample: run_queue_wait
                .checked_div(count)
                .assured("every stage records samples"),
            raw_nanoseconds,
            raw_allocated_bytes,
        }
    }
}

/// A stage of preparing a command, which measures one run of itself.
struct StageRun<'a> {
    name: &'static str,
    run: Box<dyn FnMut() -> Sample + 'a>,
}

#[derive(Serialize)]
struct Case {
    name: &'static str,
    query: &'static str,
    statements: usize,
    stages: BTreeMap<&'static str, Stage>,
}

#[derive(Serialize)]
struct CompletionCase {
    name: &'static str,
    input: &'static str,
    /// How many expectations the grammar offers at the end of the input.
    expectations: usize,
    /// Whether the grammar expects a local path at the end of the input.
    local_path: bool,
    stages: BTreeMap<&'static str, Stage>,
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    profile: &'static str,
    git_commit: String,
    rustc: String,
    load_average_before: String,
    load_average_after: String,
    rounds: usize,
    /// The bytes a built grammar holds while a client keeps it.
    grammar_retained_bytes: u64,
    cases: Vec<Case>,
    completions: Vec<CompletionCase>,
}

/// The bytes a grammar holds once built: what it allocated and did not release while building.
fn grammar_retained_bytes(allocation: &ThreadAllocation) -> u64 {
    let allocated_before = allocation.allocated();
    let deallocated_before = allocation.deallocated();
    let grammar = ClientStatementGrammar::new();
    let allocated = allocation
        .allocated()
        .checked_sub(allocated_before)
        .assured("a thread's allocation counter never decreases");
    let deallocated = allocation
        .deallocated()
        .checked_sub(deallocated_before)
        .assured("a thread's deallocation counter never decreases");
    drop(black_box(grammar));
    allocated
        .checked_sub(deallocated)
        .assured("building a grammar releases no more than it allocates")
}

/// The stages a client runs to prepare `workload`, each measuring one run of itself.
fn preparation_stages<'a>(
    workload: &'a Workload,
    grammar: &'a ClientStatementGrammar,
    allocation: &'a ThreadAllocation,
    domain: &'a DomainName,
) -> Vec<StageRun<'a>> {
    let limits = SessionLimits::DEFAULT;
    let mut next_request_id = 0_u64;
    vec![
        StageRun {
            name: "grammar_build",
            run: Box::new(move || {
                let (sample, grammar) = sampled(allocation, ClientStatementGrammar::new);
                drop(grammar);
                sample
            }),
        },
        StageRun {
            name: "grammar_drop",
            run: Box::new(move || {
                let grammar = ClientStatementGrammar::new();
                let (sample, ()) = sampled(allocation, || drop(grammar));
                sample
            }),
        },
        StageRun {
            name: "parse_building_a_grammar",
            run: Box::new(move || {
                let (sample, parsed) = sampled(allocation, || {
                    parse_client_statement_sources(workload.query)
                        .assured("every benchmark workload is valid Plexus")
                });
                drop(parsed);
                sample
            }),
        },
        StageRun {
            name: "parse_with_held_grammar",
            run: Box::new(move || {
                let (sample, parsed) = sampled(allocation, || {
                    grammar
                        .parse_sources(workload.query)
                        .assured("every benchmark workload is valid Plexus")
                });
                drop(parsed);
                sample
            }),
        },
        StageRun {
            name: "execution_identity",
            run: Box::new(move || {
                let (sample, identity) = sampled(allocation, || {
                    let reference =
                        CommandExecutionReference::parse(uuid::Uuid::now_v7().to_string())
                            .assured("a hyphenated UUID is a valid execution reference");
                    let upload = ResourceUploadIdentity::parse(uuid::Uuid::now_v7().to_string())
                        .assured("a UUID string is a valid upload identity");
                    (reference, upload)
                });
                drop(identity);
                sample
            }),
        },
        StageRun {
            name: "request_encoding",
            run: Box::new(move || {
                next_request_id = next_request_id
                    .checked_add(1)
                    .assured("a benchmark sends fewer than u64::MAX requests");
                let request_id =
                    NonZeroU64::new(next_request_id).assured("request identities count from one");
                let execution_reference =
                    CommandExecutionReference::parse(uuid::Uuid::now_v7().to_string())
                        .assured("a hyphenated UUID is a valid execution reference");
                let message = ClientMessage {
                    request_id: RequestId::new(request_id),
                    request: ClientRequest::Command(CommandRequest {
                        query: workload.query.to_string(),
                        domain: Some(domain.clone()),
                        execution_reference,
                        expected_transaction_position: None,
                        expected_preview: None,
                    }),
                };
                let (sample, frame) = sampled(allocation, || {
                    message
                        .encode(&limits)
                        .assured("a short command fits one frame")
                });
                drop(frame);
                sample
            }),
        },
    ]
}

/// The stages a server runs for one completion of `workload`, each measuring one run of itself:
/// reading it with the grammar it holds, and with a grammar built and released for the request.
fn completion_stages<'a>(
    workload: &'a CompletionWorkload,
    grammar: &'a ClientStatementGrammar,
    allocation: &'a ThreadAllocation,
) -> Vec<StageRun<'a>> {
    let cursor = workload.input.len();
    vec![
        StageRun {
            name: "complete_building_a_grammar",
            run: Box::new(move || {
                let (sample, completion) = sampled(allocation, || {
                    let built = ClientStatementGrammar::new();
                    let expectations = built.suggest_expectations(workload.input, cursor);
                    let local_path = built.local_path_fragment(workload.input, cursor);
                    (expectations, local_path)
                });
                drop(completion);
                sample
            }),
        },
        StageRun {
            name: "complete_with_held_grammar",
            run: Box::new(move || {
                let (sample, completion) = sampled(allocation, || {
                    let expectations = grammar.suggest_expectations(workload.input, cursor);
                    let local_path = grammar.local_path_fragment(workload.input, cursor);
                    (expectations, local_path)
                });
                drop(completion);
                sample
            }),
        },
    ]
}

/// Runs every stage in interleaved rounds, after untimed warmup rounds, and summarizes each one.
fn measure(stages: &mut [StageRun<'_>]) -> BTreeMap<&'static str, Stage> {
    for round in 0..WARMUP_ROUNDS {
        for offset in 0..stages.len() {
            let index = (round + offset) % stages.len();
            black_box((stages[index].run)());
        }
    }
    let mut samples: Vec<Vec<Sample>> = stages.iter().map(|_| Vec::with_capacity(ROUNDS)).collect();
    for round in 0..ROUNDS {
        for offset in 0..stages.len() {
            let index = (round + offset) % stages.len();
            let sample = (stages[index].run)();
            samples[index].push(sample);
        }
    }
    let mut summaries = BTreeMap::new();
    for (stage, stage_samples) in stages.iter().zip(&samples) {
        summaries.insert(stage.name, Stage::of(stage_samples));
    }
    summaries
}

fn run_case(
    workload: &Workload,
    grammar: &ClientStatementGrammar,
    allocation: &ThreadAllocation,
) -> Case {
    let parsed = grammar
        .parse_sources(workload.query)
        .assured("every benchmark workload is valid Plexus");
    let domain = DomainName::parse(DOMAIN).assured("the benchmark domain is a valid name");
    let mut stages = preparation_stages(workload, grammar, allocation, &domain);
    Case {
        name: workload.name,
        query: workload.query,
        statements: parsed.len(),
        stages: measure(&mut stages),
    }
}

fn run_completion(
    workload: &CompletionWorkload,
    grammar: &ClientStatementGrammar,
    allocation: &ThreadAllocation,
) -> CompletionCase {
    let cursor = workload.input.len();
    let expectations = grammar.suggest_expectations(workload.input, cursor);
    let local_path = grammar.local_path_fragment(workload.input, cursor);
    let mut stages = completion_stages(workload, grammar, allocation);
    CompletionCase {
        name: workload.name,
        input: workload.input,
        expectations: expectations.len(),
        local_path: local_path.is_some(),
        stages: measure(&mut stages),
    }
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    let output = std::process::Command::new(program)
        .args(arguments)
        .output()
        .assured("git and rustc are installed for repository benchmarks");
    assert!(output.status.success(), "{program} failed");
    String::from_utf8(output.stdout)
        .assured("git and rustc print UTF-8")
        .trim()
        .to_owned()
}

fn load_average() -> String {
    fs::read_to_string("/proc/loadavg")
        .assured("the benchmark runs on Linux")
        .trim()
        .to_owned()
}

fn main() {
    // Cargo appends `--bench` when it runs a benchmark, so the output is the first other argument.
    let output = std::env::args()
        .skip(1)
        .find(|argument| !argument.starts_with("--"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/client-wire-command-cost.json"));
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let load_average_before = load_average();
    let allocation = ThreadAllocation::new();
    let grammar_retained_bytes = grammar_retained_bytes(&allocation);
    let grammar = ClientStatementGrammar::new();
    let cases = WORKLOADS
        .iter()
        .map(|workload| run_case(workload, &grammar, &allocation))
        .collect();
    let completions = COMPLETIONS
        .iter()
        .map(|workload| run_completion(workload, &grammar, &allocation))
        .collect();
    let report = Report {
        schema_version: 2,
        profile,
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        rustc: command_output("rustc", &["-Vv"]),
        load_average_before,
        load_average_after: load_average(),
        rounds: ROUNDS,
        grammar_retained_bytes,
        cases,
        completions,
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).assured("the benchmark output directory is writable");
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&report).assured("the report serializes"),
    )
    .assured("the benchmark output file is writable");
    eprintln!("command preparation cost report: {}", output.display());
}
