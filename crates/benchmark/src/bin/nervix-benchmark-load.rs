#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the load harness owns measurement and client coordination outside the product \
                  graph"
    )
)]

use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail, ensure};
use arch_into::ArchInto as _;
use clap::{Parser, Subcommand};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_benchmark::LoadShape;
use nervix_primitives::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        blocking::Mutex,
    },
    thread,
    time::Instant,
};
use nervix_recovery::Discarded as _;
use rdkafka::{
    ClientContext, Message, Offset, TopicPartitionList,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
};
use serde::{Deserialize, Serialize};

const SEND_CLOCK_MESSAGES: u64 = 65_536;
const OFFSET_POLL_INTERVAL: Duration = Duration::from_millis(100);
const KAFKA_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const GENERATION_QUERY_DEADLINE_TOLERANCE: Duration = Duration::from_secs(1);
const PARITY_STABILITY_INTERVAL: Duration = Duration::from_millis(500);
const SUMMARY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const RECENT_SUMMARY_LIMIT: usize = 32;
const KEY_CYCLE_DIGITS: usize = 14;
const KEY_INDEX_DIGITS: usize = 6;
const KEY_DIGITS: usize = KEY_CYCLE_DIGITS + KEY_INDEX_DIGITS;
const UNIFORM_ID_DIGITS: usize = 16;
const RETAIN_MARKER: u8 = b'x';
const PADDING: u8 = b'y';

/// How long the producer keeps trying to deliver a message before it reports the message failed.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a throughput-batched record batch waits for more messages before it is sent.
const THROUGHPUT_LINGER: Duration = Duration::from_millis(5);
/// The most messages a throughput-batched record batch holds.
const THROUGHPUT_BATCH_MESSAGES: u64 = 10_000;
/// How long a record batch holding part of a keyed cycle waits for the rest of the cycle. A cycle
/// whose messages take longer than this to queue fails the run, so no record batch is ever sent
/// with part of a cycle.
const CYCLE_LINGER: Duration = Duration::from_secs(50);
// librdkafka refuses a linger that is not shorter than the delivery timeout.
const _: () = assert!(CYCLE_LINGER.as_millis() < DELIVERY_TIMEOUT.as_millis());
/// The byte budget librdkafka gives the record batches the producer's `batch.size` asks for.
const PRODUCER_BATCH_BYTES: usize = 1_048_576;
/// The most bytes one record batch, and one produce request, may hold: the producer's
/// `message.max.bytes`. librdkafka closes a batch at the smaller of this and
/// `PRODUCER_BATCH_BYTES`.
const RECORD_BATCH_BYTES: usize = 1_000_000;
// A keyed cycle is checked against this limit, so it must be the one that closes a batch.
const _: () = assert!(RECORD_BATCH_BYTES <= PRODUCER_BATCH_BYTES);
/// What librdkafka reserves for the framing of the next record when it decides whether that record
/// still fits its batch: the most the varint fields of a version 2 record can occupy.
const RECORD_FRAMING_BYTES: usize = 36;
/// The part of a record batch's byte budget the produce request and record batch headers take: the
/// fixed fields, a topic name of at most 249 bytes, and the 61-byte record batch header.
const BATCH_HEADER_BYTES: usize = 1024;

#[derive(Debug, Parser)]
#[command(about = "Drive a Kafka-to-Kafka end-to-end streaming benchmark")]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
    #[command(subcommand)]
    shape: ShapeArgs,
}

#[derive(Debug, clap::Args)]
struct CommonArgs {
    #[arg(long)]
    bootstrap_servers: String,
    #[arg(long)]
    input_topic: String,
    #[arg(long)]
    output_topic: String,
    #[arg(long)]
    consumer_group: String,
    #[arg(long)]
    minimum_consumers: usize,
    #[arg(long, default_value_t = 30)]
    duration_seconds: u64,
    #[arg(long)]
    warmup_seconds: u64,
    #[arg(long, default_value_t = 128)]
    value_bytes: usize,
    #[arg(long, default_value_t = 16_384)]
    max_backlog_messages: u64,
    #[arg(long, default_value_t = 120)]
    wait_timeout_seconds: u64,
    #[arg(long)]
    ready_file: PathBuf,
    #[arg(long)]
    go_file: PathBuf,
    #[arg(long)]
    output_diagnostics_file: PathBuf,
}

/// The load shape the workload declares, with the arguments each shape requires.
#[derive(Debug, Subcommand)]
enum ShapeArgs {
    /// Distinct record IDs and one unchanged output per input.
    UniformPassthrough,
    /// Distinct record IDs and one uppercase output per input.
    UniformUppercase,
    /// One retained and one filtered input per cycle, yielding one uppercase output.
    UniformFilterMap,
    /// Distinct record IDs copied to a fixed number of output consumers.
    UniformFanout {
        #[arg(long)]
        outputs_per_input: u64,
    },
    /// Cycles of distinct keys whose duplicates and filtered-out payloads are dropped by the
    /// measured path, aggregated into window summaries carrying a record count.
    KeyedWindowed {
        #[arg(long)]
        keys_per_cycle: u64,
        #[arg(long)]
        retained_keys: u64,
        #[arg(long)]
        copies_per_key: u64,
        #[arg(long)]
        count_field: String,
    },
}

impl ShapeArgs {
    fn into_shape(self) -> LoadShape {
        match self {
            Self::UniformPassthrough => LoadShape::UniformPassthrough,
            Self::UniformUppercase => LoadShape::UniformUppercase,
            Self::UniformFilterMap => LoadShape::UniformFilterMap,
            Self::UniformFanout { outputs_per_input } => {
                LoadShape::UniformFanout { outputs_per_input }
            }
            Self::KeyedWindowed {
                keys_per_cycle,
                retained_keys,
                copies_per_key,
                count_field,
            } => LoadShape::KeyedWindowed {
                keys_per_cycle,
                retained_keys,
                copies_per_key,
                count_field,
            },
        }
    }
}

struct DeliveryState {
    succeeded: AtomicU64,
    failed: AtomicU64,
    first_error: Mutex<Option<String>>,
}

#[derive(Clone)]
struct DeliveryContext {
    state: Arc<DeliveryState>,
}

impl Default for DeliveryContext {
    fn default() -> Self {
        Self {
            state: Arc::new(DeliveryState {
                succeeded: AtomicU64::new(0),
                failed: AtomicU64::new(0),
                first_error: Mutex::new(None),
            }),
        }
    }
}

impl DeliveryContext {
    fn succeeded(&self) -> u64 {
        self.state.succeeded.load(AtomicOrdering::Relaxed)
    }

    fn failed(&self) -> u64 {
        self.state.failed.load(AtomicOrdering::Relaxed)
    }

    fn first_error(&self) -> Option<String> {
        self.state.first_error.lock().clone()
    }
}

impl ClientContext for DeliveryContext {}

impl ProducerContext for DeliveryContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {
        match result {
            Ok(_) => {
                self.state.succeeded.fetch_add(1, AtomicOrdering::Relaxed);
            }
            Err((error, _)) => {
                self.state.failed.fetch_add(1, AtomicOrdering::Relaxed);
                let mut first_error = self.state.first_error.lock();
                if first_error.is_none() {
                    *first_error = Some(error.to_string());
                }
            }
        }
    }
}

/// Builds one shape's wire payloads, reusing prepared buffers so the send loop never allocates.
enum PayloadWriter {
    Uniform {
        retained: Vec<u8>,
        dropped: Option<Vec<u8>>,
        id_offset: usize,
    },
    Keyed {
        retained: Vec<u8>,
        dropped: Vec<u8>,
        key_offset: usize,
        keys_per_cycle: u64,
        retained_keys: u64,
    },
}

impl PayloadWriter {
    fn new(shape: &LoadShape, value_bytes: usize) -> Result<Self> {
        match shape {
            LoadShape::UniformPassthrough
            | LoadShape::UniformUppercase
            | LoadShape::UniformFilterMap
            | LoadShape::UniformFanout { .. } => {
                let prefix = br#"{"id":""#;
                let infix = br#"","value":""#;
                let suffix = br#""}"#;
                let build = |value_byte| {
                    let mut payload = Vec::with_capacity(
                        prefix.len() + UNIFORM_ID_DIGITS + infix.len() + value_bytes + suffix.len(),
                    );
                    payload.extend_from_slice(prefix);
                    payload.extend(std::iter::repeat_n(b'0', UNIFORM_ID_DIGITS));
                    payload.extend_from_slice(infix);
                    payload.extend(std::iter::repeat_n(value_byte, value_bytes));
                    payload.extend_from_slice(suffix);
                    payload
                };
                Ok(Self::Uniform {
                    retained: build(b'x'),
                    dropped: matches!(shape, LoadShape::UniformFilterMap).then(|| build(b'y')),
                    id_offset: prefix.len(),
                })
            }
            LoadShape::KeyedWindowed {
                keys_per_cycle,
                retained_keys,
                ..
            } => {
                ensure!(
                    *keys_per_cycle
                        <= 16_u64.pow(u32::try_from(KEY_INDEX_DIGITS).assured(
                            "the hexadecimal key digit count is a small compile-time constant",
                        )),
                    "keys_per_cycle exceeds the {KEY_INDEX_DIGITS} hexadecimal digits reserved \
                     for the key index"
                );
                let prefix = br#"{"key":""#;
                let infix = br#"","value":""#;
                let suffix = br#""}"#;
                let build = |marker: Option<u8>| {
                    let mut payload = Vec::with_capacity(
                        prefix.len() + KEY_DIGITS + infix.len() + value_bytes + suffix.len(),
                    );
                    payload.extend_from_slice(prefix);
                    payload.extend(std::iter::repeat_n(b'0', KEY_DIGITS));
                    payload.extend_from_slice(infix);
                    payload.extend(std::iter::repeat_n(PADDING, value_bytes));
                    if let Some(marker) = marker {
                        let value_offset = prefix.len() + KEY_DIGITS + infix.len();
                        payload[value_offset] = marker;
                    }
                    payload.extend_from_slice(suffix);
                    payload
                };
                Ok(Self::Keyed {
                    retained: build(Some(RETAIN_MARKER)),
                    dropped: build(None),
                    key_offset: prefix.len(),
                    keys_per_cycle: *keys_per_cycle,
                    retained_keys: *retained_keys,
                })
            }
        }
    }

    fn wire_bytes(&self) -> usize {
        match self {
            Self::Uniform { retained, .. } => retained.len(),
            Self::Keyed { retained, .. } => retained.len(),
        }
    }

    /// The payload for message `index` of cycle `cycle`.
    ///
    /// Keyed cycles emit one pass over the whole key list per copy, so a key's duplicates are
    /// `keys_per_cycle` messages apart on one partition, far enough to exercise a live keyspace.
    /// `RecordBatching::WholeCycles` sends each cycle as one Kafka record batch, so the duplicates
    /// reach the deduplicator together and its expiry can never split them.
    fn payload(&mut self, cycle: u64, index: u64) -> &[u8] {
        match self {
            Self::Uniform {
                retained,
                dropped,
                id_offset,
            } => {
                let payload = if index == 1 {
                    dropped
                        .as_mut()
                        .verified("only the uniform filter-map shape has a second input per cycle")
                } else {
                    retained
                };
                let mut slot = &mut payload[*id_offset..*id_offset + UNIFORM_ID_DIGITS];
                write!(slot, "{cycle:016x}")
                    .assured("fmt::Write over an in-memory buffer has no failure mode");
                payload
            }
            Self::Keyed {
                retained,
                dropped,
                key_offset,
                keys_per_cycle,
                retained_keys,
            } => {
                let key_index = index % *keys_per_cycle;
                let payload = if key_index < *retained_keys {
                    retained
                } else {
                    dropped
                };
                let mut slot = &mut payload[*key_offset..*key_offset + KEY_DIGITS];
                write!(
                    slot,
                    "{cycle:0cycle_digits$x}{key_index:0index_digits$x}",
                    cycle_digits = KEY_CYCLE_DIGITS,
                    index_digits = KEY_INDEX_DIGITS,
                )
                .assured("fmt::Write over an in-memory buffer has no failure mode");
                payload
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
struct OutputCounts {
    messages: u64,
    records: u64,
}

impl OutputCounts {
    fn since(self, baseline: Self) -> Result<Self> {
        Ok(Self {
            messages: self
                .messages
                .checked_sub(baseline.messages)
                .context("output topic message count fell below its warm-up baseline")?,
            records: self
                .records
                .checked_sub(baseline.records)
                .context("output record count fell below its warm-up baseline")?,
        })
    }
}

/// How the driver learns what the measured path produced.
///
/// A pass-through shape emits one message per input, so Kafka's high watermarks are the record
/// count. A windowed shape emits one summary per closed window, a cardinality that is not a
/// function of the input count, so the driver has to read the summaries and sum the record count
/// they carry.
enum OutputMeter {
    Watermarks,
    Summaries(SummaryDrain),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UniformOutput<'a> {
    id: &'a str,
    value: &'a str,
}

/// Checks each output identity and value after the timed measurement has finished.
struct UniformRecordAudit {
    seen: Vec<u64>,
    copies_per_cycle: u64,
    value_byte: u8,
    value_bytes: usize,
}

impl UniformRecordAudit {
    fn new(shape: &LoadShape, cycles: u64, value_bytes: usize) -> Result<Self> {
        let (copies_per_cycle, value_byte) = match shape {
            LoadShape::UniformPassthrough => (1, b'x'),
            LoadShape::UniformUppercase | LoadShape::UniformFilterMap => (1, b'X'),
            LoadShape::UniformFanout { outputs_per_input } => (*outputs_per_input, b'x'),
            LoadShape::KeyedWindowed { .. } => {
                bail!("window summaries require aggregate validation")
            }
        };
        let cycles: usize = cycles
            .try_into()
            .context("uniform output cycle count does not fit in memory")?;
        let mut seen = Vec::new();
        seen.try_reserve_exact(cycles)
            .context("failed to reserve uniform output identity counts")?;
        seen.resize(cycles, 0);
        Ok(Self {
            seen,
            copies_per_cycle,
            value_byte,
            value_bytes,
        })
    }

    fn expected_messages(&self) -> Result<u64> {
        u64::try_from(self.seen.len())
            .context("uniform output cycle count does not fit in the report")?
            .checked_mul(self.copies_per_cycle)
            .context("uniform output message count overflowed")
    }

    fn observe(&mut self, payload: &[u8]) -> Result<()> {
        let output: UniformOutput<'_> =
            serde_json::from_slice(payload).context("uniform output is not an id/value record")?;
        ensure!(
            output.id.len() == UNIFORM_ID_DIGITS,
            "uniform output id has {} digits, expected {UNIFORM_ID_DIGITS}",
            output.id.len()
        );
        let cycle =
            u64::from_str_radix(output.id, 16).context("uniform output id is not hexadecimal")?;
        let index: usize = cycle
            .try_into()
            .context("uniform output id does not fit in memory")?;
        let count = self.seen.get_mut(index).with_context(|| {
            format!(
                "uniform output id {output_id} was never produced",
                output_id = output.id
            )
        })?;
        ensure!(
            *count < self.copies_per_cycle,
            "uniform output id {} appeared more than {} times",
            output.id,
            self.copies_per_cycle
        );
        ensure!(
            output.value.len() == self.value_bytes
                && output
                    .value
                    .as_bytes()
                    .iter()
                    .all(|byte| *byte == self.value_byte),
            "uniform output id {} has an unexpected value",
            output.id
        );
        *count += 1;
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        for (cycle, count) in self.seen.iter().enumerate() {
            ensure!(
                *count == self.copies_per_cycle,
                "uniform output id {cycle:016x} appeared {count} times, expected {}",
                self.copies_per_cycle
            );
        }
        Ok(())
    }
}

#[derive(Default)]
struct DrainState {
    messages: AtomicU64,
    records: AtomicU64,
    stop: AtomicBool,
    failure: Mutex<Option<String>>,
    observations: Mutex<SummaryObservations>,
}

#[derive(Clone, Debug, Default)]
struct SummaryObservations {
    partitions: BTreeMap<i32, OutputCounts>,
    recent: VecDeque<SummaryObservation>,
}

#[derive(Clone, Debug, Serialize)]
struct SummaryObservation {
    partition: i32,
    offset: i64,
    record_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    distinct_estimate: Option<u64>,
}

#[derive(Debug, Serialize)]
struct OutputPartitionDiagnostics {
    partition: i32,
    messages: u64,
    records: u64,
}

#[derive(Debug, Serialize)]
struct OutputDiagnostics {
    topic: String,
    total: OutputCounts,
    partitions: Vec<OutputPartitionDiagnostics>,
    recent_summaries: Vec<SummaryObservation>,
    consumer_failure: Option<String>,
}

/// Consumes window summaries from the output topic and accumulates the record count they report.
struct SummaryDrain {
    state: Arc<DrainState>,
    worker: Option<thread::JoinHandle<()>>,
}

impl SummaryDrain {
    fn start(
        bootstrap_servers: &str,
        topic: &str,
        partitions: &[i32],
        count_field: &str,
    ) -> Result<Self> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", format!("{topic}_drain"))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .context("failed to create the benchmark summary consumer")?;
        let mut assignment = TopicPartitionList::new();
        for partition in partitions {
            assignment
                .add_partition_offset(topic, *partition, Offset::Beginning)
                .with_context(|| {
                    format!("failed to assign summary topic '{topic}' partition {partition}")
                })?;
        }
        consumer
            .assign(&assignment)
            .with_context(|| format!("failed to assign the summary topic '{topic}'"))?;

        let state = Arc::new(DrainState::default());
        let worker_state = Arc::clone(&state);
        let count_field = count_field.to_string();
        let worker = thread::Builder::new()
            .name("benchmark-summary-drain".to_string())
            .spawn(move || worker_state.consume(&consumer, &count_field))
            .context("failed to start the benchmark summary consumer thread")?;
        Ok(Self {
            state,
            worker: Some(worker),
        })
    }

    fn counts(&self) -> Result<OutputCounts> {
        if let Some(failure) = self.state.failure.lock().clone() {
            bail!("benchmark summary consumer failed: {failure}");
        }
        Ok(OutputCounts {
            messages: self.state.messages.load(AtomicOrdering::Relaxed),
            records: self.state.records.load(AtomicOrdering::Relaxed),
        })
    }
}

impl DrainState {
    fn record_summary(&self, observation: SummaryObservation) {
        self.messages.fetch_add(1, AtomicOrdering::Relaxed);
        self.records
            .fetch_add(observation.record_count, AtomicOrdering::Relaxed);
        let mut observations = self.observations.lock();
        let partition = observations
            .partitions
            .entry(observation.partition)
            .or_default();
        partition.messages += 1;
        partition.records += observation.record_count;
        if observations.recent.len() == RECENT_SUMMARY_LIMIT {
            observations.recent.pop_front();
        }
        observations.recent.push_back(observation);
    }

    fn consume(&self, consumer: &BaseConsumer, count_field: &str) {
        while !self.stop.load(AtomicOrdering::Relaxed) {
            let Some(result) = consumer.poll(SUMMARY_POLL_INTERVAL) else {
                continue;
            };
            let message = match result {
                Ok(message) => message,
                Err(error) => {
                    self.failure.lock().get_or_insert(error.to_string());
                    return;
                }
            };
            let Some(payload) = message.payload() else {
                self.failure
                    .lock()
                    .get_or_insert("output summary has no payload".to_string());
                return;
            };
            let output = match serde_json::from_slice::<serde_json::Value>(payload) {
                Ok(output) => output,
                Err(error) => {
                    let failure = format!("output summary is not JSON: {error}");
                    self.failure.lock().get_or_insert(failure);
                    return;
                }
            };
            let Some(record_count) = output.get(count_field).and_then(serde_json::Value::as_u64)
            else {
                let failure = format!("output summary has no unsigned '{count_field}' field");
                self.failure.lock().get_or_insert(failure);
                return;
            };
            self.record_summary(SummaryObservation {
                partition: message.partition(),
                offset: message.offset(),
                record_count,
                distinct_estimate: output
                    .get("distinct_estimate")
                    .and_then(serde_json::Value::as_u64),
            });
        }
    }
}

impl Drop for SummaryDrain {
    fn drop(&mut self) {
        self.state.stop.store(true, AtomicOrdering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker.join().discarded(
                "the worker was asked to stop above, and a summary it fails to finish is not part \
                 of the measurement",
            );
        }
    }
}

/// How the producer cuts each partition's queue of input messages into Kafka record batches.
///
/// The broker appends a record batch whole and serves it whole, but nothing bounds the time
/// between two batches of one partition: a broker that stalls holds the later batch back for as
/// long as it stalls, and the measured path reads that batch that much later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordBatching {
    /// A batch is sent after `THROUGHPUT_LINGER` or once it holds `THROUGHPUT_BATCH_MESSAGES`
    /// messages or its byte budget, so one cycle can span batches. Every uniform input is owed its
    /// own output, so it does not matter which messages travel together.
    Throughput,
    /// Every batch holds exactly one cycle. A batch is sent when the last message of its cycle is
    /// queued, and the driver flushes the producer only between cycles. The copies of each key
    /// therefore reach the deduplicator together however long the broker stalls between batches,
    /// and its `MAX TIME` never expires a key between them.
    WholeCycles { messages_per_cycle: u64 },
}

impl RecordBatching {
    /// The batching `shape` needs when each of its messages takes `wire_bytes` bytes.
    ///
    /// A keyed cycle that cannot fit in one record batch is refused, because its copies of a key
    /// could then travel in different batches.
    fn for_shape(shape: &LoadShape, wire_bytes: usize) -> Result<Self> {
        let messages_per_cycle = match shape {
            LoadShape::UniformPassthrough
            | LoadShape::UniformUppercase
            | LoadShape::UniformFilterMap
            | LoadShape::UniformFanout { .. } => return Ok(Self::Throughput),
            LoadShape::KeyedWindowed { .. } => shape.messages_per_cycle(),
        };
        let record_bytes = wire_bytes
            .checked_add(RECORD_FRAMING_BYTES)
            .context("keyed record size overflowed")?;
        let cycle_records = usize::try_from(messages_per_cycle)
            .context("keyed cycle message count does not fit in memory")?;
        let records_bytes = cycle_records
            .checked_mul(record_bytes)
            .context("keyed cycle size overflowed")?;
        let cycle_bytes = records_bytes
            .checked_add(BATCH_HEADER_BYTES)
            .context("keyed cycle size overflowed")?;
        ensure!(
            cycle_bytes <= RECORD_BATCH_BYTES,
            "a keyed cycle of {messages_per_cycle} messages of {wire_bytes} bytes needs up to \
             {cycle_bytes} bytes, more than the {RECORD_BATCH_BYTES} bytes of the one record \
             batch that keeps every key's copies together"
        );
        Ok(Self::WholeCycles { messages_per_cycle })
    }

    /// The configuration of the producer that writes the benchmark's input records.
    fn producer_config(self, bootstrap_servers: &str) -> ClientConfig {
        let (linger, batch_messages) = match self {
            Self::Throughput => (THROUGHPUT_LINGER, THROUGHPUT_BATCH_MESSAGES),
            Self::WholeCycles { messages_per_cycle } => (CYCLE_LINGER, messages_per_cycle),
        };
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_servers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set(
                "delivery.timeout.ms",
                DELIVERY_TIMEOUT.as_millis().to_string(),
            )
            .set("linger.ms", linger.as_millis().to_string())
            .set("batch.size", PRODUCER_BATCH_BYTES.to_string())
            .set("message.max.bytes", RECORD_BATCH_BYTES.to_string())
            .set("batch.num.messages", batch_messages.to_string())
            .set("queue.buffering.max.messages", "1048576")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("compression.type", "none");
        config
    }

    /// Fails a keyed cycle whose messages took `elapsed` to queue when that is long enough for
    /// its record batch to have been sent before the cycle's last message joined it.
    fn check_cycle_queued(self, cycle: u64, elapsed: Duration) -> Result<()> {
        match self {
            Self::Throughput => Ok(()),
            Self::WholeCycles { .. } => {
                ensure!(
                    elapsed < CYCLE_LINGER,
                    "queueing keyed cycle {cycle} took {elapsed:?}, so its record batch may have \
                     been sent after {CYCLE_LINGER:?} without the rest of the cycle"
                );
                Ok(())
            }
        }
    }
}

struct BenchmarkRunner {
    args: CommonArgs,
    shape: LoadShape,
    batching: RecordBatching,
    producer: ThreadedProducer<DeliveryContext>,
    deliveries: DeliveryContext,
    wait_timeout: Duration,
}

struct LoadGeneration {
    started: Instant,
    first_cycle: u64,
    next_cycle: u64,
    accepted_messages: u64,
    elapsed: Duration,
    peak_backlog_messages: u64,
}

struct LoadGenerationPlan<'a> {
    input_partitions: &'a [i32],
    output_partitions: &'a [i32],
    baseline: OutputCounts,
    first_cycle: u64,
    minimum_cycles: u64,
    target_duration: Duration,
}

impl LoadGeneration {
    fn cycles(&self) -> u64 {
        self.next_cycle - self.first_cycle
    }
}

struct BenchmarkReport {
    target_duration: Duration,
    warmup_target_duration: Duration,
    warmup_generation_elapsed: Duration,
    warmup_parity_stability_elapsed: Duration,
    generation_elapsed: Duration,
    producer_flush_elapsed: Duration,
    completion_elapsed: Duration,
    end_to_end_elapsed: Duration,
    parity_stability_elapsed: Duration,
    wire_bytes_per_message: usize,
    partitions: usize,
    warmup_messages: u64,
    max_backlog_messages: u64,
    peak_backlog_messages: u64,
    input_messages: u64,
    expected_output_records: u64,
    output_messages: u64,
    output_records: u64,
    output_validation: &'static str,
    output_records_at_generation_end: u64,
    output_records_at_flush: u64,
    backlog_messages_at_generation_end: u64,
    backlog_messages_at_flush: u64,
}

impl BenchmarkReport {
    fn print(&self) {
        let generation_seconds = self.generation_elapsed.as_secs_f64();
        let end_to_end_seconds = self.end_to_end_elapsed.as_secs_f64();
        let input_messages = self.input_messages.approx_into::<f64>();
        let input_rate = input_messages / generation_seconds;
        let end_to_end_rate = input_messages / end_to_end_seconds;
        let input_mib =
            input_messages * self.wire_bytes_per_message.approx_into::<f64>() / (1024.0 * 1024.0);

        println!(
            "target_duration_seconds={:.6}",
            self.target_duration.as_secs_f64()
        );
        println!(
            "warmup_target_seconds={:.6}",
            self.warmup_target_duration.as_secs_f64()
        );
        println!(
            "warmup_generation_seconds={:.6}",
            self.warmup_generation_elapsed.as_secs_f64()
        );
        println!(
            "warmup_parity_stability_seconds={:.6}",
            self.warmup_parity_stability_elapsed.as_secs_f64()
        );
        println!("generation_seconds={generation_seconds:.6}");
        println!(
            "producer_flush_seconds={:.6}",
            self.producer_flush_elapsed.as_secs_f64()
        );
        println!(
            "completion_seconds={:.6}",
            self.completion_elapsed.as_secs_f64()
        );
        println!("end_to_end_seconds={end_to_end_seconds:.6}");
        println!(
            "parity_stability_seconds={:.6}",
            self.parity_stability_elapsed.as_secs_f64()
        );
        println!("wire_bytes_per_message={}", self.wire_bytes_per_message);
        println!("partitions={}", self.partitions);
        println!("warmup_messages={}", self.warmup_messages);
        println!("max_backlog_messages={}", self.max_backlog_messages);
        println!("peak_backlog_messages={}", self.peak_backlog_messages);
        println!("input_messages={}", self.input_messages);
        println!("expected_output_records={}", self.expected_output_records);
        println!("output_messages={}", self.output_messages);
        println!("output_records={}", self.output_records);
        println!("output_validation=\"{}\"", self.output_validation);
        println!(
            "output_records_at_generation_end={}",
            self.output_records_at_generation_end
        );
        println!(
            "backlog_messages_at_generation_end={}",
            self.backlog_messages_at_generation_end
        );
        println!("output_records_at_flush={}", self.output_records_at_flush);
        println!(
            "backlog_messages_at_flush={}",
            self.backlog_messages_at_flush
        );
        println!("input_messages_per_second={input_rate:.3}");
        println!("end_to_end_messages_per_second={end_to_end_rate:.3}");
        println!(
            "input_payload_mib_per_second={:.3}",
            input_mib / generation_seconds
        );
        println!(
            "end_to_end_payload_mib_per_second={:.3}",
            input_mib / end_to_end_seconds
        );
    }
}

impl BenchmarkRunner {
    fn new(args: CommonArgs, shape: LoadShape) -> Result<Self> {
        ensure!(args.duration_seconds > 0, "duration must be positive");
        ensure!(args.warmup_seconds > 0, "warm-up duration must be positive");
        ensure!(args.value_bytes > 0, "value byte count must be positive");
        ensure!(
            args.max_backlog_messages > 0,
            "maximum backlog must be positive"
        );
        ensure!(
            args.value_bytes <= 1024 * 1024,
            "value byte count must not exceed 1 MiB"
        );
        ensure!(
            args.wait_timeout_seconds > 0,
            "wait timeout must be positive"
        );
        ensure!(
            !args.go_file.exists(),
            "go marker already exists at {}",
            args.go_file.display()
        );

        let wire_bytes = PayloadWriter::new(&shape, args.value_bytes)?.wire_bytes();
        let batching = RecordBatching::for_shape(&shape, wire_bytes)?;
        let deliveries = DeliveryContext::default();
        let producer = batching
            .producer_config(&args.bootstrap_servers)
            .create_with_context(deliveries.clone())
            .context("failed to create the benchmark Kafka producer")?;
        let wait_timeout = Duration::from_secs(args.wait_timeout_seconds);

        Ok(Self {
            args,
            shape,
            batching,
            producer,
            deliveries,
            wait_timeout,
        })
    }

    fn run(&self) -> Result<BenchmarkReport> {
        let mut payload = PayloadWriter::new(&self.shape, self.args.value_bytes)?;
        let wire_bytes_per_message = payload.wire_bytes();
        let input_partitions = self.topic_partitions(&self.args.input_topic)?;
        let output_partitions = self.topic_partitions(&self.args.output_topic)?;
        ensure!(
            input_partitions == output_partitions,
            "input and output topics have different partition sets: {input_partitions:?} != \
             {output_partitions:?}"
        );
        wait_for_empty_topic(&self.args.input_topic, KAFKA_QUERY_TIMEOUT, |deadline| {
            self.topic_message_count_until(&self.args.input_topic, &input_partitions, deadline)
        })?;
        wait_for_empty_topic(&self.args.output_topic, KAFKA_QUERY_TIMEOUT, |deadline| {
            self.topic_message_count_until(&self.args.output_topic, &output_partitions, deadline)
        })?;
        let meter = self.start_output_meter(&output_partitions)?;
        let benchmark_result = (|| -> Result<BenchmarkReport> {
            self.wait_for_consumer_group()?;

            let partition_count: u64 = input_partitions.len().arch_into();
            let messages_per_cycle = self.shape.messages_per_cycle();
            let minimum_warmup_messages = partition_count
                .checked_mul(messages_per_cycle)
                .context("minimum warm-up message count overflowed")?;
            ensure!(
                self.args.max_backlog_messages >= minimum_warmup_messages,
                "maximum backlog must admit one warm-up cycle per partition \
                 ({minimum_warmup_messages} messages)"
            );

            let warmup_target_duration = Duration::from_secs(self.args.warmup_seconds);
            let warmup = self.generate_load(
                &mut payload,
                &meter,
                LoadGenerationPlan {
                    input_partitions: &input_partitions,
                    output_partitions: &output_partitions,
                    baseline: OutputCounts::default(),
                    first_cycle: 0,
                    minimum_cycles: partition_count,
                    target_duration: warmup_target_duration,
                },
            )?;
            self.producer
                .flush(self.wait_timeout)
                .context("failed to flush the warm-up Kafka records")?;
            self.wait_for_delivery_total(warmup.accepted_messages)?;
            ensure!(
                self.deliveries.failed() == 0,
                "warm-up Kafka delivery failed: {}",
                self.deliveries
                    .first_error()
                    .unwrap_or_else(|| "unknown delivery error".to_string())
            );
            self.wait_for_topic_count(
                &self.args.input_topic,
                &input_partitions,
                warmup.accepted_messages,
                "warm-up input",
            )?;

            let warmup_records = self
                .shape
                .expected_output_records(warmup.cycles())
                .context("expected warm-up output record count overflowed")?;
            let baseline = self.wait_for_output_records(
                &meter,
                &output_partitions,
                warmup_records,
                "warm-up output",
            )?;
            let warmup_parity_stability_elapsed =
                self.ensure_output_stable(&meter, &output_partitions, baseline, "warm-up output")?;

            fs::write(&self.args.ready_file, b"ready\n").with_context(|| {
                format!(
                    "failed to write ready marker {}",
                    self.args.ready_file.display()
                )
            })?;
            self.wait_for_go()?;

            let succeeded_before = self.deliveries.succeeded();
            let failed_before = self.deliveries.failed();
            let target_duration = Duration::from_secs(self.args.duration_seconds);
            let measured = self.generate_load(
                &mut payload,
                &meter,
                LoadGenerationPlan {
                    input_partitions: &input_partitions,
                    output_partitions: &output_partitions,
                    baseline,
                    first_cycle: warmup.next_cycle,
                    minimum_cycles: 0,
                    target_duration,
                },
            )?;
            let completion_started = Instant::now();
            let expected_output_records = self
                .shape
                .expected_output_records(measured.cycles())
                .context("expected output record count overflowed")?;

            let at_generation_end = self
                .output_counts(
                    &meter,
                    &output_partitions,
                    Instant::now() + self.wait_timeout,
                )?
                .since(baseline)?;
            let backlog_messages_at_generation_end =
                self.backlog_messages(at_generation_end, measured.accepted_messages)?;
            let peak_backlog_messages = measured
                .peak_backlog_messages
                .max(backlog_messages_at_generation_end);

            let flush_started = Instant::now();
            self.producer
                .flush(self.wait_timeout)
                .context("failed to flush benchmark Kafka records")?;
            let producer_flush_elapsed = flush_started.elapsed();
            self.wait_for_delivery_total(
                succeeded_before
                    .checked_add(failed_before)
                    .and_then(|baseline| baseline.checked_add(measured.accepted_messages))
                    .context("Kafka delivery total overflowed")?,
            )?;

            let delivered = self.deliveries.succeeded() - succeeded_before;
            let failed = self.deliveries.failed() - failed_before;
            ensure!(
                failed == 0,
                "{failed} benchmark Kafka deliveries failed: {}",
                self.deliveries
                    .first_error()
                    .unwrap_or_else(|| "unknown delivery error".to_string())
            );
            ensure!(
                delivered == measured.accepted_messages,
                "producer accepted {} records but Kafka acknowledged {delivered}",
                measured.accepted_messages
            );

            let expected_input_total = delivered
                .checked_add(warmup.accepted_messages)
                .context("expected topic count overflowed")?;
            self.wait_for_topic_count(
                &self.args.input_topic,
                &input_partitions,
                expected_input_total,
                "benchmark input",
            )?;

            let at_flush = self
                .output_counts(
                    &meter,
                    &output_partitions,
                    Instant::now() + self.wait_timeout,
                )?
                .since(baseline)?;
            ensure!(
                at_flush.records <= expected_output_records,
                "output topic exceeded the expected record count before the drain wait: {} > {}",
                at_flush.records,
                expected_output_records
            );
            let backlog_messages_at_flush =
                self.backlog_messages(at_flush, measured.accepted_messages)?;
            let drained = self.wait_for_output_records(
                &meter,
                &output_partitions,
                expected_output_records
                    .checked_add(baseline.records)
                    .context("expected output record count overflowed")?,
                "benchmark output",
            )?;
            let completion_elapsed = completion_started.elapsed();
            let end_to_end_elapsed = measured.started.elapsed();
            let parity_stability_elapsed =
                self.ensure_output_stable(&meter, &output_partitions, drained, "benchmark output")?;
            let output = drained.since(baseline)?;
            let output_validation = match &self.shape {
                LoadShape::KeyedWindowed { .. } => "aggregate-count",
                _ => {
                    self.verify_uniform_output(&output_partitions, measured.next_cycle, drained)?;
                    self.ensure_output_stable(
                        &meter,
                        &output_partitions,
                        drained,
                        "verified benchmark output",
                    )?;
                    "ids-and-values"
                }
            };

            Ok(BenchmarkReport {
                target_duration,
                warmup_target_duration,
                warmup_generation_elapsed: warmup.elapsed,
                warmup_parity_stability_elapsed,
                generation_elapsed: measured.elapsed,
                producer_flush_elapsed,
                completion_elapsed,
                end_to_end_elapsed,
                parity_stability_elapsed,
                wire_bytes_per_message,
                partitions: input_partitions.len(),
                warmup_messages: warmup.accepted_messages,
                max_backlog_messages: self.args.max_backlog_messages,
                peak_backlog_messages,
                input_messages: delivered,
                expected_output_records,
                output_messages: output.messages,
                output_records: output.records,
                output_validation,
                output_records_at_generation_end: at_generation_end.records,
                output_records_at_flush: at_flush.records,
                backlog_messages_at_generation_end,
                backlog_messages_at_flush,
            })
        })();
        let diagnostics_result = self.write_output_diagnostics(
            &meter,
            &output_partitions,
            &self.args.output_diagnostics_file,
        );
        match (benchmark_result, diagnostics_result) {
            (Ok(report), Ok(())) => Ok(report),
            (Ok(_), Err(error)) => Err(error.context("failed to retain output diagnostics")),
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(diagnostics_error)) => Err(error.context(format!(
                "output diagnostics also failed: {diagnostics_error:#}"
            ))),
        }
    }

    fn start_output_meter(&self, partitions: &[i32]) -> Result<OutputMeter> {
        match &self.shape {
            LoadShape::UniformPassthrough
            | LoadShape::UniformUppercase
            | LoadShape::UniformFilterMap
            | LoadShape::UniformFanout { .. } => Ok(OutputMeter::Watermarks),
            LoadShape::KeyedWindowed { count_field, .. } => SummaryDrain::start(
                &self.args.bootstrap_servers,
                &self.args.output_topic,
                partitions,
                count_field,
            )
            .map(OutputMeter::Summaries),
        }
    }

    fn verify_uniform_output(
        &self,
        partitions: &[i32],
        cycles: u64,
        observed: OutputCounts,
    ) -> Result<()> {
        let mut audit = UniformRecordAudit::new(&self.shape, cycles, self.args.value_bytes)?;
        let expected = audit.expected_messages()?;
        ensure!(
            observed.messages == expected,
            "uniform output topic has {} messages, expected {expected}",
            observed.messages
        );
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.args.bootstrap_servers)
            .set("group.id", format!("{}_verify", self.args.output_topic))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .context("failed to create the uniform output verifier")?;
        let mut assignment = TopicPartitionList::new();
        for partition in partitions {
            assignment
                .add_partition_offset(&self.args.output_topic, *partition, Offset::Beginning)
                .with_context(|| {
                    format!(
                        "failed to assign output verifier partition {partition} for topic '{}'",
                        self.args.output_topic
                    )
                })?;
        }
        consumer
            .assign(&assignment)
            .context("failed to assign uniform output verifier partitions")?;

        let mut deadline = Instant::now() + self.wait_timeout;
        for received in 0..expected {
            let message = loop {
                if let Some(result) = consumer.poll(SUMMARY_POLL_INTERVAL) {
                    break result.context("failed to read uniform output record")?;
                }
                ensure!(
                    Instant::now() < deadline,
                    "timed out verifying uniform output after {received} of {expected} messages"
                );
            };
            let payload = message
                .payload()
                .context("uniform output record has no payload")?;
            audit.observe(payload)?;
            deadline = Instant::now() + self.wait_timeout;
        }
        audit.finish()
    }

    fn output_counts(
        &self,
        meter: &OutputMeter,
        partitions: &[i32],
        deadline: Instant,
    ) -> Result<OutputCounts> {
        match meter {
            OutputMeter::Watermarks => {
                let messages =
                    self.topic_message_count_until(&self.args.output_topic, partitions, deadline)?;
                Ok(OutputCounts {
                    messages,
                    records: messages,
                })
            }
            OutputMeter::Summaries(drain) => drain.counts(),
        }
    }

    fn generate_load(
        &self,
        payload: &mut PayloadWriter,
        meter: &OutputMeter,
        plan: LoadGenerationPlan<'_>,
    ) -> Result<LoadGeneration> {
        let partition_count: u64 = plan.input_partitions.len().arch_into();
        let messages_per_cycle = self.shape.messages_per_cycle();
        let cycles_per_clock_batch = (SEND_CLOCK_MESSAGES / messages_per_cycle).max(1);
        let started = Instant::now();
        let generation_deadline = started + plan.target_duration;
        let mut cycle = plan.first_cycle;
        let mut accepted_messages = 0_u64;
        let mut peak_backlog_messages = 0_u64;

        // Every warm-up sends at least one complete cycle to every partition before relying on
        // the target duration. Subsequent cycles use the same bounded-pressure loop as the
        // measured phase.
        for _ in 0..plan.minimum_cycles {
            let partition_index: usize = (cycle % partition_count).arch_into();
            self.send_cycle(
                payload,
                cycle,
                plan.input_partitions[partition_index],
                messages_per_cycle,
                Instant::now() + self.wait_timeout,
            )?;
            cycle += 1;
            accepted_messages = accepted_messages
                .checked_add(messages_per_cycle)
                .context("accepted Kafka message count overflowed")?;
        }

        while Instant::now() < generation_deadline {
            let observed =
                match self.output_counts(meter, plan.output_partitions, generation_deadline) {
                    Ok(observed) => observed,
                    Err(_)
                        if generation_deadline.saturating_duration_since(Instant::now())
                            <= GENERATION_QUERY_DEADLINE_TOLERANCE =>
                    {
                        thread::sleep(
                            generation_deadline.saturating_duration_since(Instant::now()),
                        );
                        break;
                    }
                    Err(error) => return Err(error),
                };
            let backlog_messages =
                self.backlog_messages(observed.since(plan.baseline)?, accepted_messages)?;
            peak_backlog_messages = peak_backlog_messages.max(backlog_messages);
            // A backlog past the configured ceiling leaves no room for another cycle.
            let remaining_messages = nervix_primitives::expect_lint!(
                nervix::clamped_arithmetic,
                "backlog beyond its configured ceiling permits zero further messages",
                self.args
                    .max_backlog_messages
                    .saturating_sub(backlog_messages)
            );
            let available_cycles = remaining_messages / messages_per_cycle;
            if available_cycles == 0 {
                let remaining = generation_deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                thread::sleep(OFFSET_POLL_INTERVAL.min(remaining));
                continue;
            }

            // A cycle is indivisible: parity is exact only for whole cycles, so the deadline is
            // observed between cycles and overshoots by at most one.
            for _ in 0..cycles_per_clock_batch.min(available_cycles) {
                let partition_index = (cycle % partition_count).arch_into();
                self.send_cycle(
                    payload,
                    cycle,
                    plan.input_partitions[partition_index],
                    messages_per_cycle,
                    Instant::now() + self.wait_timeout,
                )?;
                cycle += 1;
                accepted_messages = accepted_messages
                    .checked_add(messages_per_cycle)
                    .context("accepted Kafka message count overflowed")?;
                if Instant::now() >= generation_deadline {
                    break;
                }
            }
        }

        Ok(LoadGeneration {
            started,
            first_cycle: plan.first_cycle,
            next_cycle: cycle,
            accepted_messages,
            elapsed: started.elapsed(),
            peak_backlog_messages,
        })
    }

    fn write_output_diagnostics(
        &self,
        meter: &OutputMeter,
        partitions: &[i32],
        path: &Path,
    ) -> Result<()> {
        let diagnostics = match meter {
            OutputMeter::Watermarks => {
                let partition_counts = self.topic_partition_message_counts_until(
                    &self.args.output_topic,
                    partitions,
                    Instant::now() + KAFKA_QUERY_TIMEOUT,
                )?;
                let mut total = OutputCounts::default();
                let partitions = partition_counts
                    .into_iter()
                    .map(|(partition, messages)| {
                        total.messages += messages;
                        total.records += messages;
                        OutputPartitionDiagnostics {
                            partition,
                            messages,
                            records: messages,
                        }
                    })
                    .collect();
                OutputDiagnostics {
                    topic: self.args.output_topic.clone(),
                    total,
                    partitions,
                    recent_summaries: Vec::new(),
                    consumer_failure: None,
                }
            }
            OutputMeter::Summaries(drain) => {
                let observations = drain.state.observations.lock().clone();
                let mut partition_counts = observations.partitions;
                let mut total = OutputCounts::default();
                let partitions = partitions
                    .iter()
                    .map(|partition| {
                        let counts = partition_counts.remove(partition).unwrap_or_default();
                        total.messages += counts.messages;
                        total.records += counts.records;
                        OutputPartitionDiagnostics {
                            partition: *partition,
                            messages: counts.messages,
                            records: counts.records,
                        }
                    })
                    .collect();
                OutputDiagnostics {
                    topic: self.args.output_topic.clone(),
                    total,
                    partitions,
                    recent_summaries: observations.recent.into_iter().collect(),
                    consumer_failure: drain.state.failure.lock().clone(),
                }
            }
        };
        let mut encoded = serde_json::to_vec_pretty(&diagnostics)
            .context("failed to encode output diagnostics")?;
        encoded.push(b'\n');
        fs::write(path, encoded)
            .with_context(|| format!("failed to write output diagnostics {}", path.display()))
    }

    /// Input messages still owed output, derived from the records the measured path has settled.
    fn backlog_messages(&self, settled: OutputCounts, accepted: u64) -> Result<u64> {
        let settled_messages = self
            .shape
            .input_messages_for_output_records(settled.records);
        ensure!(
            settled_messages <= accepted,
            "output topic settled {settled_messages} input messages while only {accepted} were \
             accepted"
        );
        Ok(accepted - settled_messages)
    }

    /// Writes one indivisible cycle to one partition.
    fn send_cycle(
        &self,
        payload: &mut PayloadWriter,
        cycle: u64,
        partition: i32,
        messages_per_cycle: u64,
        deadline: Instant,
    ) -> Result<()> {
        let started = Instant::now();
        for index in 0..messages_per_cycle {
            self.send_before(deadline, partition, payload.payload(cycle, index))?;
        }
        self.batching.check_cycle_queued(cycle, started.elapsed())
    }

    fn send_before(&self, deadline: Instant, partition: i32, payload: &[u8]) -> Result<()> {
        let mut record: BaseRecord<'_, (), [u8], ()> = BaseRecord::to(&self.args.input_topic)
            .partition(partition)
            .payload(payload);
        loop {
            match self.producer.send(record) {
                Ok(()) => return Ok(()),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                    ensure!(
                        Instant::now() < deadline,
                        "timed out enqueueing a Kafka record for partition {partition}"
                    );
                    record = returned;
                    thread::sleep(Duration::from_micros(100));
                }
                Err((error, _)) => return Err(error).context("failed to enqueue Kafka record"),
            }
        }
    }

    fn wait_for_delivery_total(&self, expected: u64) -> Result<()> {
        let deadline = Instant::now() + self.wait_timeout;
        loop {
            let observed = self
                .deliveries
                .succeeded()
                .checked_add(self.deliveries.failed())
                .context("observed Kafka delivery total overflowed")?;
            if observed == expected {
                return Ok(());
            }
            ensure!(
                observed < expected,
                "observed {observed} Kafka deliveries while waiting for {expected}"
            );
            if Instant::now() >= deadline {
                bail!("timed out waiting for {expected} Kafka delivery callbacks; got {observed}");
            }
            thread::sleep(OFFSET_POLL_INTERVAL);
        }
    }

    fn wait_for_consumer_group(&self) -> Result<()> {
        if self.args.minimum_consumers == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + self.wait_timeout;
        let mut stable_since = None;
        loop {
            let groups = match self
                .producer
                .client()
                .fetch_group_list(Some(&self.args.consumer_group), KAFKA_QUERY_TIMEOUT)
            {
                Ok(groups) => groups,
                Err(_) if Instant::now() < deadline => {
                    thread::sleep(OFFSET_POLL_INTERVAL);
                    continue;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to inspect Kafka consumer group '{}' before timeout",
                            self.args.consumer_group
                        )
                    });
                }
            };
            let group = groups
                .groups()
                .iter()
                .find(|group| group.name() == self.args.consumer_group);
            let observed = match group {
                Some(group) => group.members().len(),
                None => 0,
            };
            let is_stable = group.is_some_and(|group| group.state() == "Stable")
                && observed >= self.args.minimum_consumers;
            if is_stable {
                let stable_since = stable_since.get_or_insert_with(Instant::now);
                if stable_since.elapsed() >= PARITY_STABILITY_INTERVAL {
                    return Ok(());
                }
            } else {
                stable_since = None;
            }
            if Instant::now() >= deadline {
                let state = match group {
                    Some(group) => group.state(),
                    None => "missing",
                };
                bail!(
                    "timed out waiting for Kafka consumer group '{}' to stabilize at no fewer \
                     than {} members; observed {observed} in state {state}",
                    self.args.consumer_group,
                    self.args.minimum_consumers,
                );
            }
            thread::sleep(OFFSET_POLL_INTERVAL);
        }
    }

    fn wait_for_go(&self) -> Result<()> {
        let deadline = Instant::now() + self.wait_timeout;
        while !self.args.go_file.exists() {
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for go marker {}",
                    self.args.go_file.display()
                );
            }
            thread::sleep(Duration::from_millis(25));
        }
        Ok(())
    }

    fn wait_for_output_records(
        &self,
        meter: &OutputMeter,
        partitions: &[i32],
        expected: u64,
        label: &str,
    ) -> Result<OutputCounts> {
        let deadline = Instant::now() + self.wait_timeout;
        let mut observed = OutputCounts::default();
        loop {
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for the {label} to reach {expected} records; observed {} \
                     records in {} messages",
                    observed.records,
                    observed.messages
                );
            }
            observed = self.output_counts(meter, partitions, deadline)?;
            if observed.records == expected {
                return Ok(observed);
            }
            ensure!(
                observed.records < expected,
                "{label} exceeded the expected record count: {} > {expected}",
                observed.records
            );
            thread::sleep(OFFSET_POLL_INTERVAL);
        }
    }

    fn ensure_output_stable(
        &self,
        meter: &OutputMeter,
        partitions: &[i32],
        expected: OutputCounts,
        label: &str,
    ) -> Result<Duration> {
        let started = Instant::now();
        let deadline = started + PARITY_STABILITY_INTERVAL;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            thread::sleep(OFFSET_POLL_INTERVAL.min(remaining));
            let observed = self.output_counts(meter, partitions, deadline + KAFKA_QUERY_TIMEOUT)?;
            ensure!(
                observed == expected,
                "{label} changed during the parity stability interval: {} messages / {} records \
                 != {} messages / {} records",
                observed.messages,
                observed.records,
                expected.messages,
                expected.records
            );
        }
        Ok(started.elapsed())
    }

    fn wait_for_topic_count(
        &self,
        topic: &str,
        partitions: &[i32],
        expected: u64,
        label: &str,
    ) -> Result<u64> {
        let deadline = Instant::now() + self.wait_timeout;
        let mut observed = 0;
        loop {
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for {label} topic '{topic}' to reach {expected} records; \
                     observed {observed}"
                );
            }
            observed = self.topic_message_count_until(topic, partitions, deadline)?;
            if observed == expected {
                return Ok(observed);
            }
            ensure!(
                observed < expected,
                "{label} topic '{topic}' exceeded the expected count: {observed} > {expected}"
            );
            thread::sleep(OFFSET_POLL_INTERVAL);
        }
    }

    fn topic_partitions(&self, topic: &str) -> Result<Vec<i32>> {
        let metadata = self
            .producer
            .client()
            .fetch_metadata(Some(topic), KAFKA_QUERY_TIMEOUT)
            .with_context(|| format!("failed to fetch Kafka metadata for topic '{topic}'"))?;
        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|metadata| metadata.name() == topic)
            .with_context(|| format!("Kafka metadata omitted topic '{topic}'"))?;
        ensure!(
            topic_metadata.error().is_none(),
            "Kafka metadata for topic '{topic}' reported {:?}",
            topic_metadata.error()
        );
        let mut partitions = topic_metadata
            .partitions()
            .iter()
            .map(|partition| partition.id())
            .collect::<Vec<_>>();
        partitions.sort_unstable();
        ensure!(
            !partitions.is_empty(),
            "Kafka topic '{topic}' has no partitions"
        );
        Ok(partitions)
    }

    fn topic_message_count_until(
        &self,
        topic: &str,
        partitions: &[i32],
        deadline: Instant,
    ) -> Result<u64> {
        self.topic_partition_message_counts_until(topic, partitions, deadline)?
            .into_iter()
            .try_fold(0_u64, |total, (_, messages)| {
                total
                    .checked_add(messages)
                    .context("Kafka topic message count overflowed")
            })
    }

    fn topic_partition_message_counts_until(
        &self,
        topic: &str,
        partitions: &[i32],
        deadline: Instant,
    ) -> Result<Vec<(i32, u64)>> {
        let request_timeout = || -> Result<Duration> {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !remaining.is_zero(),
                "timed out querying Kafka offsets for topic '{topic}'"
            );
            Ok(remaining.min(KAFKA_QUERY_TIMEOUT))
        };
        let watermark_timeout = request_timeout()?;
        let watermark_results = thread::scope(|scope| {
            let mut queries = Vec::with_capacity(partitions.len());
            for partition_id in partitions.iter().copied() {
                queries.push((
                    partition_id,
                    scope.spawn(move || {
                        self.producer.client().fetch_watermarks(
                            topic,
                            partition_id,
                            watermark_timeout,
                        )
                    }),
                ));
            }
            queries
                .into_iter()
                .map(|(partition, query)| (partition, query.join()))
                .collect::<Vec<_>>()
        });

        let mut counts = Vec::with_capacity(watermark_results.len());
        for (partition, query) in watermark_results {
            let (low, high) = query
                .map_err(|_| {
                    anyhow!(
                        "Kafka watermark query thread panicked for topic '{topic}' partition \
                         {partition}"
                    )
                })?
                .with_context(|| {
                    format!(
                        "failed to fetch Kafka watermarks for topic '{topic}' partition \
                         {partition}"
                    )
                })?;
            ensure!(
                low == 0,
                "Kafka topic '{topic}' partition {partition} has non-zero low watermark {low}"
            );
            let high = u64::try_from(high).with_context(|| {
                format!(
                    "Kafka topic '{topic}' partition {partition} has negative high watermark \
                     {high}"
                )
            })?;
            counts.push((partition, high));
        }
        Ok(counts)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runner = BenchmarkRunner::new(args.common, args.shape.into_shape())?;
    let report = runner.run()?;
    report.print();
    Ok(())
}

/// A newly created partition can publish its metadata before its leader serves offsets.
/// Wait inside one setup budget; no load is accepted until both topics are readable and empty.
fn wait_for_empty_topic(
    topic: &str,
    timeout: Duration,
    mut query: impl FnMut(Instant) -> Result<u64>,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match query(deadline) {
            Ok(messages) => {
                ensure!(messages == 0, "Kafka topic '{topic}' is not empty");
                return Ok(());
            }
            Err(error) => {
                let leader_unavailable = matches!(
                    error.downcast_ref::<KafkaError>(),
                    Some(KafkaError::MetadataFetch(
                        RDKafkaErrorCode::NotLeaderForPartition
                            | RDKafkaErrorCode::LeaderNotAvailable
                    ))
                );
                let remaining = deadline.saturating_duration_since(Instant::now());
                if !leader_unavailable || remaining.is_zero() {
                    return Err(error).context("failed to prepare an empty benchmark topic");
                }
                thread::sleep(OFFSET_POLL_INTERVAL.min(remaining));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_primitives::sync::blocking::mpsc;
    use nervix_recovery::NoReceiver as _;

    use super::*;

    #[test]
    fn empty_topic_setup_waits_for_partition_leaders_inside_one_budget() {
        let mut outcomes = VecDeque::from([
            Err(anyhow!(KafkaError::MetadataFetch(
                RDKafkaErrorCode::NotLeaderForPartition
            ))
            .context("initial watermarks")),
            Err(anyhow!(KafkaError::MetadataFetch(
                RDKafkaErrorCode::LeaderNotAvailable
            ))),
            Ok(0),
        ]);
        let mut first_deadline = None;
        wait_for_empty_topic("input", Duration::from_secs(30), |deadline| {
            assert_eq!(*first_deadline.get_or_insert(deadline), deadline);
            outcomes.pop_front().assured("three queries were prepared")
        })
        .assured("an empty topic whose leader became ready permits setup");
        assert!(outcomes.is_empty());
    }

    #[test]
    fn empty_topic_setup_refuses_records_and_unrelated_failures() {
        let error = wait_for_empty_topic("input", Duration::from_secs(30), |_| Ok(1))
            .err()
            .assured("a nonempty topic cannot start a benchmark");
        assert!(error.to_string().contains("topic 'input' is not empty"));

        let mut queries = 0;
        let error = wait_for_empty_topic("output", Duration::from_secs(30), |_| {
            queries += 1;
            Err(anyhow!(KafkaError::MetadataFetch(
                RDKafkaErrorCode::TopicAuthorizationFailed
            )))
        })
        .err()
        .assured("authorization failure must remain fatal");
        assert_eq!(queries, 1);
        assert!(matches!(
            error.downcast_ref::<KafkaError>(),
            Some(KafkaError::MetadataFetch(
                RDKafkaErrorCode::TopicAuthorizationFailed
            ))
        ));
    }

    #[test]
    fn empty_topic_setup_stops_when_its_budget_is_exhausted() {
        let mut queries = 0;
        let error = wait_for_empty_topic("input", Duration::ZERO, |_| {
            queries += 1;
            Err(anyhow!(KafkaError::MetadataFetch(
                RDKafkaErrorCode::NotLeaderForPartition
            )))
        })
        .err()
        .assured("an unavailable leader cannot extend the setup budget");
        assert_eq!(queries, 1);
        assert!(
            error
                .to_string()
                .contains("failed to prepare an empty benchmark topic")
        );
    }

    #[test]
    fn filter_map_generates_both_input_cases_and_audits_every_output_id() {
        let shape = LoadShape::UniformFilterMap;
        let mut writer = PayloadWriter::new(&shape, 4).assured("test shape and width are valid");
        assert_eq!(
            writer.payload(0, 0),
            br#"{"id":"0000000000000000","value":"xxxx"}"#
        );
        assert_eq!(
            writer.payload(0, 1),
            br#"{"id":"0000000000000000","value":"yyyy"}"#
        );

        let mut audit =
            UniformRecordAudit::new(&shape, 2, 4).assured("two test cycles fit in the verifier");
        assert_eq!(audit.expected_messages().assured("two outputs fit"), 2);
        audit
            .observe(br#"{"id":"0000000000000001","value":"XXXX"}"#)
            .assured("second retained output is valid");
        assert!(audit.finish().is_err(), "the first cycle is still missing");
        audit
            .observe(br#"{"id":"0000000000000000","value":"XXXX"}"#)
            .assured("first retained output is valid");
        audit.finish().assured("every expected output arrived");
        assert!(
            audit
                .observe(br#"{"id":"0000000000000000","value":"XXXX"}"#)
                .is_err(),
            "a duplicate cannot replace a missing record"
        );
        let mut wrong_value =
            UniformRecordAudit::new(&shape, 1, 4).assured("one test cycle fits in the verifier");
        assert!(
            wrong_value
                .observe(br#"{"id":"0000000000000000","value":"xxxx"}"#)
                .is_err(),
            "the map must uppercase the retained value"
        );
    }

    #[test]
    fn fanout_audit_requires_every_copy() {
        let shape = LoadShape::UniformFanout {
            outputs_per_input: 4,
        };
        let mut audit =
            UniformRecordAudit::new(&shape, 1, 1).assured("one test cycle fits in the verifier");
        for _ in 0..4 {
            audit
                .observe(br#"{"id":"0000000000000000","value":"x"}"#)
                .assured("each expected fanout copy is valid");
        }
        audit.finish().assured("all four copies arrived");
        assert!(
            audit
                .observe(br#"{"id":"0000000000000000","value":"x"}"#)
                .is_err(),
            "a fifth copy is invalid"
        );
    }

    #[test]
    fn summary_diagnostics_keep_partition_totals_and_the_most_recent_counts() {
        let state = DrainState::default();
        for offset in 0_i64..40 {
            state.record_summary(SummaryObservation {
                partition: i32::try_from(offset % 2)
                    .assured("the partition remainder is either zero or one"),
                offset,
                record_count: u64::try_from(offset + 1)
                    .assured("the test iterates positive offsets below forty"),
                distinct_estimate: Some(37),
            });
        }

        assert_eq!(state.messages.load(AtomicOrdering::Relaxed), 40);
        assert_eq!(state.records.load(AtomicOrdering::Relaxed), 820);
        let observations = state.observations.lock();
        assert_eq!(
            observations.partitions[&0],
            OutputCounts {
                messages: 20,
                records: 400,
            }
        );
        assert_eq!(
            observations.partitions[&1],
            OutputCounts {
                messages: 20,
                records: 420,
            }
        );
        assert_eq!(observations.recent.len(), RECENT_SUMMARY_LIMIT);
        assert_eq!(observations.recent.front().map(|item| item.offset), Some(8));
        assert_eq!(observations.recent.back().map(|item| item.offset), Some(39));
        assert_eq!(
            observations
                .recent
                .back()
                .and_then(|item| item.distinct_estimate),
            Some(37)
        );
    }

    /// The keyed shape of the `kafka-dedup-window` catalog workload.
    fn dedup_window_shape() -> LoadShape {
        LoadShape::KeyedWindowed {
            keys_per_cycle: 768,
            retained_keys: 576,
            copies_per_key: 2,
            count_field: "record_count".to_string(),
        }
    }

    const BATCH_TOPIC: &str = "keyed-cycles";
    /// The topic of the cycle that brings the producer to the broker before a test's cycles.
    const WARM_UP_TOPIC: &str = "warm-up";

    /// Hands the test the message counts of the record batches a producer formed for
    /// `BATCH_TOPIC`, from the statistics librdkafka emits once per interval for the batches of
    /// that interval.
    struct FormedBatches {
        windows: mpsc::Sender<rdkafka::statistics::Window>,
    }

    impl ClientContext for FormedBatches {
        fn stats(&self, statistics: rdkafka::Statistics) {
            let Some(topic) = statistics.topics.get(BATCH_TOPIC) else {
                return;
            };
            if topic.batchcnt.cnt > 0 {
                self.windows
                    .send(topic.batchcnt.clone())
                    .means_peer_left("the test reading the formed record batches");
            }
        }
    }

    impl ProducerContext for FormedBatches {
        type DeliveryOpaque = ();

        fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
    }

    /// One partition of a mock broker, and a producer configured as the driver configures it for
    /// `shape`, reporting the record batches it forms.
    struct BatchedPartition {
        cluster: rdkafka::mocking::MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
        producer: ThreadedProducer<FormedBatches>,
        windows: mpsc::Receiver<rdkafka::statistics::Window>,
        payload: PayloadWriter,
        messages_per_cycle: u64,
    }

    impl BatchedPartition {
        fn open(shape: &LoadShape) -> Self {
            let cluster = rdkafka::mocking::MockCluster::new(1)
                .assured("librdkafka builds an in-process mock cluster");
            cluster
                .create_topic(BATCH_TOPIC, 1, 1)
                .assured("the mock cluster creates a topic");
            cluster
                .create_topic(WARM_UP_TOPIC, 1, 1)
                .assured("the mock cluster creates a topic");
            let payload = PayloadWriter::new(shape, 128).assured("a 128-byte value fits a payload");
            let batching = RecordBatching::for_shape(shape, payload.wire_bytes())
                .assured("a catalog workload's keyed cycle fits one record batch");
            let mut config = batching.producer_config(&cluster.bootstrap_servers());
            config.set("statistics.interval.ms", "10");
            let (sender, windows) = mpsc::channel();
            let producer = config
                .create_with_context(FormedBatches { windows: sender })
                .assured("the driver's producer configuration is accepted by librdkafka");
            let mut partition = Self {
                payload,
                messages_per_cycle: shape.messages_per_cycle(),
                cluster,
                producer,
                windows,
            };
            // Until the idempotent producer holds its producer id it sends nothing, and the
            // backed-up queue would cut the first batches by size whatever a test does to its
            // cycles. A cycle delivered on another topic shows that it holds one. The cycle is
            // whole because a partial one would wait out the cycle linger: rust-rdkafka calls
            // librdkafka's flush with a zero timeout, which lifts the linger only while it lasts.
            partition.send_cycle(WARM_UP_TOPIC, 0, 0, Duration::ZERO);
            partition
                .producer
                .flush(Duration::from_secs(30))
                .assured("the mock broker acknowledges the warm-up cycle");
            partition
        }

        /// Queues every message of `cycle` for `topic`, stopping for `pause` before message
        /// `pause_at`.
        fn send_cycle(&mut self, topic: &str, cycle: u64, pause_at: u64, pause: Duration) {
            for index in 0..self.messages_per_cycle {
                if index == pause_at {
                    thread::sleep(pause);
                }
                let record = BaseRecord::<'_, (), [u8], ()>::to(topic)
                    .partition(0)
                    .payload(self.payload.payload(cycle, index));
                self.producer
                    .send(record)
                    .map_err(|(error, _)| error)
                    .assured("the producer queue holds every message of a few cycles");
            }
        }

        /// The message counts of the record batches formed for `cycles` whole cycles: one window
        /// for each statistics interval in which the producer formed a batch.
        fn batch_windows(&self, cycles: u64) -> Vec<rdkafka::statistics::Window> {
            self.producer
                .flush(Duration::from_secs(30))
                .assured("the mock broker acknowledges every queued message");
            let messages = cycles
                .checked_mul(self.messages_per_cycle)
                .assured("the tests send a few cycles of fewer than two thousand messages");
            let expected = i64::try_from(messages).assured("a few cycles of messages fit an i64");
            let mut windows = Vec::new();
            let mut counted = 0_i64;
            while counted < expected {
                let Ok(window) = self.windows.recv_timeout(Duration::from_secs(30)) else {
                    panic!(
                        "the producer statistics counted {counted} of {expected} batched messages"
                    );
                };
                counted = counted
                    .checked_add(window.sum)
                    .assured("the windows count the batches of the few cycles the test sent");
                windows.push(window);
            }
            assert_eq!(
                counted, expected,
                "the producer statistics counted more batched messages than the test sent"
            );
            windows
        }

        /// Asserts that every record batch formed for `cycles` cycles held exactly one cycle.
        fn assert_one_cycle_per_batch(&self, cycles: u64) {
            let cycle = i64::try_from(self.messages_per_cycle)
                .assured("a cycle's message count fits an i64");
            for window in self.batch_windows(cycles) {
                assert_eq!(
                    (window.min, window.max),
                    (cycle, cycle),
                    "record batches of {} to {} messages split {cycle}-message cycles",
                    window.min,
                    window.max
                );
            }
        }
    }

    #[test]
    fn keyed_cycles_travel_whole_and_uniform_messages_in_throughput_batches() {
        let keyed = dedup_window_shape();
        let wire_bytes = PayloadWriter::new(&keyed, 128)
            .assured("a 128-byte value fits a payload")
            .wire_bytes();
        assert_eq!(
            RecordBatching::for_shape(&keyed, wire_bytes)
                .assured("the catalog's keyed cycle fits one record batch"),
            RecordBatching::WholeCycles {
                messages_per_cycle: 1536
            }
        );
        for uniform in [
            LoadShape::UniformPassthrough,
            LoadShape::UniformUppercase,
            LoadShape::UniformFilterMap,
            LoadShape::UniformFanout {
                outputs_per_input: 3,
            },
        ] {
            assert_eq!(
                RecordBatching::for_shape(&uniform, wire_bytes)
                    .assured("uniform messages need no particular batching"),
                RecordBatching::Throughput
            );
        }
    }

    #[test]
    fn keyed_batches_wait_for_their_cycle_and_uniform_batches_keep_the_throughput_linger() {
        let throughput = RecordBatching::Throughput.producer_config("127.0.0.1:9092");
        assert_eq!(throughput.get("linger.ms"), Some("5"));
        assert_eq!(throughput.get("batch.num.messages"), Some("10000"));
        let cycles = RecordBatching::WholeCycles {
            messages_per_cycle: 1536,
        }
        .producer_config("127.0.0.1:9092");
        assert_eq!(cycles.get("linger.ms"), Some("50000"));
        assert_eq!(cycles.get("batch.num.messages"), Some("1536"));
        for config in [&throughput, &cycles] {
            assert_eq!(config.get("bootstrap.servers"), Some("127.0.0.1:9092"));
            assert_eq!(config.get("delivery.timeout.ms"), Some("60000"));
            assert_eq!(config.get("message.max.bytes"), Some("1000000"));
        }
    }

    #[test]
    fn refuses_a_keyed_cycle_that_cannot_travel_in_one_record_batch() {
        let keyed = dedup_window_shape();
        let wire_bytes = PayloadWriter::new(&keyed, 1024)
            .assured("a 1024-byte value fits a payload")
            .wire_bytes();
        let error = RecordBatching::for_shape(&keyed, wire_bytes)
            .err()
            .assured("1536 messages of over a kilobyte exceed one record batch");
        assert_eq!(
            error.to_string(),
            "a keyed cycle of 1536 messages of 1065 bytes needs up to 1692160 bytes, more than \
             the 1000000 bytes of the one record batch that keeps every key's copies together"
        );
    }

    #[test]
    fn fails_a_keyed_cycle_queued_for_as_long_as_its_record_batch_waits() {
        let keyed = RecordBatching::WholeCycles {
            messages_per_cycle: 1536,
        };
        let within = CYCLE_LINGER
            .checked_sub(Duration::from_millis(1))
            .assured("the cycle linger is longer than a millisecond");
        keyed
            .check_cycle_queued(7, within)
            .assured("a cycle queued within the linger left in one record batch");
        let error = keyed
            .check_cycle_queued(7, CYCLE_LINGER)
            .err()
            .assured("a cycle queued for the whole linger may have left in two batches");
        assert_eq!(
            error.to_string(),
            "queueing keyed cycle 7 took 50s, so its record batch may have been sent after 50s \
             without the rest of the cycle"
        );
        RecordBatching::Throughput
            .check_cycle_queued(7, Duration::MAX)
            .assured("uniform messages need not travel together");
    }

    #[test]
    fn a_keyed_cycle_leaves_in_one_record_batch_when_its_enqueueing_pauses() {
        let shape = dedup_window_shape();
        let mut partition = BatchedPartition::open(&shape);
        let cycles = 4;
        for cycle in 0..cycles {
            // A driver descheduled in the middle of a cycle, as it is on a loaded host.
            partition.send_cycle(BATCH_TOPIC, cycle, 500, Duration::from_millis(50));
        }
        partition.assert_one_cycle_per_batch(cycles);
    }

    #[test]
    fn a_keyed_cycle_leaves_in_one_record_batch_after_its_broker_returns() {
        let shape = dedup_window_shape();
        let mut partition = BatchedPartition::open(&shape);
        partition
            .cluster
            .broker_down(1)
            .assured("the mock broker can be taken down");
        let cycles = 8;
        for cycle in 0..cycles {
            partition.send_cycle(BATCH_TOPIC, cycle, 0, Duration::ZERO);
        }
        partition
            .cluster
            .broker_up(1)
            .assured("the mock broker can be brought back");
        partition.assert_one_cycle_per_batch(cycles);
    }
}
