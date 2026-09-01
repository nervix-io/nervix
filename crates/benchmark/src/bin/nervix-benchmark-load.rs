use std::{
    fs,
    io::Write as _,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow, bail, ensure};
use clap::{Parser, Subcommand};
use nervix_benchmark::LoadShape;
use parking_lot::Mutex;
use rdkafka::{
    ClientContext, Message, Offset, TopicPartitionList,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
};
use triomphe::Arc;

const SEND_CLOCK_MESSAGES: u64 = 65_536;
const OFFSET_POLL_INTERVAL: Duration = Duration::from_millis(100);
const KAFKA_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const GENERATION_QUERY_DEADLINE_TOLERANCE: Duration = Duration::from_secs(1);
const PARITY_STABILITY_INTERVAL: Duration = Duration::from_millis(500);
const SUMMARY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const KEY_CYCLE_DIGITS: usize = 14;
const KEY_INDEX_DIGITS: usize = 6;
const KEY_DIGITS: usize = KEY_CYCLE_DIGITS + KEY_INDEX_DIGITS;
const RETAIN_MARKER: u8 = b'x';
const PADDING: u8 = b'y';

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
}

/// The load shape the workload declares, with the arguments each shape requires.
#[derive(Debug, Subcommand)]
enum ShapeArgs {
    /// Identical payloads and one output message for every accepted input message.
    UniformPassthrough,
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
        payload: Vec<u8>,
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
            LoadShape::UniformPassthrough => Ok(Self::Uniform {
                payload: format!(r#"{{"value":"{}"}}"#, "x".repeat(value_bytes)).into_bytes(),
            }),
            LoadShape::KeyedWindowed {
                keys_per_cycle,
                retained_keys,
                ..
            } => {
                ensure!(
                    *keys_per_cycle <= 16_u64.pow(KEY_INDEX_DIGITS as u32),
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
            Self::Uniform { payload } => payload.len(),
            Self::Keyed { retained, .. } => retained.len(),
        }
    }

    /// The payload for message `index` of cycle `cycle`.
    ///
    /// Keyed cycles emit one pass over the whole key list per copy, so a key's duplicates are
    /// `keys_per_cycle` messages apart on one partition: far enough to exercise a live keyspace,
    /// close enough that deduplication expiry can never split them.
    fn payload(&mut self, cycle: u64, index: u64) -> &[u8] {
        match self {
            Self::Uniform { payload } => payload,
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
                .expect("the key slot is exactly as wide as the formatted key");
                payload
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

struct DrainState {
    messages: AtomicU64,
    records: AtomicU64,
    stop: AtomicBool,
    failure: Mutex<Option<String>>,
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

        let state = Arc::new(DrainState {
            messages: AtomicU64::new(0),
            records: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            failure: Mutex::new(None),
        });
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
    fn consume(&self, consumer: &BaseConsumer, count_field: &str) {
        while !self.stop.load(AtomicOrdering::Relaxed) {
            let Some(result) = consumer.poll(SUMMARY_POLL_INTERVAL) else {
                continue;
            };
            match result
                .map_err(|error| error.to_string())
                .and_then(|message| {
                    let payload = message
                        .payload()
                        .ok_or_else(|| "output summary has no payload".to_string())?;
                    serde_json::from_slice::<serde_json::Value>(payload)
                        .map_err(|error| format!("output summary is not JSON: {error}"))?
                        .get(count_field)
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| {
                            format!("output summary has no unsigned '{count_field}' field")
                        })
                }) {
                Ok(records) => {
                    self.messages.fetch_add(1, AtomicOrdering::Relaxed);
                    self.records.fetch_add(records, AtomicOrdering::Relaxed);
                }
                Err(failure) => {
                    self.failure.lock().get_or_insert(failure);
                    return;
                }
            }
        }
    }
}

impl Drop for SummaryDrain {
    fn drop(&mut self) {
        self.state.stop.store(true, AtomicOrdering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct BenchmarkRunner {
    args: CommonArgs,
    shape: LoadShape,
    producer: ThreadedProducer<DeliveryContext>,
    deliveries: DeliveryContext,
    wait_timeout: Duration,
}

struct BenchmarkReport {
    target_duration: Duration,
    generation_elapsed: Duration,
    producer_flush_elapsed: Duration,
    drain_elapsed: Duration,
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
    output_records_at_generation_end: u64,
    output_records_at_flush: u64,
    backlog_messages_at_generation_end: u64,
    backlog_messages_at_flush: u64,
}

impl BenchmarkReport {
    fn print(&self) {
        let generation_seconds = self.generation_elapsed.as_secs_f64();
        let end_to_end_seconds = self.end_to_end_elapsed.as_secs_f64();
        let input_rate = self.input_messages as f64 / generation_seconds;
        let end_to_end_rate = self.input_messages as f64 / end_to_end_seconds;
        let output_rate_during_generation =
            self.output_records_at_generation_end as f64 / generation_seconds;
        let input_mib =
            self.input_messages as f64 * self.wire_bytes_per_message as f64 / (1024.0 * 1024.0);

        println!(
            "target_duration_seconds={:.6}",
            self.target_duration.as_secs_f64()
        );
        println!("generation_seconds={generation_seconds:.6}");
        println!(
            "producer_flush_seconds={:.6}",
            self.producer_flush_elapsed.as_secs_f64()
        );
        println!("drain_seconds={:.6}", self.drain_elapsed.as_secs_f64());
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
        println!("output_records_per_second_during_generation={output_rate_during_generation:.3}");
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
        ensure!(
            args.minimum_consumers > 0,
            "minimum consumer count must be positive"
        );
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

        let deliveries = DeliveryContext::default();
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &args.bootstrap_servers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("delivery.timeout.ms", "60000")
            .set("linger.ms", "5")
            .set("batch.size", "1048576")
            .set("batch.num.messages", "10000")
            .set("queue.buffering.max.messages", "1048576")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("compression.type", "none")
            .create_with_context(deliveries.clone())
            .context("failed to create the benchmark Kafka producer")?;
        let wait_timeout = Duration::from_secs(args.wait_timeout_seconds);

        Ok(Self {
            args,
            shape,
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
        ensure!(
            self.topic_message_count(&self.args.input_topic, &input_partitions)? == 0,
            "input topic is not empty"
        );
        ensure!(
            self.topic_message_count(&self.args.output_topic, &output_partitions)? == 0,
            "output topic is not empty"
        );
        let meter = self.start_output_meter(&output_partitions)?;
        self.wait_for_consumer_group()?;

        let partition_count =
            u64::try_from(input_partitions.len()).context("Kafka partition count does not fit")?;
        let messages_per_cycle = self.shape.messages_per_cycle();
        let warmup_messages = partition_count
            .checked_mul(messages_per_cycle)
            .context("warm-up message count overflowed")?;
        let warmup_records = self.shape.expected_output_records(partition_count);
        let warmup_deadline = Instant::now() + self.wait_timeout;

        // Warm-up produces one complete cycle per partition, so every stateful node observes a
        // full cycle and every window it fills is closed before measurement starts.
        let mut cycle = 0_u64;
        for partition in &input_partitions {
            self.send_cycle(
                &mut payload,
                cycle,
                *partition,
                messages_per_cycle,
                warmup_deadline,
            )?;
            cycle += 1;
        }
        self.producer
            .flush(self.wait_timeout)
            .context("failed to flush the warm-up Kafka records")?;
        self.wait_for_delivery_total(warmup_messages)?;
        ensure!(
            self.deliveries.failed() == 0,
            "warm-up Kafka delivery failed: {}",
            self.deliveries
                .first_error()
                .unwrap_or_else(|| "unknown delivery error".to_string())
        );
        ensure!(
            self.topic_message_count(&self.args.input_topic, &input_partitions)? == warmup_messages,
            "warm-up input topic count did not equal one cycle per partition"
        );
        // Warm-up parity means every warm-up record already left a closed window, so this is both
        // the baseline the measured phase subtracts and the proof that no window still holds one.
        let baseline = self.wait_for_output_records(
            &meter,
            &output_partitions,
            warmup_records,
            "warm-up output",
        )?;

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
        let generation_started = Instant::now();
        let generation_deadline = generation_started + target_duration;
        let first_measured_cycle = cycle;
        let mut accepted = 0_u64;
        let mut peak_backlog_messages = 0_u64;
        let cycles_per_clock_batch = (SEND_CLOCK_MESSAGES / messages_per_cycle).max(1);

        while Instant::now() < generation_deadline {
            let observed = match self.output_counts(&meter, &output_partitions, generation_deadline)
            {
                Ok(observed) => observed,
                Err(_)
                    if generation_deadline.saturating_duration_since(Instant::now())
                        <= GENERATION_QUERY_DEADLINE_TOLERANCE =>
                {
                    thread::sleep(generation_deadline.saturating_duration_since(Instant::now()));
                    break;
                }
                Err(error) => return Err(error),
            };
            let backlog_messages = self.backlog_messages(observed.since(baseline)?, accepted)?;
            peak_backlog_messages = peak_backlog_messages.max(backlog_messages);
            let available_cycles = self
                .args
                .max_backlog_messages
                .saturating_sub(backlog_messages)
                / messages_per_cycle;
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
                let partition_index = usize::try_from(cycle % partition_count)
                    .context("Kafka partition index does not fit in usize")?;
                let send_deadline = Instant::now() + self.wait_timeout;
                self.send_cycle(
                    &mut payload,
                    cycle,
                    input_partitions[partition_index],
                    messages_per_cycle,
                    send_deadline,
                )?;
                cycle += 1;
                accepted = accepted
                    .checked_add(messages_per_cycle)
                    .context("accepted Kafka message count overflowed")?;
                if Instant::now() >= generation_deadline {
                    break;
                }
            }
        }
        let generation_elapsed = generation_started.elapsed();
        let measured_cycles = cycle - first_measured_cycle;
        let expected_output_records = self.shape.expected_output_records(measured_cycles);

        let at_generation_end = self
            .output_counts(
                &meter,
                &output_partitions,
                Instant::now() + self.wait_timeout,
            )?
            .since(baseline)?;
        let backlog_messages_at_generation_end =
            self.backlog_messages(at_generation_end, accepted)?;
        peak_backlog_messages = peak_backlog_messages.max(backlog_messages_at_generation_end);

        let flush_started = Instant::now();
        self.producer
            .flush(self.wait_timeout)
            .context("failed to flush benchmark Kafka records")?;
        let producer_flush_elapsed = flush_started.elapsed();
        self.wait_for_delivery_total(
            succeeded_before
                .checked_add(failed_before)
                .and_then(|baseline| baseline.checked_add(accepted))
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
            delivered == accepted,
            "producer accepted {accepted} records but Kafka acknowledged {delivered}"
        );

        let expected_input_total = delivered
            .checked_add(warmup_messages)
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
        let backlog_messages_at_flush = self.backlog_messages(at_flush, accepted)?;
        let drain_started = Instant::now();
        let drained = self.wait_for_output_records(
            &meter,
            &output_partitions,
            expected_output_records
                .checked_add(baseline.records)
                .context("expected output record count overflowed")?,
            "benchmark output",
        )?;
        let drain_elapsed = drain_started.elapsed();
        let end_to_end_elapsed = generation_started.elapsed();
        let parity_stability_elapsed = self.ensure_output_records_stable(
            &meter,
            &output_partitions,
            expected_output_records + baseline.records,
        )?;
        let measured = drained.since(baseline)?;

        Ok(BenchmarkReport {
            target_duration,
            generation_elapsed,
            producer_flush_elapsed,
            drain_elapsed,
            end_to_end_elapsed,
            parity_stability_elapsed,
            wire_bytes_per_message,
            partitions: input_partitions.len(),
            warmup_messages,
            max_backlog_messages: self.args.max_backlog_messages,
            peak_backlog_messages,
            input_messages: delivered,
            expected_output_records,
            output_messages: measured.messages,
            output_records: measured.records,
            output_records_at_generation_end: at_generation_end.records,
            output_records_at_flush: at_flush.records,
            backlog_messages_at_generation_end,
            backlog_messages_at_flush,
        })
    }

    fn start_output_meter(&self, partitions: &[i32]) -> Result<OutputMeter> {
        match &self.shape {
            LoadShape::UniformPassthrough => Ok(OutputMeter::Watermarks),
            LoadShape::KeyedWindowed { count_field, .. } => SummaryDrain::start(
                &self.args.bootstrap_servers,
                &self.args.output_topic,
                partitions,
                count_field,
            )
            .map(OutputMeter::Summaries),
        }
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
        for index in 0..messages_per_cycle {
            self.send_before(deadline, partition, payload.payload(cycle, index))?;
        }
        Ok(())
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
            let observed = group.map_or(0, |group| group.members().len());
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
                bail!(
                    "timed out waiting for Kafka consumer group '{}' to stabilize at no fewer \
                     than {} members; observed {observed} in state {}",
                    self.args.consumer_group,
                    self.args.minimum_consumers,
                    group.map_or("missing", |group| group.state())
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

    fn ensure_output_records_stable(
        &self,
        meter: &OutputMeter,
        partitions: &[i32],
        expected: u64,
    ) -> Result<Duration> {
        let started = Instant::now();
        let deadline = started + PARITY_STABILITY_INTERVAL;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            thread::sleep(OFFSET_POLL_INTERVAL.min(remaining));
            let observed = self.output_counts(meter, partitions, deadline + KAFKA_QUERY_TIMEOUT)?;
            ensure!(
                observed.records == expected,
                "output record count changed during the parity stability interval: {} != \
                 {expected}",
                observed.records
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

    fn topic_message_count(&self, topic: &str, partitions: &[i32]) -> Result<u64> {
        self.topic_message_count_until(topic, partitions, Instant::now() + KAFKA_QUERY_TIMEOUT)
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

        let mut total = 0_u64;
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
            total = total
                .checked_add(high)
                .context("Kafka topic message count overflowed")?;
        }
        Ok(total)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runner = BenchmarkRunner::new(args.common, args.shape.into_shape())?;
    let report = runner.run()?;
    report.print();
    Ok(())
}
