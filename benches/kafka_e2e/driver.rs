use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow, bail, ensure};
use clap::Parser;
use parking_lot::Mutex;
use rdkafka::{
    ClientContext,
    config::ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
};
use triomphe::Arc;

const SEND_CLOCK_BATCH: u64 = 65_536;
const OFFSET_POLL_INTERVAL: Duration = Duration::from_millis(100);
const KAFKA_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const GENERATION_QUERY_DEADLINE_TOLERANCE: Duration = Duration::from_secs(1);
const PARITY_STABILITY_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Parser)]
#[command(about = "Drive the Nervix Kafka-to-Kafka end-to-end benchmark")]
struct Args {
    #[arg(long)]
    bootstrap_servers: String,
    #[arg(long)]
    input_topic: String,
    #[arg(long)]
    output_topic: String,
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

struct BenchmarkRunner {
    args: Args,
    producer: ThreadedProducer<DeliveryContext>,
    deliveries: DeliveryContext,
    payload: Vec<u8>,
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
    output_messages: u64,
    output_messages_at_generation_end: u64,
    output_messages_at_flush: u64,
}

impl BenchmarkReport {
    fn print(&self) {
        let generation_seconds = self.generation_elapsed.as_secs_f64();
        let end_to_end_seconds = self.end_to_end_elapsed.as_secs_f64();
        let input_rate = self.input_messages as f64 / generation_seconds;
        let end_to_end_rate = self.input_messages as f64 / end_to_end_seconds;
        let output_rate_during_generation =
            self.output_messages_at_generation_end as f64 / generation_seconds;
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
        println!("output_messages={}", self.output_messages);
        println!(
            "output_messages_at_generation_end={}",
            self.output_messages_at_generation_end
        );
        println!(
            "backlog_messages_at_generation_end={}",
            self.input_messages
                .saturating_sub(self.output_messages_at_generation_end)
        );
        println!("output_messages_at_flush={}", self.output_messages_at_flush);
        println!(
            "backlog_messages_at_flush={}",
            self.input_messages
                .saturating_sub(self.output_messages_at_flush)
        );
        println!("input_messages_per_second={input_rate:.3}");
        println!("output_messages_per_second_during_generation={output_rate_during_generation:.3}");
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
    fn new(args: Args) -> Result<Self> {
        ensure!(args.duration_seconds > 0, "duration must be positive");
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
            .set("delivery.timeout.ms", "60000")
            .set("linger.ms", "5")
            .set("batch.size", "1048576")
            .set("batch.num.messages", "10000")
            .set("queue.buffering.max.messages", "1048576")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("compression.type", "none")
            .create_with_context(deliveries.clone())
            .context("failed to create the benchmark Kafka producer")?;
        let payload = format!(r#"{{"value":"{}"}}"#, "x".repeat(args.value_bytes)).into_bytes();
        let wait_timeout = Duration::from_secs(args.wait_timeout_seconds);

        Ok(Self {
            args,
            producer,
            deliveries,
            payload,
            wait_timeout,
        })
    }

    fn run(&self) -> Result<BenchmarkReport> {
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
        let warmup_messages = u64::try_from(input_partitions.len())
            .context("Kafka partition count does not fit in u64")?;
        let warmup_deadline = Instant::now() + self.wait_timeout;

        for partition in &input_partitions {
            ensure!(
                self.send_before(warmup_deadline, *partition)?,
                "timed out enqueueing the warm-up Kafka record for partition {partition}"
            );
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
            "warm-up input topic count did not equal the partition count"
        );
        self.wait_for_topic_count(
            &self.args.output_topic,
            &output_partitions,
            warmup_messages,
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
        let mut accepted = 0_u64;
        let mut peak_backlog_messages = 0_u64;

        while Instant::now() < generation_deadline {
            let output_total = match self.topic_message_count_until(
                &self.args.output_topic,
                &output_partitions,
                generation_deadline,
            ) {
                Ok(output_total) => output_total,
                Err(_)
                    if generation_deadline.saturating_duration_since(Instant::now())
                        <= GENERATION_QUERY_DEADLINE_TOLERANCE =>
                {
                    thread::sleep(generation_deadline.saturating_duration_since(Instant::now()));
                    break;
                }
                Err(error) => return Err(error),
            };
            ensure!(
                output_total >= warmup_messages,
                "warm-up output records disappeared during load generation"
            );
            let output_messages = output_total - warmup_messages;
            ensure!(
                output_messages <= accepted,
                "output topic exceeded the accepted benchmark input count during generation"
            );
            let backlog_messages = accepted - output_messages;
            peak_backlog_messages = peak_backlog_messages.max(backlog_messages);
            if backlog_messages >= self.args.max_backlog_messages {
                let remaining = generation_deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                thread::sleep(OFFSET_POLL_INTERVAL.min(remaining));
                continue;
            }

            let available = self.args.max_backlog_messages - backlog_messages;
            for _ in 0..SEND_CLOCK_BATCH.min(available) {
                let partition_index = usize::try_from(accepted % warmup_messages)
                    .context("Kafka partition index does not fit in usize")?;
                if !self.send_before(generation_deadline, input_partitions[partition_index])? {
                    break;
                }
                accepted = accepted
                    .checked_add(1)
                    .context("accepted Kafka message count overflowed")?;
            }
        }
        let generation_elapsed = generation_started.elapsed();
        let output_total_at_generation_end =
            self.topic_message_count(&self.args.output_topic, &output_partitions)?;
        peak_backlog_messages =
            peak_backlog_messages
                .max(accepted.saturating_sub(
                    output_total_at_generation_end.saturating_sub(warmup_messages),
                ));

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

        let expected_total = delivered
            .checked_add(warmup_messages)
            .context("expected topic count overflowed")?;
        self.wait_for_topic_count(
            &self.args.input_topic,
            &input_partitions,
            expected_total,
            "benchmark input",
        )?;

        let output_total_at_flush =
            self.topic_message_count(&self.args.output_topic, &output_partitions)?;
        ensure!(
            output_total_at_flush <= expected_total,
            "output topic exceeded the input count before the drain wait"
        );
        let drain_started = Instant::now();
        let output_total = self.wait_for_topic_count(
            &self.args.output_topic,
            &output_partitions,
            expected_total,
            "benchmark output",
        )?;
        let drain_elapsed = drain_started.elapsed();
        let end_to_end_elapsed = generation_started.elapsed();
        let parity_stability_elapsed = self.ensure_topic_count_stable(
            &self.args.output_topic,
            &output_partitions,
            expected_total,
        )?;

        Ok(BenchmarkReport {
            target_duration,
            generation_elapsed,
            producer_flush_elapsed,
            drain_elapsed,
            end_to_end_elapsed,
            parity_stability_elapsed,
            wire_bytes_per_message: self.payload.len(),
            partitions: input_partitions.len(),
            warmup_messages,
            max_backlog_messages: self.args.max_backlog_messages,
            peak_backlog_messages,
            input_messages: delivered,
            output_messages: output_total - warmup_messages,
            output_messages_at_generation_end: output_total_at_generation_end
                .saturating_sub(warmup_messages),
            output_messages_at_flush: output_total_at_flush.saturating_sub(warmup_messages),
        })
    }

    fn send_before(&self, deadline: Instant, partition: i32) -> Result<bool> {
        if Instant::now() >= deadline {
            return Ok(false);
        }
        let mut record: BaseRecord<'_, (), [u8], ()> = BaseRecord::to(&self.args.input_topic)
            .partition(partition)
            .payload(self.payload.as_slice());
        loop {
            match self.producer.send(record) {
                Ok(()) => return Ok(true),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                    if Instant::now() >= deadline {
                        return Ok(false);
                    }
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

    fn wait_for_topic_count(
        &self,
        topic: &str,
        partitions: &[i32],
        expected: u64,
        label: &str,
    ) -> Result<u64> {
        let deadline = Instant::now() + self.wait_timeout;
        loop {
            if Instant::now() >= deadline {
                bail!("timed out waiting for {label} topic '{topic}' to reach {expected} records");
            }
            let observed = self.topic_message_count_until(topic, partitions, deadline)?;
            if observed == expected {
                return Ok(observed);
            }
            ensure!(
                observed < expected,
                "{label} topic '{topic}' exceeded the expected count: {observed} > {expected}"
            );
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for {label} topic '{topic}' to reach {expected} records; \
                     observed {observed}"
                );
            }
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

    fn ensure_topic_count_stable(
        &self,
        topic: &str,
        partitions: &[i32],
        expected: u64,
    ) -> Result<Duration> {
        let started = Instant::now();
        let deadline = started + PARITY_STABILITY_INTERVAL;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            thread::sleep(OFFSET_POLL_INTERVAL.min(remaining));
            let observed = self.topic_message_count(topic, partitions)?;
            ensure!(
                observed == expected,
                "output topic '{topic}' changed during the parity stability interval: {observed} \
                 != {expected}"
            );
        }
        Ok(started.elapsed())
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
    let runner = BenchmarkRunner::new(Args::parse())?;
    let report = runner.run()?;
    report.print();
    Ok(())
}
