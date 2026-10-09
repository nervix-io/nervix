//! One run of the simulation: connecting, attaching to the domain clock, opening the endpoints,
//! planning and submitting readings tick by tick, and finishing.
//!
//! - **Owns.** The order a run starts and stops its loops in, the planner that steps through the
//!   tick centers the clock reaches and submits each tick's readings, the outcomes it awaits,
//!   reopening changed endpoint contracts, following a new START generation, deliberate replay,
//!   inspection, and the exit status a run's outcomes imply.
//! - **Depends on.** Every other module of the driver, and the Rust client library.
//! - **Must not know.** How the server admits, routes or delivers anything.
//!
//! The consumers start before the producer submits anything: with attached emitters a reading
//! completes only once a consumer acknowledged its output, so a run that waited for its outcomes
//! before consuming would wait on itself.

use std::{collections::BTreeMap, process::ExitCode, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    Client, ClientError, ClientProducerAdmission, ClientProducerGrant, ClientProducerLimits,
    ConnectOptions, DomainName, EmitterName, IngestorName, Producer, ProducerBatch, ProducerEnd,
    ProducerError, ProducerOutcome, SubmissionId, SubmissionUncertainty,
};
use nervix_models::{DomainAdmissionWindow, Timestamp};
use nervix_primitives::{
    sync::{Arc, CancellationToken, Mutex, blocking::Mutex as SyncMutex, watch},
    task::JoinHandle,
    time::Instant,
};
use nervix_recovery::Discarded as _;
use thiserror::Error;

use crate::{
    clock::{self, Clock, ClockError, Pace, Reached, TickGrid},
    consumers::{
        self, ConsumerError, ConsumerLoop, OPEN_RETRY_BUDGET, OPEN_RETRY_DELAY, Output, Shared,
    },
    effects::{EffectError, EffectStore},
    ledger::{Ledger, LedgerError, OutcomeKind},
    options::{OptionsError, Settings, TimestampSource},
    readings::{self, Reading, ReadingSlot, Stamp, reading_fields},
    refusal::Refusal,
    reopen::Reopen,
    report::{self, Report as Counters},
};

/// How long a run waits for the serving node to install a paced clock it attached to.
const INSTALL_BUDGET: Duration = Duration::from_secs(30);

/// How long a run waits before submitting again after its session could not be restored.
const UNAVAILABLE_RETRY: Duration = Duration::from_secs(1);

/// How long a run waits for its consumers to close once it stopped them.
const CLOSE_BUDGET: Duration = Duration::from_secs(30);

/// How a run ended, which the exit status reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finish {
    /// Every reading the run submitted completed. Exit status 0.
    Completed,
    /// A reading ended without completing: not admitted, failed, or of unknown outcome. Replaying
    /// it is the application's decision. Exit status 3.
    Unresolved,
    /// The command line, the graph or the domain is not one the simulation can run against.
    /// Exit status 2.
    Configuration,
    /// The run failed on its own side, or outcomes were still outstanding at its deadline. Exit
    /// status 1.
    Failed,
}

impl Finish {
    pub fn exit_code(self) -> ExitCode {
        match self {
            Self::Completed => ExitCode::SUCCESS,
            Self::Unresolved => ExitCode::from(3),
            Self::Configuration => ExitCode::from(2),
            Self::Failed => ExitCode::from(1),
        }
    }
}

/// Why a producer could not be opened.
#[derive(Debug, Error)]
pub(crate) enum ProducerOpenError {
    #[error("ingestor '{ingestor}' of domain '{domain}' refused the producer: {}", .refusal.as_str())]
    Refused {
        ingestor: IngestorName,
        domain: DomainName,
        refusal: Refusal,
    },
    #[error("cannot open a producer on ingestor '{ingestor}' of domain '{domain}': {reason}")]
    Session {
        ingestor: IngestorName,
        domain: DomainName,
        reason: String,
    },
}

/// Why a run ended before or instead of simulating.
#[derive(Debug, Error)]
enum RunError {
    #[error("{0}")]
    Options(OptionsError),
    #[error("cannot connect to '{server}': {report:#}")]
    Connect {
        server: String,
        report: Report<ClientError>,
    },
    #[error("{}", .0.current_context())]
    Clock(Report<ClockError>),
    #[error("{}", .0.current_context())]
    Consumer(Report<ConsumerError>),
    #[error("{}", .0.current_context())]
    Producer(Report<ProducerOpenError>),
    #[error("{0}")]
    Ledger(#[from] LedgerError),
    #[error("{0}")]
    Effects(#[from] EffectError),
    #[error("cannot submit the readings of tick {tick}: {reason}")]
    Batch { tick: u64, reason: String },
    #[error(
        "{outstanding} submissions still had no outcome {} after the run stopped submitting",
        clock::duration_text(*.deadline)
    )]
    Deadline {
        outstanding: u64,
        deadline: Duration,
    },
}

impl RunError {
    /// How a run that failed this way ends.
    fn finish(&self) -> Finish {
        match self {
            Self::Clock(report)
                if matches!(report.current_context(), ClockError::Arithmetic { .. }) =>
            {
                Finish::Failed
            }
            Self::Options(_)
            | Self::Connect { .. }
            | Self::Clock(_)
            | Self::Consumer(_)
            | Self::Producer(_) => Finish::Configuration,
            Self::Ledger(_) | Self::Effects(_) | Self::Batch { .. } | Self::Deadline { .. } => {
                Finish::Failed
            }
        }
    }
}

/// Runs the simulation the command line describes and reports how it ended.
pub async fn run(options: crate::options::Options) -> Finish {
    let settings = match Settings::try_from(options) {
        Ok(settings) => settings,
        Err(error) => return failed(&RunError::Options(error)),
    };
    match simulate(settings).await {
        Ok(finish) => finish,
        Err(error) => failed(&error),
    }
}

fn failed(error: &RunError) -> Finish {
    report::error(error.to_string());
    error.finish()
}

async fn connect(settings: &Settings) -> Result<Client, RunError> {
    let options = ConnectOptions {
        username: settings.username.clone(),
        password: settings.password.clone(),
        ..ConnectOptions::default()
    };
    let connected =
        Client::connect_with_options(&settings.server, Some(settings.domain.clone()), options)
            .await;
    match connected {
        Ok(client) => Ok(client),
        Err(report) => Err(RunError::Connect {
            server: settings.server.clone(),
            report,
        }),
    }
}

/// Whether the application opens in its current generation or follows an observed START.
#[derive(Clone, Copy)]
enum OpenIntent {
    CurrentGeneration,
    FollowingStart,
}

impl OpenIntent {
    fn retries(self, refusal: Refusal) -> bool {
        refusal == Refusal::EndpointUnavailable
            || matches!(self, Self::FollowingStart) && refusal == Refusal::DomainStopped
    }
}

/// Opens a producer on the ingestor the settings name, asking again while it is not running on its
/// node yet.
async fn open_producer(
    client: &Client,
    settings: &Settings,
    intent: OpenIntent,
) -> error_stack::Result<Producer, ProducerOpenError> {
    let limits = ClientProducerLimits {
        batches: settings.credit_batches,
        bytes: settings.credit_bytes,
    };
    let deadline = Instant::now() + OPEN_RETRY_BUDGET;
    loop {
        nervix_primitives::task::consume_budget().await;
        let opened = client
            .open_ingestor(
                settings.domain.clone(),
                settings.ingestor.clone(),
                reading_fields(),
                limits,
            )
            .await;
        let failure = match opened {
            Ok(producer) => return Ok(producer),
            Err(failure) => failure,
        };
        let refusal = match failure.current_context() {
            ClientError::ProducerRefused { refusal, .. } => Refusal::from(*refusal),
            other => {
                let reason = other.to_string();
                return Err(failure.change_context(ProducerOpenError::Session {
                    ingestor: settings.ingestor.clone(),
                    domain: settings.domain.clone(),
                    reason,
                }));
            }
        };
        if intent.retries(refusal) && Instant::now() < deadline {
            nervix_primitives::time::sleep(OPEN_RETRY_DELAY).await;
            continue;
        }
        return Err(failure.change_context(ProducerOpenError::Refused {
            ingestor: settings.ingestor.clone(),
            domain: settings.domain.clone(),
            refusal,
        }));
    }
}

fn producer_line(settings: &Settings, producer: &Producer) -> String {
    let description = producer.description();
    let grant = description.grant;
    format!(
        "PRODUCER opened ingestor={} generation={} batches={} bytes={} max_batch_rows={} \
         max_batch_bytes={}",
        settings.ingestor,
        description.generation,
        grant.batches,
        grant.bytes,
        grant.max_batch_rows,
        grant.max_batch_bytes
    )
}

/// The generation a run starts in: the attached clock's, once the serving node installed it.
async fn starting_generation(
    clock: &mut Clock,
    domain: &DomainName,
    stop: &CancellationToken,
) -> Result<u64, RunError> {
    let deadline = Instant::now() + INSTALL_BUDGET;
    loop {
        nervix_primitives::task::consume_budget().await;
        let failure = match clock.pace() {
            Pace::Paced { generation } => return Ok(generation),
            Pace::Stopped { generation } => ClockError::Stopped {
                domain: domain.clone(),
                generation,
            },
            Pace::Unpaced { generation } => ClockError::Unpaced {
                domain: domain.clone(),
                generation,
            },
            Pace::Ended => ClockError::Attach {
                domain: domain.clone(),
                message: "the domain was removed".to_string(),
            },
            Pace::Paused { .. } => {
                let waited =
                    nervix_primitives::time::timeout_at(deadline, clock.changed(stop)).await;
                match waited {
                    Ok(true) => continue,
                    Ok(false) | Err(_) => ClockError::Uninstalled {
                        domain: domain.clone(),
                        waited: INSTALL_BUDGET,
                    },
                }
            }
        };
        return Err(RunError::Clock(Report::new(failure)));
    }
}

/// The text of an outcome's cause, as the ledger and the report write it.
pub(crate) fn outcome_of(outcome: &ProducerOutcome) -> (OutcomeKind, &'static str) {
    match outcome {
        ProducerOutcome::Completed => (OutcomeKind::Completed, ""),
        ProducerOutcome::NotAdmitted { refusal, .. } => (OutcomeKind::NotAdmitted, refusal.label()),
        ProducerOutcome::ProcessingFailed { failure, .. } => (
            OutcomeKind::ProcessingFailed,
            <&'static str>::from(*failure),
        ),
        ProducerOutcome::OutcomeUnknown { cause, .. } => {
            let cause = match cause {
                SubmissionUncertainty::Interrupted => "interrupted",
                SubmissionUncertainty::OwnerLost => "owner_lost",
                SubmissionUncertainty::SessionLost => "session_lost",
            };
            (OutcomeKind::OutcomeUnknown, cause)
        }
    }
}

/// The report line of a batch's outcome.
fn outcome_line(tick: u64, readings: u64, kind: OutcomeKind, cause: &str) -> String {
    if cause.is_empty() {
        return format!("OUTCOME tick={tick} readings={readings} {}", kind.as_str());
    }
    format!(
        "OUTCOME tick={tick} readings={readings} {} {cause}",
        kind.as_str()
    )
}

/// The batches a producer holds that have no outcome yet, and their bytes.
#[derive(Debug, Default, Clone, Copy)]
struct Outstanding {
    batches: u64,
    bytes: u64,
}

impl Outstanding {
    fn hold(&mut self, bytes: u64) {
        self.batches = self
            .batches
            .checked_add(1)
            .assured("a producer holds at most its granted batches");
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .assured("a producer holds at most its granted bytes");
    }

    fn release(&mut self, bytes: u64) {
        self.batches = self
            .batches
            .checked_sub(1)
            .assured("a batch is released only after it was held");
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .assured("a batch releases only the bytes it held");
    }

    /// Whether one more batch of `bytes` fits beside these within `grant`.
    fn admits(&self, grant: &ClientProducerGrant, bytes: u64) -> bool {
        let batches_fit = self.batches < u64::from(grant.batches.get());
        let bytes_fit = match self.bytes.checked_add(bytes) {
            Some(total) => total <= grant.bytes.get(),
            None => false,
        };
        batches_fit && bytes_fit
    }
}

/// The producer credit a run's submissions hold, and a signal each time an outcome returns some.
///
/// A run waits for credit before it checks the admission window and submits, so it never checks a
/// batch's event times and then holds the batch back while the window moves on.
struct Credit {
    held: SyncMutex<Outstanding>,
    released: watch::Sender<()>,
}

impl Credit {
    fn new() -> Self {
        let (released, _) = watch::channel(());
        Self {
            held: SyncMutex::new(Outstanding::default()),
            released,
        }
    }

    fn held(&self) -> Outstanding {
        *self.held.lock()
    }

    /// Holds a submitted batch's credit and returns the bytes held now.
    fn hold(&self, bytes: u64) -> u64 {
        let mut held = self.held.lock();
        held.hold(bytes);
        held.bytes
    }

    fn release(&self, bytes: u64) {
        self.held.lock().release(bytes);
        self.released.send_replace(());
    }
}

/// One batch of readings, encoded and waiting to be submitted.
struct Prepared {
    tick: u64,
    readings: Vec<Reading>,
    batch: ProducerBatch,
    bytes: u64,
}

/// One submitted batch whose outcome a task awaits.
struct Submission {
    tick: u64,
    reading_ids: Vec<String>,
    /// The readings of the batch a `TIMESTAMP AT` ingestor rejects, whose notices the run waits
    /// for once the batch completed.
    rejected_ids: Vec<String>,
    bytes: u64,
    id: SubmissionId,
}

/// The planner: it steps through the tick centers of the generation it runs in and submits each
/// tick's readings.
struct Planner {
    settings: Settings,
    client: Client,
    clock: Clock,
    ledger: Arc<Ledger>,
    counters: Arc<Counters>,
    producer: Arc<Producer>,
    credit: Arc<Credit>,
    generation: u64,
    grid: TickGrid,
    /// The generation the consumers follow.
    generations: watch::Sender<u64>,
    outcomes: Vec<JoinHandle<()>>,
    /// The readings of completed batches whose rejection notices the run waits for before it
    /// stops its consumers: an error route acknowledges a reading once its notice is published,
    /// before the rejection emitter delivers it.
    awaited_notices: Arc<SyncMutex<Vec<String>>>,
    effects: Arc<Mutex<EffectStore>>,
    /// Ends the planning, when the application is interrupted or a consumer could not open.
    planning: CancellationToken,
}

impl Planner {
    /// The newest tick center the clock of the current generation reached.
    fn first_tick(&self) -> u64 {
        match self.clock.logical_now(self.generation) {
            Some(now) => self.grid.reached(now),
            None => 0,
        }
    }

    /// Accepts a changed endpoint with a fresh producer and its own fresh credit. Outcome tasks
    /// retain the producer and credit that accepted their submissions; none is resubmitted here.
    async fn reopen(&mut self) -> Result<bool, RunError> {
        let Some(ProducerEnd::ReopenRequired(reason)) = self.producer.end() else {
            return Ok(true);
        };
        let reason = Reopen::from(&reason);
        if reason.waits_for_generation() {
            return Ok(true);
        }
        if let Err(failure) = self.producer.close().await {
            report::line(format!(
                "PRODUCER close_failed reason={}",
                failure.current_context()
            ));
        }
        let producer = open_producer(&self.client, &self.settings, OpenIntent::CurrentGeneration)
            .await
            .map_err(RunError::Producer)?;
        let opened = producer.description().generation;
        if opened != self.generation {
            // START raced the open. The clock and --follow-generations still decide whether
            // this run may plan in that generation.
            drop(producer);
            return self.follow(Pace::Paced { generation: opened }).await;
        }
        report::line(producer_line(&self.settings, &producer));
        self.producer = Arc::new(producer);
        self.credit = Arc::new(Credit::new());
        report::line(format!(
            "REOPENED generation={opened} ingestor={} reason={}",
            self.settings.ingestor,
            reason.text()
        ));
        Ok(true)
    }

    async fn simulate(&mut self) -> Result<(), RunError> {
        let mut tick = self.first_tick();
        let mut planned: u64 = 0;
        while planned < self.settings.ticks {
            nervix_primitives::task::consume_budget().await;
            let generation = self.generation;
            if !self.reopen().await? {
                return Ok(());
            }
            if self.generation != generation {
                tick = self.first_tick();
            }
            let Some(center) = self.grid.center(tick) else {
                return Err(RunError::Clock(Report::new(ClockError::Arithmetic {
                    domain: self.settings.domain.clone(),
                })));
            };
            // Even a very slow domain clock must let the application accept an endpoint change.
            let reached = nervix_primitives::time::timeout(
                OPEN_RETRY_DELAY,
                self.clock.reach(self.generation, center, &self.planning),
            )
            .await;
            let reached = match reached {
                Ok(reached) => reached.map_err(RunError::Clock)?,
                Err(_) => continue,
            };
            let window = match reached {
                Reached::Center(window) => window,
                Reached::Stopping => return Ok(()),
                Reached::Moved(pace) => {
                    if !self.follow(pace).await? {
                        return Ok(());
                    }
                    tick = self.first_tick();
                    continue;
                }
            };
            let generation = self.generation;
            if !self.reopen().await? {
                return Ok(());
            }
            if self.generation != generation {
                tick = self.first_tick();
                continue;
            }
            if self.producer.admission() == ClientProducerAdmission::Suspended {
                nervix_primitives::select! {
                    () = nervix_primitives::time::sleep(OPEN_RETRY_DELAY) => {}
                    () = self.planning.cancelled() => return Ok(()),
                }
                continue;
            }
            let number = planned
                .checked_add(1)
                .assured("a run plans at most --ticks ticks");
            let readings = self.plan(tick, center, &window, number);
            let prepared = self.prepare(tick, readings)?;
            if !self.wait_for_credit(prepared.bytes).await {
                return Ok(());
            }
            // The window moves on while a batch waits for credit, so it is read again now.
            let Some(window) = self.clock.window(self.generation) else {
                continue;
            };
            planned = number;
            let next = tick
                .checked_add(1)
                .assured("a tick center past 2^64 periods is unrepresentable and ended the loop");
            if self.settings.timestamps == TimestampSource::At && !window.contains(center) {
                // Domain time does not wait for a slow application: a tick center the window no
                // longer retains would only be rejected.
                report::line(format!(
                    "SKIPPED tick={tick} reason=behind_admission_window"
                ));
                tick = next;
                continue;
            }
            report::line(format!(
                "SUBMIT tick={tick} occurred_at={} window={}..{}",
                center.to_rfc3339(),
                window.earliest_center().to_rfc3339(),
                window.latest_center().to_rfc3339()
            ));
            self.send(prepared).await?;
            tick = next;
        }
        Ok(())
    }

    /// The readings of one tick: every sensor's burst, stamped with the tick center. The first
    /// reading of every `--invalid-every`th planned tick is stamped before the window instead.
    fn plan(
        &self,
        tick: u64,
        center: Timestamp,
        window: &DomainAdmissionWindow,
        planned: u64,
    ) -> Vec<Reading> {
        let invalid = match self.settings.invalid_every {
            Some(every) => planned.is_multiple_of(every.get()),
            None => false,
        };
        let before_window = if invalid {
            before_the_window(window)
        } else {
            None
        };
        let sensors = self.settings.sensors.get();
        let burst = self.settings.burst.get();
        let mut readings = Vec::new();
        for sensor in 0..sensors {
            for position in 0..burst {
                let slot = ReadingSlot {
                    generation: self.generation,
                    tick,
                    sensor,
                    position,
                };
                let first = sensor == 0 && position == 0;
                let reading = match before_window {
                    Some(stale) if first => Reading::planned(slot, stale, Stamp::BeforeWindow),
                    Some(_) | None => Reading::planned(slot, center, Stamp::Center),
                };
                readings.push(reading);
            }
        }
        readings
    }

    /// Encodes the readings of one tick as the producer's batch.
    fn prepare(&self, tick: u64, readings: Vec<Reading>) -> Result<Prepared, RunError> {
        let record_batch = readings::record_batch(self.producer.arrow_schema(), &readings)
            .map_err(|error| RunError::Batch {
                tick,
                reason: error.to_string(),
            })?;
        let batch = self
            .producer
            .batch(&record_batch)
            .map_err(|failure| RunError::Batch {
                tick,
                reason: failure.current_context().to_string(),
            })?;
        let bytes = u64::try_from(batch.len()).assured("a batch's length fits u64");
        let grant = self.producer.description().grant;
        if bytes > grant.max_batch_bytes.get() {
            return Err(RunError::Batch {
                tick,
                reason: format!(
                    "its {bytes} bytes exceed the {} bytes one submission may carry",
                    grant.max_batch_bytes
                ),
            });
        }
        Ok(Prepared {
            tick,
            readings,
            batch,
            bytes,
        })
    }

    /// Waits until a batch of `bytes` fits in the producer's credit beside the batches it holds.
    /// Returns `false` when the planning stops first.
    async fn wait_for_credit(&self, bytes: u64) -> bool {
        let grant = self.producer.description().grant;
        let mut released = self.credit.released.subscribe();
        let mut reported = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            released.borrow_and_update();
            if self.producer.end().is_some() {
                // This already-planned batch reaches send's definitely-unsent ledger path.
                return true;
            }
            let held = self.credit.held();
            if held.admits(&grant, bytes) {
                return true;
            }
            if !reported {
                self.counters.credit_wait();
                report::line(format!(
                    "WAITING credit outstanding_batches={} outstanding_bytes={}",
                    held.batches, held.bytes
                ));
                reported = true;
            }
            nervix_primitives::select! {
                changed = released.changed() => {
                    if changed.is_err() {
                        return true;
                    }
                }
                () = self.planning.cancelled() => return false,
                () = nervix_primitives::time::sleep(OPEN_RETRY_DELAY) => {}
            }
        }
    }

    /// Records the readings in the ledger, submits them as one batch, and starts awaiting its
    /// outcome.
    async fn send(&mut self, prepared: Prepared) -> Result<(), RunError> {
        let Prepared {
            tick,
            readings,
            batch,
            bytes,
        } = prepared;
        let count = u64::try_from(readings.len()).assured("a batch's row count fits u64");
        let mut reading_ids = Vec::with_capacity(readings.len());
        let mut rejected_ids = Vec::new();
        for reading in &readings {
            reading_ids.push(reading.reading_id.clone());
            if self.settings.timestamps == TimestampSource::At
                && reading.stamp == Stamp::BeforeWindow
            {
                rejected_ids.push(reading.reading_id.clone());
            }
        }
        self.ledger
            .readings(
                &readings,
                self.settings.ingestor.as_str(),
                self.settings.timestamps,
            )
            .await?;
        self.counters.submitted(count);
        let id = loop {
            nervix_primitives::task::consume_budget().await;
            let submitted = nervix_primitives::select! {
                submitted = self.producer.submit(batch.clone()) => submitted,
                // A submission that still waits for credit has sent nothing.
                () = self.planning.cancelled() => {
                    return self.not_sent(tick, reading_ids, count, "not_sent").await;
                }
            };
            let failure = match submitted {
                Ok(id) => break id,
                Err(failure) => failure,
            };
            match failure.current_context() {
                ProducerError::SessionUnavailable => {
                    report::line("PRODUCER unavailable retry_after=1s");
                    nervix_primitives::select! {
                        () = nervix_primitives::time::sleep(UNAVAILABLE_RETRY) => {}
                        () = self.planning.cancelled() => {}
                    }
                }
                ProducerError::Ended(ProducerEnd::ReopenRequired(reason))
                    if !Reopen::from(reason).waits_for_generation() =>
                {
                    // The contract ended after this tick was planned. Keep the definitely
                    // unsent readings for an explicit --replay; the next tick opens afresh.
                    return self.not_sent(tick, reading_ids, count, "not_sent").await;
                }
                ProducerError::Ended(_) => {
                    return self
                        .not_sent(tick, reading_ids, count, "producer_ended")
                        .await;
                }
                other => {
                    return Err(RunError::Batch {
                        tick,
                        reason: other.to_string(),
                    });
                }
            }
        };
        let held = self.credit.hold(bytes);
        self.counters.outstanding(held);
        report::line(format!(
            "SUBMITTED tick={tick} readings={count} bytes={bytes}"
        ));
        let submission = Submission {
            tick,
            reading_ids,
            rejected_ids,
            bytes,
            id,
        };
        let task = nervix_primitives::task::spawn(await_outcome(
            submission,
            Awaiting {
                producer: self.producer.clone(),
                ledger: self.ledger.clone(),
                counters: self.counters.clone(),
                credit: self.credit.clone(),
                awaited_notices: self.awaited_notices.clone(),
            },
        ));
        self.outcomes.push(task);
        Ok(())
    }

    /// Records the outcome of readings that never left the client: no row of them was admitted.
    async fn not_sent(
        &self,
        tick: u64,
        reading_ids: Vec<String>,
        count: u64,
        cause: &str,
    ) -> Result<(), RunError> {
        self.ledger
            .outcome(reading_ids, OutcomeKind::NotAdmitted, cause)
            .await?;
        self.counters.outcome(OutcomeKind::NotAdmitted, count);
        report::line(outcome_line(tick, count, OutcomeKind::NotAdmitted, cause));
        Ok(())
    }

    /// Follows the domain into a later START generation, when the run is asked to. Returns
    /// whether the simulation continues.
    async fn follow(&mut self, pace: Pace) -> Result<bool, RunError> {
        if !self.settings.follow_generations {
            report::line(format!(
                "STOPPING generation={} reason=generation_ended",
                self.generation
            ));
            return Ok(false);
        }
        let mut current = pace;
        let target = loop {
            nervix_primitives::task::consume_budget().await;
            match current {
                Pace::Paced { generation } if generation > self.generation => break generation,
                Pace::Ended | Pace::Unpaced { .. } => {
                    report::line(format!(
                        "STOPPING generation={} reason=domain_unavailable",
                        self.generation
                    ));
                    return Ok(false);
                }
                Pace::Paced { .. } | Pace::Paused { .. } | Pace::Stopped { .. } => {
                    if !self.clock.changed(&self.planning).await {
                        return Ok(false);
                    }
                    current = self.clock.pace();
                }
            }
        };
        if let Err(failure) = self.producer.close().await {
            report::line(format!(
                "PRODUCER close_failed reason={}",
                failure.current_context()
            ));
        }
        let producer = open_producer(&self.client, &self.settings, OpenIntent::FollowingStart)
            .await
            .map_err(RunError::Producer)?;
        let opened = producer.description().generation;
        // The producer opens under the generation the domain runs now, and the clock must have
        // been observed in that same generation before anything is planned against it.
        let deadline = Instant::now() + INSTALL_BUDGET;
        let paced = loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(paced) = self.clock.paced(opened) {
                break paced;
            }
            let waited =
                nervix_primitives::time::timeout_at(deadline, self.clock.changed(&self.planning))
                    .await;
            if !matches!(waited, Ok(true)) {
                report::line(format!(
                    "STOPPING generation={opened} reason=clock_not_observed"
                ));
                return Ok(false);
            }
        };
        report::line(producer_line(&self.settings, &producer));
        self.producer = Arc::new(producer);
        self.credit = Arc::new(Credit::new());
        self.generation = opened;
        self.grid = TickGrid::of(opened, &paced);
        self.generations.send_replace(opened);
        self.ledger.generation(opened).await?;
        self.counters.generation(opened);
        report::line(format!(
            "REOPENED generation={opened} ingestor={} after={target}",
            self.settings.ingestor
        ));
        Ok(true)
    }

    /// Resubmits the readings the ledger holds without a completed outcome, when they belong to
    /// the generation the run is in and the window still admits them. A reading of another
    /// generation is never submitted against this one.
    async fn replay(&mut self, unresolved: Vec<Reading>) -> Result<(), RunError> {
        let generation = self.generation;
        let mut by_tick: BTreeMap<u64, Vec<Reading>> = BTreeMap::new();
        for reading in unresolved {
            if reading.generation != self.generation {
                report::line(format!(
                    "REPLAY skipped reading_id={} generation={} current={}",
                    reading.reading_id, reading.generation, self.generation
                ));
                continue;
            }
            by_tick.entry(reading.tick).or_default().push(reading);
        }
        for (tick, readings) in by_tick {
            nervix_primitives::task::consume_budget().await;
            if !self.reopen().await? || self.generation != generation {
                return Ok(());
            }
            let admissible = self.admissible(readings);
            if admissible.is_empty() {
                continue;
            }
            let mut prepared = self.prepare(tick, admissible)?;
            if !self.wait_for_credit(prepared.bytes).await {
                return Ok(());
            }
            // The window moves on while a batch waits for credit, so it is checked again now.
            let count = prepared.readings.len();
            let admissible = self.admissible(prepared.readings);
            if admissible.is_empty() {
                continue;
            }
            if admissible.len() == count {
                prepared.readings = admissible;
            } else {
                prepared = self.prepare(tick, admissible)?;
            }
            report::line(format!(
                "REPLAY tick={tick} readings={}",
                prepared.readings.len()
            ));
            self.send(prepared).await?;
        }
        Ok(())
    }

    /// The readings a replay may still submit: under `TIMESTAMP AT`, those whose event time the
    /// window admits now. The others are reported, and stay unresolved in the ledger.
    fn admissible(&self, readings: Vec<Reading>) -> Vec<Reading> {
        let window = self.clock.window(self.generation);
        let mut admissible = Vec::with_capacity(readings.len());
        for reading in readings {
            let admitted = match (&window, self.settings.timestamps) {
                (_, TimestampSource::Now) => true,
                (Some(window), TimestampSource::At) => window.contains(reading.occurred_at),
                (None, TimestampSource::At) => false,
            };
            if admitted {
                admissible.push(reading);
                continue;
            }
            report::line(format!(
                "REPLAY expired reading_id={} occurred_at={}",
                reading.reading_id,
                reading.occurred_at.to_rfc3339()
            ));
        }
        admissible
    }

    /// Waits for every outcome still outstanding, and then for the rejection notices of the
    /// completed batches, up to `deadline`. Returns how many outcomes never came.
    async fn settle(&mut self, deadline: Duration) -> u64 {
        let until = Instant::now() + deadline;
        let mut missing: u64 = 0;
        for task in std::mem::take(&mut self.outcomes) {
            nervix_primitives::task::consume_budget().await;
            if nervix_primitives::time::timeout_at(until, task)
                .await
                .is_err()
            {
                missing = missing
                    .checked_add(1)
                    .assured("a run submits far fewer than 2^64 batches");
            }
        }
        let awaited = std::mem::take(&mut *self.awaited_notices.lock());
        self.await_notices(awaited, until).await;
        missing
    }

    /// Keeps the consumers running until the effect store holds the rejection notice of every
    /// reading in `awaited`, or `until` passes. A notice still missing then reaches a later run.
    async fn await_notices(&self, awaited: Vec<String>, until: Instant) {
        let mut notices = self.effects.lock().await.notices();
        let mut reported = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            notices.borrow_and_update();
            let mut outstanding: u64 = 0;
            {
                let store = self.effects.lock().await;
                for reading_id in &awaited {
                    if !store.has_rejection(reading_id) {
                        outstanding = outstanding
                            .checked_add(1)
                            .assured("a run stamps far fewer than 2^64 readings");
                    }
                }
            }
            if outstanding == 0 {
                return;
            }
            if !reported {
                report::line(format!("WAITING notices outstanding={outstanding}"));
                reported = true;
            }
            let changed = nervix_primitives::time::timeout_at(until, notices.changed()).await;
            if !matches!(changed, Ok(Ok(()))) {
                report::line(format!("NOTICES missing={outstanding}"));
                return;
            }
        }
    }
}

/// The stamp of a reading deliberately made too old: one nanosecond further than the skew before
/// the oldest tick center the window retains.
fn before_the_window(window: &DomainAdmissionWindow) -> Option<Timestamp> {
    let distance = window
        .skew()
        .as_duration()
        .checked_add(Duration::from_nanos(1))?;
    window.earliest_center().checked_sub(distance).ok()
}

/// What an outcome task records the outcome of its submission into.
struct Awaiting {
    producer: Arc<Producer>,
    ledger: Arc<Ledger>,
    counters: Arc<Counters>,
    credit: Arc<Credit>,
    awaited_notices: Arc<SyncMutex<Vec<String>>>,
}

/// Awaits one submission's terminal outcome, records it in the ledger and reports it.
async fn await_outcome(submission: Submission, awaiting: Awaiting) {
    let Submission {
        tick,
        reading_ids,
        rejected_ids,
        bytes,
        id,
    } = submission;
    let outcome = awaiting
        .producer
        .rejoin(id)
        .await
        .assured("only this task takes the outcome of its submission");
    awaiting.credit.release(bytes);
    let (kind, cause) = outcome_of(&outcome);
    if kind == OutcomeKind::Completed {
        awaiting.awaited_notices.lock().extend(rejected_ids);
    }
    let count = u64::try_from(reading_ids.len()).assured("a batch's row count fits u64");
    if let Err(error) = awaiting.ledger.outcome(reading_ids, kind, cause).await {
        report::error(error.to_string());
    }
    awaiting.counters.outcome(kind, count);
    report::line(outcome_line(tick, count, kind, cause));
}

/// Inspects the ingestor and the output emitter on the session, at an interval, while the run
/// produces and consumes on the same session.
struct Inspector {
    client: Client,
    ingestor: IngestorName,
    emitter: EmitterName,
    counters: Arc<Counters>,
    every: Duration,
    stop: CancellationToken,
}

impl Inspector {
    async fn run(self) {
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                () = nervix_primitives::time::sleep(self.every) => {}
                () = self.stop.cancelled() => return,
            }
            let listed = nervix_primitives::select! {
                listed = self.client.execute("SHOW INGESTORS;") => listed,
                () = self.stop.cancelled() => return,
            };
            let listing = match listed {
                Ok(outcome) => outcome.message,
                Err(error) => {
                    report::line(format!("INSPECT failed reason={error}"));
                    continue;
                }
            };
            let prefix = format!("{} ", self.ingestor);
            for line in listing.lines() {
                if line.starts_with(&prefix) {
                    report::line(format!("INSPECT {line}"));
                }
            }
            let described = nervix_primitives::select! {
                described = self.client.execute(format!("DESCRIBE EMITTER {};", self.emitter)) => described,
                () = self.stop.cancelled() => return,
            };
            let description = match described {
                Ok(outcome) => outcome.message,
                Err(error) => {
                    report::line(format!("INSPECT failed reason={error}"));
                    continue;
                }
            };
            let consumers = described_value(&description, "consumers: ");
            let retained = described_value(&description, "retained batches: ");
            report::line(format!(
                "INSPECT emitter={} consumers={consumers} retained_batches={retained}",
                self.emitter
            ));
            self.counters.inspection();
        }
    }
}

/// The value of the description line that starts with `label`, or `-` when there is none.
fn described_value<'text>(description: &'text str, label: &str) -> &'text str {
    for line in description.lines() {
        if let Some(value) = line.trim().strip_prefix(label) {
            return value;
        }
    }
    "-"
}

/// The output consumers of a run, opened at once or after the consumer delay.
struct OutputConsumers {
    tasks: Vec<JoinHandle<()>>,
}

impl OutputConsumers {
    fn loops(settings: &Settings, shared: &Shared) -> Vec<ConsumerLoop> {
        let count = settings.consumers.get();
        let mut loops = Vec::new();
        for index in 1..=count {
            let leave_after = if index == count {
                settings.consumer_leave_after
            } else {
                None
            };
            loops.push(ConsumerLoop {
                name: format!("output-{index}"),
                emitter: settings.emitter.clone(),
                output: Output::Readings,
                processing_time: settings.processing_time,
                leave_after,
                shared: shared.clone(),
            });
        }
        loops
    }

    /// Opens every output consumer now, failing the run when one is refused.
    async fn open_now(settings: &Settings, shared: &Shared) -> Result<Self, RunError> {
        let mut tasks = Vec::new();
        for consumer_loop in Self::loops(settings, shared) {
            let consumer = consumers::open(
                &shared.client,
                &shared.domain,
                &consumer_loop.emitter,
                Output::Readings,
            )
            .await
            .map_err(RunError::Consumer)?;
            report::line(consumers::opened_line(
                &consumer_loop.name,
                &consumer_loop.emitter,
                &consumer,
            ));
            shared.counters.consumer_joined();
            tasks.push(nervix_primitives::task::spawn(consumer_loop.run(consumer)));
        }
        Ok(Self { tasks })
    }

    /// Opens every output consumer after `delay`, while the run already submits. A refusal ends
    /// the planning and is kept for the run to report.
    fn open_later(settings: &Settings, shared: &Shared, delay: Duration) -> Self {
        let loops = Self::loops(settings, shared);
        let shared = shared.clone();
        let starter = nervix_primitives::task::spawn(async move {
            nervix_primitives::select! {
                () = nervix_primitives::time::sleep(delay) => {}
                () = shared.stop.cancelled() => return,
            }
            let mut tasks = Vec::new();
            for consumer_loop in loops {
                let opened = consumers::open(
                    &shared.client,
                    &shared.domain,
                    &consumer_loop.emitter,
                    Output::Readings,
                )
                .await;
                let consumer = match opened {
                    Ok(consumer) => consumer,
                    Err(failure) => {
                        shared.fail(failure);
                        break;
                    }
                };
                report::line(consumers::opened_line(
                    &consumer_loop.name,
                    &consumer_loop.emitter,
                    &consumer,
                ));
                shared.counters.consumer_joined();
                tasks.push(nervix_primitives::task::spawn(consumer_loop.run(consumer)));
            }
            for task in tasks {
                nervix_primitives::task::consume_budget().await;
                task.await
                    .discarded("a consumer loop that panicked has already reported its panic");
            }
        });
        Self {
            tasks: vec![starter],
        }
    }
}

/// Runs the simulation, from connecting to the summary.
async fn simulate(settings: Settings) -> Result<Finish, RunError> {
    let client = connect(&settings).await?;
    report::line(format!(
        "CONNECTED server={} domain={}",
        settings.server, settings.domain
    ));
    let attached = clock::attach(&client, &settings.domain)
        .await
        .map_err(RunError::Clock)?;
    report::line(clock::describe(&attached));
    let counters = Arc::new(Counters::default());
    // Ends the consumers, the clock follower and the inspector once the run finished.
    let stop = CancellationToken::new();
    // Ends the planning: on an interrupt, or when a late consumer could not open.
    let planning = CancellationToken::new();
    let (mut clock, follower) = Clock::follow(
        client.clone(),
        settings.domain.clone(),
        &attached,
        counters.clone(),
        stop.clone(),
    );
    let started = starting_generation(&mut clock, &settings.domain, &stop).await;
    let generation = match started {
        Ok(generation) => generation,
        Err(error) => {
            stop.cancel();
            follower
                .await
                .discarded("the clock follower ends with the run");
            return Err(error);
        }
    };
    let effects = Arc::new(Mutex::new(EffectStore::open(&settings.effects).await?));
    let unresolved = if settings.replay {
        Ledger::unresolved(&settings.ledger).await?
    } else {
        Vec::new()
    };
    let ledger = Arc::new(Ledger::open(&settings.ledger).await?);
    let (generations, following) = watch::channel(generation);
    let (refused, _) = watch::channel(None);
    let shared = Shared {
        client: client.clone(),
        domain: settings.domain.clone(),
        effects,
        counters: counters.clone(),
        generations: following,
        refused,
        planning: planning.clone(),
        stop: stop.clone(),
    };

    // Every consumer starts before the producer submits anything.
    let mut consumer_tasks = Vec::new();
    let rejections = consumers::open(
        &client,
        &settings.domain,
        &settings.rejections,
        Output::Rejections,
    )
    .await
    .map_err(RunError::Consumer)?;
    report::line(consumers::opened_line(
        "rejections",
        &settings.rejections,
        &rejections,
    ));
    let rejection_loop = ConsumerLoop {
        name: "rejections".to_string(),
        emitter: settings.rejections.clone(),
        output: Output::Rejections,
        processing_time: Duration::ZERO,
        leave_after: None,
        shared: shared.clone(),
    };
    consumer_tasks.push(nervix_primitives::task::spawn(
        rejection_loop.run(rejections),
    ));
    let outputs = if settings.consumer_delay.is_zero() {
        OutputConsumers::open_now(&settings, &shared).await?
    } else {
        report::line(format!(
            "CONSUMER delayed emitter={} for={}",
            settings.emitter,
            clock::duration_text(settings.consumer_delay)
        ));
        OutputConsumers::open_later(&settings, &shared, settings.consumer_delay)
    };
    consumer_tasks.extend(outputs.tasks);

    let producer = open_producer(&client, &settings, OpenIntent::CurrentGeneration)
        .await
        .map_err(RunError::Producer)?;
    report::line(producer_line(&settings, &producer));
    let Some(paced) = clock.paced(generation) else {
        return Err(RunError::Clock(Report::new(ClockError::Changed {
            domain: settings.domain.clone(),
        })));
    };
    let inspector = settings.inspect_every.map(|every| {
        nervix_primitives::task::spawn(
            Inspector {
                client: client.clone(),
                ingestor: settings.ingestor.clone(),
                emitter: settings.emitter.clone(),
                counters: counters.clone(),
                every,
                stop: stop.clone(),
            }
            .run(),
        )
    });
    let interrupt = {
        let planning = planning.clone();
        nervix_primitives::task::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                report::line("STOPPING reason=interrupt");
                planning.cancel();
            }
        })
    };

    let mut planner = Planner {
        settings,
        client: client.clone(),
        clock,
        ledger,
        counters: counters.clone(),
        producer: Arc::new(producer),
        credit: Arc::new(Credit::new()),
        generation,
        grid: TickGrid::of(generation, &paced),
        generations,
        outcomes: Vec::new(),
        awaited_notices: Arc::new(SyncMutex::new(Vec::new())),
        effects: shared.effects.clone(),
        planning,
    };
    // The opened producer may already belong to a later generation than the clock the run
    // attached to; the planner then follows the clock into it before planning anything.
    let opened = planner.producer.description().generation;
    let mut continuing = true;
    if opened != generation {
        continuing = planner.follow(Pace::Paced { generation: opened }).await?;
    }
    planner.ledger.generation(planner.generation).await?;
    counters.generation(planner.generation);
    report::line(format!("READY generation={}", planner.generation));
    if continuing {
        planner.replay(unresolved).await?;
        planner.simulate().await?;
    }
    let missing = nervix_primitives::select! {
        biased;
        error = shared.failure() => {
            // A refused output cannot finish the held batches, even if planning already ended.
            stop.cancel();
            interrupt.abort();
            return Err(RunError::Consumer(error));
        }
        missing = async {
            let missing = planner.settle(planner.settings.deadline).await;
            if let Err(failure) = planner.producer.close().await {
                report::line(format!(
                    "PRODUCER close_failed reason={}",
                    failure.current_context()
                ));
            }
            missing
        } => missing,
    };
    interrupt.abort();
    stop.cancel();
    for task in consumer_tasks {
        nervix_primitives::task::consume_budget().await;
        if nervix_primitives::time::timeout(CLOSE_BUDGET, task)
            .await
            .is_err()
        {
            report::line("CONSUMER close_timed_out");
        }
    }
    follower
        .await
        .discarded("the clock follower ends with the run");
    if let Some(inspector) = inspector {
        inspector.await.discarded("the inspector ends with the run");
    }
    let detached = client
        .detach_domain_clock(planner.settings.domain.clone())
        .await;
    if let Err(failure) = detached {
        report::line(format!(
            "CLOCK detach_failed reason={}",
            failure.current_context()
        ));
    }
    report::line(counters.summary());
    let refusal = shared.refused.send_replace(None);
    if let Some(error) = refusal {
        return Err(RunError::Consumer(error));
    }
    if missing > 0 {
        return Err(RunError::Deadline {
            outstanding: missing,
            deadline: planner.settings.deadline,
        });
    }
    if counters.all_completed() {
        return Ok(Finish::Completed);
    }
    Ok(Finish::Unresolved)
}

#[cfg(test)]
mod tests {
    use nervix_client_core::ClientSubmissionRefusal;
    use nervix_models::{ClientProcessingFailure, DomainClockPeriod, DomainClockSkew};

    use super::*;

    #[test]
    fn only_an_open_following_start_retries_a_stopped_domain() {
        assert!(OpenIntent::FollowingStart.retries(Refusal::DomainStopped));
        assert!(!OpenIntent::CurrentGeneration.retries(Refusal::DomainStopped));
        for intent in [OpenIntent::CurrentGeneration, OpenIntent::FollowingStart] {
            assert!(intent.retries(Refusal::EndpointUnavailable));
            assert!(!intent.retries(Refusal::SchemaMismatch));
            assert!(!intent.retries(Refusal::DomainNotFound));
        }
    }

    #[test]
    fn every_outcome_has_its_ledger_cause() {
        let classified = outcome_of(&ProducerOutcome::Completed);
        assert_eq!(classified, (OutcomeKind::Completed, ""));
        let refused = outcome_of(&ProducerOutcome::NotAdmitted {
            refusal: ClientSubmissionRefusal::Draining,
            message: String::new(),
        });
        assert_eq!(refused, (OutcomeKind::NotAdmitted, "draining"));
        let failed = outcome_of(&ProducerOutcome::ProcessingFailed {
            failure: ClientProcessingFailure::AckTimedOut,
            message: String::new(),
        });
        assert_eq!(failed, (OutcomeKind::ProcessingFailed, "ack_timeout"));
        let unknown = outcome_of(&ProducerOutcome::OutcomeUnknown {
            cause: SubmissionUncertainty::SessionLost,
            message: String::new(),
        });
        assert_eq!(unknown, (OutcomeKind::OutcomeUnknown, "session_lost"));
        assert_eq!(
            outcome_line(4, 3, OutcomeKind::OutcomeUnknown, "session_lost"),
            "OUTCOME tick=4 readings=3 outcome_unknown session_lost"
        );
        assert_eq!(
            outcome_line(4, 3, OutcomeKind::Completed, ""),
            "OUTCOME tick=4 readings=3 completed"
        );
    }

    #[test]
    fn a_reading_stamped_before_the_window_is_one_nanosecond_past_its_skew() {
        let origin = Timestamp::from_unix_nanos(1_000_000_000);
        let window = DomainAdmissionWindow::reached(
            origin,
            Timestamp::from_unix_nanos(1_300_000_000),
            DomainClockPeriod::try_from(Duration::from_millis(100))
                .assured("one hundred milliseconds is a valid period"),
            DomainClockSkew::try_from(Duration::from_millis(50))
                .assured("fifty milliseconds is a valid skew"),
        )
        .assured("the instant follows the origin");
        let stale = before_the_window(&window).assured("the stamp is representable");
        assert_eq!(stale, Timestamp::from_unix_nanos(949_999_999));
        assert!(!window.contains(stale));
        assert!(window.contains(Timestamp::from_unix_nanos(950_000_000)));
    }

    #[test]
    fn outstanding_batches_are_held_and_released_by_their_bytes() {
        let mut outstanding = Outstanding::default();
        outstanding.hold(10);
        outstanding.hold(20);
        outstanding.release(10);
        assert_eq!((outstanding.batches, outstanding.bytes), (1, 20));
    }

    #[test]
    fn a_description_value_is_read_by_its_label() {
        let description = "emitter: out\n  consumers: 2\nretained batches: 7\n";
        assert_eq!(described_value(description, "consumers: "), "2");
        assert_eq!(described_value(description, "retained batches: "), "7");
        assert_eq!(described_value(description, "retries: "), "-");
    }

    #[test]
    fn exit_statuses_follow_how_a_run_ended() {
        assert_eq!(Finish::Completed.exit_code(), ExitCode::SUCCESS);
        assert_eq!(Finish::Unresolved.exit_code(), ExitCode::from(3));
        assert_eq!(Finish::Configuration.exit_code(), ExitCode::from(2));
        assert_eq!(Finish::Failed.exit_code(), ExitCode::from(1));
        let stopped = RunError::Clock(Report::new(ClockError::Stopped {
            domain: DomainName::parse("paced_simulation").assured("the name is valid"),
            generation: 0,
        }));
        assert_eq!(stopped.finish(), Finish::Configuration);
        assert_eq!(
            stopped.to_string(),
            "the clock of domain 'paced_simulation' is stopped at generation 0; START the domain \
             before running the simulation"
        );
        let deadline = RunError::Deadline {
            outstanding: 2,
            deadline: Duration::from_secs(1),
        };
        assert_eq!(deadline.finish(), Finish::Failed);
        let refused = ProducerOpenError::Refused {
            ingestor: IngestorName::parse("simulated_readings").assured("the name is valid"),
            domain: DomainName::parse("paced_simulation").assured("the name is valid"),
            refusal: Refusal::from(nervix_models::ClientProducerRefusal::IngestorNotFound),
        };
        assert_eq!(
            refused.to_string(),
            "ingestor 'simulated_readings' of domain 'paced_simulation' refused the producer: \
             endpoint not found"
        );
    }
}
