//! Scripted implementations of the four emitter boundaries.
//!
//! Layer: test harness.
//! - **Owns.** Publish scripts, complete write observations, payload preparation and staged commits.
//! - **Depends on.** The connector contract, vocabulary, Arrow and selected primitives.
//! - **Must not know.** Runtime buffers, ACK trees, graph state or transport drivers.

use std::{collections::VecDeque, marker::PhantomData, num::NonZeroUsize, time::Duration};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::*;
use nervix_models::{EmitterEntityKind, StructuredMessageError, Timestamp};
use nervix_primitives::{
    sync::{Arc, blocking::Mutex},
    time::Instant,
};

/// The answer to one member, in the order the host handed members over.
#[derive(Clone, Debug)]
pub enum MemberAnswer {
    Deliver,
    Reject(String),
    Unanswered,
}

/// The external answer to one publish call. No real wait or transport is involved.
#[derive(Clone, Debug)]
pub enum PublishAnswer {
    ConfirmAll,
    Fail,
    DeliverFirstThenFail,
    RejectFirst,
    Members(Vec<MemberAnswer>),
    RetryAfter(Duration),
}

/// A finite script refuses an unexpected extra publish; repetition is always explicit.
pub enum PublishScript {
    Finite(VecDeque<PublishAnswer>),
    Repeat(PublishAnswer),
}

impl<const N: usize> From<[PublishAnswer; N]> for PublishScript {
    fn from(answers: [PublishAnswer; N]) -> Self {
        Self::Finite(VecDeque::from(answers))
    }
}

impl From<VecDeque<PublishAnswer>> for PublishScript {
    fn from(answers: VecDeque<PublishAnswer>) -> Self {
        Self::Finite(answers)
    }
}

impl PublishScript {
    fn next(&mut self) -> PublishAnswer {
        match self {
            Self::Finite(answers) => answers
                .pop_front()
                .assured("the fixture scripts every publish"),
            Self::Repeat(answer) => answer.clone(),
        }
    }

    fn answer<Id: Copy>(&mut self, members: &[(Id, Timestamp)]) -> PerRecordOutcome<Id> {
        let mut outcome = PerRecordOutcome::with_capacity(members.len());
        let answer = self.next();
        match answer {
            PublishAnswer::ConfirmAll => {
                for (id, _) in members {
                    outcome.deliver(*id);
                }
            }
            PublishAnswer::Fail => outcome.fail(Report::new(EmitterWriteError::Publish {
                connector: "scripted",
            })),
            PublishAnswer::RetryAfter(delay) => outcome.fail(
                Report::new(EmitterWriteError::Publish {
                    connector: "scripted",
                })
                .attach(EmitterRetryDelay(delay)),
            ),
            PublishAnswer::DeliverFirstThenFail => {
                let (id, _) = members
                    .first()
                    .assured("the partial-delivery script requires a member");
                outcome.deliver(*id);
                outcome.fail(Report::new(EmitterWriteError::Publish {
                    connector: "scripted",
                }));
            }
            PublishAnswer::RejectFirst => {
                for (index, (id, at)) in members.iter().enumerate() {
                    if index == 0 {
                        outcome.reject(RejectedEmitterMember::external(
                            *id,
                            *at,
                            "refused".to_string(),
                        ));
                    } else {
                        outcome.deliver(*id);
                    }
                }
            }
            PublishAnswer::Members(answers) => {
                assert_eq!(
                    answers.len(),
                    members.len(),
                    "the script answers each handed member"
                );
                for ((id, at), answer) in members.iter().zip(answers) {
                    match answer {
                        MemberAnswer::Deliver => outcome.deliver(*id),
                        MemberAnswer::Reject(reason) => {
                            outcome.reject(RejectedEmitterMember::external(*id, *at, reason))
                        }
                        MemberAnswer::Unanswered => {}
                    }
                }
            }
        }
        outcome
    }
}

/// Everything this fixture was handed, kept under the boundary's original types.
pub struct EmitterObservations<W> {
    pub writes: Vec<W>,
    pub preparations: usize,
    pub finishes: Vec<Instant>,
    pub commits: Vec<EmitterCommitReport>,
}

impl<W> Default for EmitterObservations<W> {
    fn default() -> Self {
        Self {
            writes: Vec::new(),
            preparations: 0,
            finishes: Vec::new(),
            commits: Vec::new(),
        }
    }
}

impl EmitterObservations<Vec<EmitterRecord>> {
    pub fn payloads(&self) -> Vec<Vec<Vec<u8>>> {
        let mut writes = Vec::with_capacity(self.writes.len());
        for write in &self.writes {
            let payloads = write.iter().map(|record| record.payload.clone()).collect();
            writes.push(payloads);
        }
        writes
    }

    /// The envelope of every record of every write, in the order the host handed them over.
    pub fn envelopes(&self) -> Vec<Vec<RecordEnvelope>> {
        let mut writes = Vec::with_capacity(self.writes.len());
        for write in &self.writes {
            let envelopes = write.iter().map(|record| record.envelope.clone()).collect();
            writes.push(envelopes);
        }
        writes
    }
}

/// HTTP content, separate from the identity and admission time retained in `writes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpContent {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

impl EmitterObservations<Vec<EmitterHttpRequest>> {
    pub fn contents(&self) -> Vec<Vec<HttpContent>> {
        let mut writes = Vec::with_capacity(self.writes.len());
        for write in &self.writes {
            let mut requests = Vec::with_capacity(write.len());
            for request in write {
                let headers = request
                    .headers
                    .iter()
                    .map(|(name, value)| (name.as_str().to_string(), value.as_str().to_string()))
                    .collect();
                requests.push(HttpContent {
                    method: request.method.as_str().to_string(),
                    target: request.target.as_str().to_string(),
                    headers,
                    body: request.body.clone(),
                });
            }
            writes.push(requests);
        }
        writes
    }
}

/// One row carrier. Cloning Arrow retains its columns rather than copying row values.
pub struct RecordedCarrier {
    pub batch_index: usize,
    pub batch: RecordBatch,
    pub rows: Vec<usize>,
    pub occurred_at: Timestamp,
}

pub struct RecordedRows {
    pub columns: Vec<String>,
    pub carriers: Vec<RecordedCarrier>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CarrierSelection {
    pub batch_index: usize,
    pub rows: Vec<usize>,
}

impl EmitterObservations<RecordedRows> {
    pub fn selections(&self) -> Vec<Vec<CarrierSelection>> {
        let mut writes = Vec::with_capacity(self.writes.len());
        for write in &self.writes {
            let carriers = write
                .carriers
                .iter()
                .map(|carrier| CarrierSelection {
                    batch_index: carrier.batch_index,
                    rows: carrier.rows.clone(),
                })
                .collect();
            writes.push(carriers);
        }
        writes
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct HandedRequest {
    pub id: EmitterRecordId,
    pub body: Vec<u8>,
}

impl HandedRequest {
    pub fn new(id: usize, body: &str) -> Self {
        Self {
            id: EmitterRecordId::new(id),
            body: body.as_bytes().to_vec(),
        }
    }
}

impl EmitterObservations<Vec<EmitterRowRequest>> {
    pub fn requests(&self) -> Vec<Vec<HandedRequest>> {
        let mut writes = Vec::with_capacity(self.writes.len());
        for write in &self.writes {
            let requests = write
                .iter()
                .map(|request| HandedRequest {
                    id: request.id,
                    body: request.body.clone(),
                })
                .collect();
            writes.push(requests);
        }
        writes
    }
}

/// The fixture uses a real entity declaration for the family it implements.
pub trait EmitterFamily: Send + Sync + 'static {
    const KIND: EmitterEntityKind;
    type Write: Send;
    type Preparation: Default + Send;
}

pub struct Records;
pub struct HttpRequests;
pub struct Rows;
pub struct RowRequests;

#[derive(Default)]
pub enum PayloadPreparation {
    #[default]
    Identity,
    Numbered,
    Refuse(StructuredMessageError),
}

pub enum RequestPreparation {
    Pairs(NonZeroUsize),
    ForeignRow,
}

impl Default for RequestPreparation {
    fn default() -> Self {
        Self::Pairs(NonZeroUsize::new(2).assured("two is positive"))
    }
}

impl EmitterFamily for Records {
    const KIND: EmitterEntityKind =
        EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic);
    type Write = Vec<EmitterRecord>;
    type Preparation = PayloadPreparation;
}
impl EmitterFamily for HttpRequests {
    const KIND: EmitterEntityKind =
        EmitterEntityKind::Http(nervix_models::HttpEmitterEntity::Request);
    type Write = Vec<EmitterHttpRequest>;
    type Preparation = ();
}
impl EmitterFamily for Rows {
    const KIND: EmitterEntityKind =
        EmitterEntityKind::ClickHouse(nervix_models::ClickHouseEmitterEntity::Table);
    type Write = RecordedRows;
    type Preparation = ();
}
impl EmitterFamily for RowRequests {
    const KIND: EmitterEntityKind = EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Logs);
    type Write = Vec<EmitterRowRequest>;
    type Preparation = RequestPreparation;
}

struct PendingStage {
    messages: u64,
    occurred_at: Timestamp,
    acknowledgements: Vec<EmitterRecordAcknowledgements>,
}

struct Staging {
    cadence: Duration,
    size_boundary: u64,
    pending: Option<PendingStage>,
}

/// One scripted emitter, with its preparation and observations typed by connector family.
pub struct FakeEmitter<F: EmitterFamily> {
    script: PublishScript,
    observations: Arc<Mutex<EmitterObservations<F::Write>>>,
    pub preparation: F::Preparation,
    pub finish_results: VecDeque<EmitterFinishResult>,
    pub commit_errors: VecDeque<Report<EmitterWriteError>>,
    staging: Option<Staging>,
    family: PhantomData<F>,
}

impl<F: EmitterFamily> FakeEmitter<F> {
    pub fn new(script: impl Into<PublishScript>) -> Self {
        Self {
            script: script.into(),
            observations: Arc::new(Mutex::new(EmitterObservations::default())),
            preparation: F::Preparation::default(),
            staging: None,
            finish_results: VecDeque::new(),
            commit_errors: VecDeque::new(),
            family: PhantomData,
        }
    }

    pub fn observations(&self) -> Arc<Mutex<EmitterObservations<F::Write>>> {
        self.observations.clone()
    }

    pub fn staging(mut self, cadence: Duration, size_boundary: u64) -> Self {
        assert!(
            size_boundary > 0,
            "a staged commit needs a positive size boundary"
        );
        self.staging = Some(Staging {
            cadence,
            size_boundary,
            pending: None,
        });
        self
    }

    fn stage(
        &mut self,
        count: usize,
        occurred_at: Timestamp,
        acknowledgements: Option<EmitterRecordAcknowledgements>,
    ) {
        let Some(staging) = self.staging.as_mut() else {
            return;
        };
        let pending = staging.pending.get_or_insert_with(|| PendingStage {
            messages: 0,
            occurred_at,
            acknowledgements: Vec::new(),
        });
        pending.messages = pending
            .messages
            .checked_add(u64::try_from(count).assured("the fixture's member count fits u64"))
            .assured("the fixture stages a bounded set of rows");
        pending.occurred_at = pending.occurred_at.max(occurred_at);
        if let Some(acks) = acknowledgements {
            pending.acknowledgements.push(acks);
        }
    }
}

#[async_trait]
impl<F: EmitterFamily> EmitterLifecycle for FakeEmitter<F> {
    fn kind(&self) -> EmitterEntityKind {
        F::KIND
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness records the supplied deadline before its scripted finish \
                      outcome",
            key = "one scripted emitter observation log",
            bound = "one synchronous deadline observation; guards end before any await"
        )
    )]
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        self.observations.lock().finishes.push(deadline);
        match self.finish_results.pop_front() {
            Some(result) => result,
            None => {
                assert_eq!(
                    self.staged_messages(),
                    0,
                    "the host commits every staged row before finishing"
                );
                Ok(EmitterFinish::NothingHeld)
            }
        }
    }

    fn retains_acknowledgements(&self) -> bool {
        self.staging.is_some()
    }

    fn staged_messages(&self) -> u64 {
        if let Some(staging) = &self.staging
            && let Some(pending) = &staging.pending
        {
            return pending.messages;
        }
        0
    }

    fn staged_commit(&self) -> Option<EmitterStagedCommit> {
        let staging = self.staging.as_ref()?;
        let pending = staging.pending.as_ref()?;
        if pending.messages >= staging.size_boundary {
            return Some(EmitterStagedCommit::SizeReached);
        }
        Some(EmitterStagedCommit::Cadence(staging.cadence))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness serializes the recorded commit after settling its retained \
                      acknowledgements",
            key = "one scripted emitter observation log",
            bound = "one synchronous commit observation; guards end before any await"
        )
    )]
    async fn commit(&mut self) -> EmitterWriteResult<Option<EmitterCommitReport>> {
        if let Some(error) = self.commit_errors.pop_front() {
            return Err(error);
        }
        let Some(staging) = self.staging.as_mut() else {
            return Ok(None);
        };
        let Some(pending) = staging.pending.take() else {
            return Ok(None);
        };
        for acknowledgement in pending.acknowledgements {
            acknowledgement.acknowledge();
        }
        let report = EmitterCommitReport {
            messages: pending.messages,
            bytes: 0,
            domain_timestamp: pending.occurred_at,
        };
        self.observations.lock().commits.push(report);
        Ok(Some(report))
    }
}

#[async_trait]
impl RecordEmitter for FakeEmitter<Records> {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "the fixture scripts the contract's definitive payload refusal"
        )
    )]
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness records preparation before returning a scripted payload \
                      outcome",
            key = "one scripted emitter observation log",
            bound = "one synchronous update for the caller-owned payload; guards end before any \
                     await"
        )
    )]
    fn prepare_payload(
        &mut self,
        payload: Vec<u8>,
        _occurred_at: Timestamp,
    ) -> Result<Vec<u8>, StructuredMessageError> {
        let mut observations = self.observations.lock();
        observations.preparations = observations
            .preparations
            .checked_add(1)
            .assured("the fixture prepares a bounded set of payloads");
        match &self.preparation {
            PayloadPreparation::Identity => Ok(payload),
            PayloadPreparation::Numbered => {
                let mut prepared = format!("prepared-{}:", observations.preparations).into_bytes();
                prepared.extend_from_slice(&payload);
                Ok(prepared)
            }
            PayloadPreparation::Refuse(error) => Err(error.clone()),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness publishes one complete received record batch to its \
                      assertion owner",
            key = "one scripted emitter observation log",
            bound = "one synchronous append of the caller-owned batch; guards end before any await"
        )
    )]
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        let members: Vec<_> = records
            .iter()
            .map(|record| (record.id, record.occurred_at))
            .collect();
        let outcome = self.script.answer(&members);
        self.observations.lock().writes.push(records);
        outcome
    }
}

#[async_trait]
impl HttpRequestEmitter for FakeEmitter<HttpRequests> {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness publishes one complete received HTTP batch to its \
                      assertion owner",
            key = "one scripted emitter observation log",
            bound = "one synchronous append of the caller-owned batch; guards end before any await"
        )
    )]
    async fn publish(
        &mut self,
        requests: Vec<EmitterHttpRequest>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let members: Vec<_> = requests
            .iter()
            .map(|request| (request.id, request.occurred_at))
            .collect();
        let outcome = self.script.answer(&members);
        self.observations.lock().writes.push(requests);
        outcome
    }
}

#[async_trait]
impl RowEmitter for FakeEmitter<Rows> {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness publishes one complete mapped-row observation to its \
                      assertion owner",
            key = "one scripted emitter observation log",
            bound = "one synchronous append of the caller-owned batch; guards end before any await"
        )
    )]
    async fn publish(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> PerRecordOutcome<EmitterRecordPosition> {
        let members: Vec<_> = rows
            .members()
            .into_iter()
            .map(|member| (rows.position(member), rows.occurred_at(member)))
            .collect();
        let outcome = self.script.answer(&members);
        let mut stage_write = false;
        let outcome = if self.staging.is_some() {
            let parts = outcome.into_parts();
            assert!(
                parts.delivered.is_empty() || parts.delivered.len() == members.len(),
                "a staged fixture accepts a complete carrier or none of it"
            );
            stage_write = !parts.delivered.is_empty();
            let mut outcome = PerRecordOutcome::with_capacity(members.len());
            for id in parts.delivered {
                outcome.deliver(id);
            }
            for rejected in parts.rejected {
                outcome.reject(rejected);
            }
            if let Some(error) = parts.infrastructure_error {
                outcome.fail(error);
            }
            outcome
        } else {
            outcome
        };
        let mut carriers = Vec::new();
        for carrier in rows.carriers {
            if stage_write {
                self.stage(
                    carrier.selected_rows.len(),
                    carrier.occurred_at,
                    carrier.acknowledgements,
                );
            }
            carriers.push(RecordedCarrier {
                batch_index: carrier.batch_index,
                batch: carrier.batch.clone(),
                rows: carrier.selected_rows.to_vec(),
                occurred_at: carrier.occurred_at,
            });
        }
        self.observations.lock().writes.push(RecordedRows {
            columns: rows.target_columns.to_vec(),
            carriers,
        });
        outcome
    }
}

#[async_trait]
impl RowRequestEmitter for FakeEmitter<RowRequests> {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness records one preparation before constructing its scripted \
                      requests",
            key = "one scripted emitter observation log",
            bound = "one synchronous preparation-counter update; guards end before any await"
        )
    )]
    async fn prepare(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> EmitterWriteResult<RowRequestPreparation> {
        let preparation = {
            let mut observations = self.observations.lock();
            let index = observations.preparations;
            observations.preparations = index
                .checked_add(1)
                .assured("the fixture prepares a bounded set of batches");
            index
        };
        let mut prepared = RowRequestPreparation::default();
        match self.preparation {
            RequestPreparation::Pairs(size) => {
                for carrier in &rows.carriers {
                    for pair in carrier.selected_rows.chunks(size.get()) {
                        let members = pair
                            .iter()
                            .map(|row| EmitterRecordPosition {
                                batch_index: carrier.batch_index,
                                row_index: *row,
                            })
                            .collect();
                        prepared.requests.push(PreparedRowRequest {
                            members,
                            body: format!("preparation {preparation} of {pair:?}").into_bytes(),
                        });
                    }
                }
            }
            RequestPreparation::ForeignRow => {
                let carrier = rows
                    .carriers
                    .first()
                    .assured("the host supplies a carrier to prepare");
                let mut members: Vec<_> = carrier
                    .selected_rows
                    .iter()
                    .map(|row| EmitterRecordPosition {
                        batch_index: carrier.batch_index,
                        row_index: *row,
                    })
                    .collect();
                members.push(EmitterRecordPosition {
                    batch_index: carrier.batch_index,
                    row_index: carrier.batch.num_rows(),
                });
                prepared.requests.push(PreparedRowRequest {
                    members,
                    body: b"every row and one more".to_vec(),
                });
            }
        }
        Ok(prepared)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness publishes one complete received request batch to its \
                      assertion owner",
            key = "one scripted emitter observation log",
            bound = "one synchronous append of the caller-owned batch; guards end before any await"
        )
    )]
    async fn publish(
        &mut self,
        requests: Vec<EmitterRowRequest>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let members: Vec<_> = requests
            .iter()
            .map(|request| (request.id, request.occurred_at))
            .collect();
        let outcome = self.script.answer(&members);
        self.observations.lock().writes.push(requests);
        outcome
    }
}
