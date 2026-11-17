//! The allowance of one ingestor execution unit, and what its payloads pay.
//!
//! Layer: data plane.
//!
//! - **Owns.** The compiled form of an ingestor's rate-limit plan; one execution unit's allowance;
//!   the read budget an on-demand source is asked for; what each payload costs in each clause,
//!   measured once per read before its codec decodes it; admission; and the rejection of a payload
//!   no allowance can ever admit or whose measure fails, reported under the ingestor's
//!   `ON GENERAL ERROR` policy and counted.
//! - **Depends on.** The typed rate-limit plan, the allowance engine, compiled measure programs,
//!   the ingest metadata projection, and the node's executor, metrics and runtime events.
//! - **Must not know.** A connector driver, the codec, how a payload's position is settled, or how
//!   a caller waits: an admission that must wait returns the instant, and its caller waits on the
//!   boundary's timer and asks again.
//!
//! An allowance belongs to one execution unit and is owned by the one task that admits the unit's
//! payloads, so admitting takes no lock: the engine's exclusive borrow is the whole transaction.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{
    fmt,
    num::{NonZeroU32, NonZeroUsize},
};

use arrow_array::RecordBatchOptions;
use error_stack::ResultExt as _;
use nervix_connector::{IngestMetadataRow, IngestorReadBudget};
use nervix_models::IngestorPayloadField;
use nervix_rate_limit::{Admission, Charge, Clause, Limiter, Quota, UnitCapacity};
use uuid::Uuid;

use super::{
    filter_map::{FilterMapBatchInputs, execute_filter_map_program_on_batch},
    message_error_decode::TransportPosition,
    *,
};
use crate::{metrics::IngestorRateLimitCounters, registry::RateLimitCharge};

/// The compiled form of an ingestor's `RATE LIMIT` clauses, which every execution unit of the
/// ingestor on this node shares.
#[derive(Debug)]
pub(crate) struct BoundRateLimit {
    clauses: Vec<BoundClause>,
    /// The position of every clause, which a read's hold asks about: no payload is admitted while
    /// one of them has no room for a single unit.
    every: Vec<usize>,
    /// The positions of the clauses every payload pays one unit of, which alone bound how many
    /// payloads a read may take.
    counting: Vec<usize>,
    /// What the clauses' measure programs read of each payload. Absent where no clause runs one.
    measured: Option<Arc<IngestMetadataProjection>>,
}

#[derive(Debug)]
struct BoundClause {
    quota: Quota,
    charge: BoundCharge,
}

#[derive(Debug)]
enum BoundCharge {
    /// Every payload costs one unit.
    Payload,
    /// A payload costs one of its payload facts, read without a program.
    PayloadFact(IngestorPayloadField),
    /// A payload costs what this program computes from it.
    Measure(CompiledProgramWithMaterializedInterest),
}

impl BoundRateLimit {
    /// Binds the planned clauses, compiling each measure through `compile`. `measured` is the
    /// projection the measures read, built where any clause runs one.
    pub(super) fn bind<E>(
        clauses: &[crate::registry::RateLimitClausePlan],
        measured: Option<Arc<IngestMetadataProjection>>,
        mut compile: impl FnMut(
            &crate::registry::LoweredMeasure,
        ) -> Result<CompiledProgramWithMaterializedInterest, E>,
    ) -> Result<Self, E> {
        let mut bound = Vec::with_capacity(clauses.len());
        let mut every = Vec::with_capacity(clauses.len());
        let mut counting = Vec::new();
        for (position, clause) in clauses.iter().enumerate() {
            every.push(position);
            if clause.charge.counts_payloads() {
                counting.push(position);
            }
            let charge = match &clause.charge {
                RateLimitCharge::Payload => BoundCharge::Payload,
                RateLimitCharge::PayloadFact(fact) => BoundCharge::PayloadFact(*fact),
                RateLimitCharge::Measure(measure) => BoundCharge::Measure(compile(measure)?),
            };
            bound.push(BoundClause {
                quota: clause.quota,
                charge,
            });
        }
        Ok(Self {
            clauses: bound,
            every,
            counting,
            measured,
        })
    }
}

/// Where a unit reports and counts what its allowance holds back and rejects.
pub(super) struct RateLimitReporting {
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) policy: GeneralErrorPolicy,
    pub(super) counters: IngestorRateLimitCounters,
}

/// One execution unit's allowance, owned by the one task that admits the unit's payloads.
pub(super) struct UnitAllowance {
    bound: Arc<BoundRateLimit>,
    limiter: Limiter,
    reporting: RateLimitReporting,
}

/// What admitting one payload decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AllowanceDecision {
    Admitted,
    /// No room before this instant: the caller waits on the boundary's timer and asks again.
    WaitUntil(Instant),
    /// The payload costs more than the clause at this position, from 1, can ever hold.
    AboveBurst {
        clause: usize,
        cost: u64,
        burst: NonZeroU32,
    },
}

/// What one payload costs, clause by clause, or why no allowance can take it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PayloadCost {
    Charges(Vec<Charge>),
    Rejected {
        clause: usize,
        cause: RateLimitRejectionCause,
    },
}

/// Why an allowance could not decide on a payload, which ends its admission without judging it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(super) enum AllowanceError {
    #[error("failed to project the metadata a RATE LIMIT measure reads")]
    ProjectMetadata,
    #[error("failed to run a RATE LIMIT measure")]
    RunMeasure,
    #[error("a RATE LIMIT measure returned {produced} values for {payloads} payloads")]
    MeasureRows { produced: usize, payloads: usize },
    #[error("the allowance refused its own charges")]
    Admission,
}

impl UnitAllowance {
    /// The allowance of one execution unit that starts now, full.
    pub(super) fn start(bound: Arc<BoundRateLimit>, reporting: RateLimitReporting) -> Self {
        let mut clauses = Vec::with_capacity(bound.clauses.len());
        for clause in &bound.clauses {
            clauses.push(Clause::direct(clause.quota));
        }
        Self {
            limiter: Limiter::new(clauses, Instant::now()),
            bound,
            reporting,
        }
    }

    /// How many messages the next read may take: the `window` its delivery mode leaves room for,
    /// lowered to how many payloads the clauses every payload pays one unit of admit now. A clause
    /// with a measure bounds no payload count, so its payloads pay once the read returned; while
    /// any clause has no room for even one unit, no payload could be admitted, so the read waits.
    pub(super) fn read_budget(&self, window: NonZeroUsize) -> IngestorReadBudget {
        let now = Instant::now();
        match self.limiter.unit_capacity_at(&self.bound.every, now) {
            Ok(UnitCapacity::ExhaustedUntil(until)) => return IngestorReadBudget::WaitUntil(until),
            Ok(UnitCapacity::Available(_)) => {}
            Err(error) => return self.unsized_read(window, &error),
        }
        if self.bound.counting.is_empty() {
            return IngestorReadBudget::Take(window);
        }
        match self.limiter.unit_capacity_at(&self.bound.counting, now) {
            Ok(UnitCapacity::Available(units)) => {
                let units: usize = units.get().arch_into();
                let units = NonZeroUsize::new(units)
                    .verified("an available capacity admits at least one payload");
                IngestorReadBudget::Take(window.min(units))
            }
            Ok(UnitCapacity::ExhaustedUntil(until)) => IngestorReadBudget::WaitUntil(until),
            Err(error) => self.unsized_read(window, &error),
        }
    }

    /// The budget of a read the allowance could not size. Every clause is direct and read at the
    /// node's monotonic clock, so this is a defect of the allowance; the read takes its window and
    /// admission, which charges every payload it took, reports the same failure as the intake's.
    fn unsized_read(
        &self,
        window: NonZeroUsize,
        error: &Report<nervix_rate_limit::AdmissionError>,
    ) -> IngestorReadBudget {
        debug!(
            domain = self.reporting.domain.as_str(),
            ingestor = self.reporting.ingestor.as_str(),
            error = %format_args!("{error:#}"),
            "a RATE LIMIT allowance could not size a read; the read takes its window"
        );
        IngestorReadBudget::Take(window)
    }

    /// What each payload costs in each clause, in the order given: `payloads` names each payload's
    /// metadata, and `batch_rows` the row count of a client producer's batch. Every clause with a
    /// measure runs its program once over all of them; `run` is the executor and the instant the
    /// caller took the payloads in, which a deterministic measure never reads.
    pub(super) async fn costs(
        &self,
        run: ProgramRun<'_>,
        payloads: &[IngestMetadataRow<'_>],
        batch_rows: Option<usize>,
    ) -> Result<Vec<PayloadCost>, Report<AllowanceError>> {
        let measured = self.measure(run, payloads, batch_rows).await?;
        let mut costs = Vec::with_capacity(payloads.len());
        for (index, payload) in payloads.iter().enumerate() {
            let mut charges = Vec::with_capacity(self.bound.clauses.len());
            let mut rejected = None;
            for (position, clause) in self.bound.clauses.iter().enumerate() {
                let clause_number = position
                    .checked_add(1)
                    .verified("a body's clauses are far fewer than usize::MAX");
                let units = match &clause.charge {
                    BoundCharge::Payload => 1,
                    BoundCharge::PayloadFact(IngestorPayloadField::Bytes) => {
                        payload.fields.payload_bytes().arch_into()
                    }
                    BoundCharge::PayloadFact(IngestorPayloadField::Rows) => {
                        let rows = batch_rows.verified(
                            "only a client producer declares payload.rows, and its admission \
                             passes the row count its batch's framing declares",
                        );
                        rows.arch_into()
                    }
                    BoundCharge::Measure(_) => {
                        let values = measured
                            .get(position)
                            .verified("the measurement has one entry for every clause");
                        let values = values
                            .as_ref()
                            .verified("a value was measured for every measured clause");
                        let value = values
                            .get(index)
                            .verified("a measured clause has one value for each payload it costs");
                        match value {
                            Ok(units) => *units,
                            Err(cause) => {
                                rejected = Some(PayloadCost::Rejected {
                                    clause: clause_number,
                                    cause: cause.clone(),
                                });
                                break;
                            }
                        }
                    }
                };
                match Self::charge(units) {
                    Ok(charge) => charges.push(charge),
                    Err(cost) => {
                        rejected = Some(PayloadCost::Rejected {
                            clause: clause_number,
                            cause: RateLimitRejectionCause::AboveBurst {
                                cost,
                                burst: clause.quota.burst(),
                            },
                        });
                        break;
                    }
                }
            }
            let cost = match rejected {
                Some(rejected) => rejected,
                None => PayloadCost::Charges(charges),
            };
            costs.push(cost);
        }
        Ok(costs)
    }

    /// The charge of `units` in one clause: none for a payload that costs nothing, and a cost no
    /// burst holds, beyond `u32`, refused as the cost it is.
    fn charge(units: u64) -> Result<Charge, u64> {
        let Ok(cost) = u32::try_from(units) else {
            return Err(units);
        };
        match NonZeroU32::new(cost) {
            Some(cost) => Ok(Charge::Direct(cost)),
            None => Ok(Charge::Free),
        }
    }

    /// The value each measured clause computes for each payload, by clause position; a clause
    /// without a measure has none.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the VM selected-row iterator is the bounded row selection of this read's \
                      payloads"
        )
    )]
    async fn measure(
        &self,
        run: ProgramRun<'_>,
        payloads: &[IngestMetadataRow<'_>],
        batch_rows: Option<usize>,
    ) -> Result<Vec<Option<Vec<Result<u64, RateLimitRejectionCause>>>>, Report<AllowanceError>>
    {
        let mut measured = Vec::with_capacity(self.bound.clauses.len());
        let Some(projection) = &self.bound.measured else {
            measured.resize_with(self.bound.clauses.len(), || None);
            return Ok(measured);
        };
        let rows = payloads.len();
        let mut builders = IngestMetadataBuilders::new(projection.clone(), rows);
        for payload in payloads {
            builders
                .append(payload, batch_rows)
                .change_context(AllowanceError::ProjectMetadata)?;
        }
        let metadata = builders
            .finish()
            .change_context(AllowanceError::ProjectMetadata)?;
        // A measure reads no decoded field, so its carrier holds one empty row per payload.
        let empty = StdArc::new(arrow_schema::Schema::empty());
        let carrier = RecordBatch::try_new_with_options(
            empty.clone(),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .change_context(AllowanceError::RunMeasure)?;
        let carrier = RuntimeRecordBatch::from_record_batch(empty, carrier)
            .change_context(AllowanceError::RunMeasure)?;
        let keys = vec![None; rows];
        let side_inputs = HashMap::default();
        for clause in &self.bound.clauses {
            let BoundCharge::Measure(program) = &clause.charge else {
                measured.push(None);
                continue;
            };
            let executed = execute_filter_map_program_on_batch(
                ProgramRun {
                    executor: run.executor,
                    now: run.now,
                },
                program,
                FilterMapBatchInputs {
                    carrier: &carrier,
                    namespace_batches: &[],
                    keys: &keys,
                    side_inputs: &side_inputs,
                    ingest_metadata: Some(&metadata),
                },
                (0..rows).map(|_| AckSet::empty()).collect(),
                None,
            )
            .await
            .map_err(|failure| failure.error.change_context(AllowanceError::RunMeasure))?;
            if executed.selected_rows.len() != rows || executed.batch.row_count() != rows {
                return Err(Report::new(AllowanceError::MeasureRows {
                    produced: executed.batch.row_count(),
                    payloads: rows,
                }));
            }
            let Some(values) = executed.batch.column(0).as_uint64() else {
                return Err(Report::new(AllowanceError::RunMeasure));
            };
            // A measure has no FILTER, so the run keeps every payload, in order.
            let mut clause_values = Vec::with_capacity(rows);
            for (output_row, input_row) in executed.selected_rows.iter().enumerate() {
                if input_row != output_row {
                    return Err(Report::new(AllowanceError::MeasureRows {
                        produced: executed.batch.row_count(),
                        payloads: rows,
                    }));
                }
                if let Some(refusal) = metadata.refusal(input_row) {
                    clause_values.push(Err(RateLimitRejectionCause::MeasureFailed {
                        reason: refusal.message(),
                    }));
                    continue;
                }
                if let Some(side_error) = executed.batch.errors().row(output_row).first() {
                    clause_values.push(Err(RateLimitRejectionCause::MeasureFailed {
                        reason: format!("{}: {}", side_error.code().as_str(), side_error.reason),
                    }));
                    continue;
                }
                if values.is_null(output_row) {
                    clause_values.push(Err(RateLimitRejectionCause::MeasureFailed {
                        reason: "the measure computed no value".to_owned(),
                    }));
                    continue;
                }
                clause_values.push(Ok(values.value(output_row)));
            }
            measured.push(Some(clause_values));
        }
        Ok(measured)
    }

    /// Admits one payload whose cost is `charges` at this instant of the node's monotonic clock.
    pub(super) fn admit(
        &mut self,
        charges: &[Charge],
    ) -> Result<AllowanceDecision, Report<AllowanceError>> {
        match self
            .limiter
            .admit(charges)
            .change_context(AllowanceError::Admission)?
        {
            Admission::Admitted => Ok(AllowanceDecision::Admitted),
            Admission::WaitUntil(until) => Ok(AllowanceDecision::WaitUntil(until)),
            Admission::NeverAdmissible {
                clause,
                cost,
                burst,
            } => Ok(AllowanceDecision::AboveBurst {
                clause: clause
                    .checked_add(1)
                    .verified("a body's clauses are far fewer than usize::MAX"),
                cost: cost.get().into(),
                burst,
            }),
            // No clause of this allowance is keyed, so no key budget can run out.
            Admission::KeyBudgetExhausted { .. } => Err(Report::new(AllowanceError::Admission)),
        }
    }

    /// Counts one hold of the unit's source for the allowance as it starts, so a long hold is
    /// visible while it lasts.
    pub(super) fn record_hold_started(&self) {
        self.reporting.counters.held.inc();
    }

    /// Adds the time one hold of the unit's source lasted, once it ended.
    pub(super) fn record_hold_ended(&self, held: Duration) {
        self.reporting
            .counters
            .hold_seconds
            .inc_by(held.as_secs_f64());
    }

    /// Consumes the payload `rejection` stands for: reports it under the ingestor's general error
    /// policy and counts it.
    pub(super) fn reject(&self, rejection: &RateLimitRejection) {
        match rejection.cause {
            RateLimitRejectionCause::AboveBurst { .. } => self.reporting.counters.rejected.inc(),
            RateLimitRejectionCause::MeasureFailed { .. } => {
                self.reporting.counters.program_rejections.inc();
            }
        }
        let RateLimitReporting {
            runtime,
            domain,
            ingestor,
            policy,
            ..
        } = &self.reporting;
        match policy {
            GeneralErrorPolicy::Ignore => {
                debug!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    error_reference = %rejection.reference,
                    "consumed a payload its RATE LIMIT rejected under an IGNORE general error policy"
                );
            }
            GeneralErrorPolicy::Log => {
                runtime.inner.events.report_error(format!(
                    "ingestor '{}' general error in domain '{}': rate limit rejection {rejection}",
                    ingestor.as_str(),
                    domain.as_str(),
                ));
                warn!(
                    domain = domain.as_str(),
                    node_kind = ModelKind::Ingestor.as_str(),
                    node = ingestor.as_str(),
                    error_reference = %rejection.reference,
                    error_code = RateLimitRejection::CODE.as_ref(),
                    error_operation = RateLimitRejection::OPERATION.as_ref(),
                    rejection = %rejection,
                    "runtime node consumed a payload its RATE LIMIT rejected"
                );
            }
        }
    }
}

/// Why a payload's `RATE LIMIT` rejected it. The rejection consumes the payload rather than
/// presenting it again, which would only meet the same refusal.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "a payload its allowance can never admit, or whose measure fails, is a record \
                  outcome the ingestor consumes"
    )
)]
pub(crate) enum RateLimitRejectionCause {
    /// The payload costs more than the clause's burst, which is the most it ever holds.
    #[error("a payload costing {cost} exceeds the burst {burst}")]
    AboveBurst { cost: u64, burst: NonZeroU32 },
    /// The clause's measure failed on the payload; `reason` names the failure and no value.
    #[error("the measure failed: {reason}")]
    MeasureFailed { reason: String },
}

/// A payload its ingestor's `RATE LIMIT` rejected, as the message error that remains of it once
/// the ingestor consumed it. It never holds the payload or a value read from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RateLimitRejection {
    /// The rejection's stable reference, a UUIDv7.
    pub(crate) reference: Uuid,
    /// The position of the clause that rejected the payload, from 1.
    pub(crate) clause: usize,
    pub(crate) cause: RateLimitRejectionCause,
    /// Where the ingestor read the payload, when its metadata names a position.
    pub(crate) position: Option<TransportPosition>,
    pub(crate) occurred_at: Timestamp,
}

impl RateLimitRejection {
    /// The payload does not satisfy its ingestor's allowance.
    pub(crate) const CODE: MessageErrorCode = MessageErrorCode::Validation;
    pub(crate) const OPERATION: MessageErrorOperation = MessageErrorOperation::RateLimit;

    /// The rejection of the payload `metadata` names, by the clause at `clause`, from 1, which the
    /// host rejected at `occurred_at`.
    pub(super) fn new(
        clause: usize,
        cause: RateLimitRejectionCause,
        metadata: &IngestMetadataRow<'_>,
        occurred_at: Timestamp,
    ) -> Self {
        Self {
            reference: Uuid::now_v7(),
            clause,
            cause,
            position: metadata.position.map(TransportPosition::retain),
            occurred_at,
        }
    }
}

impl fmt::Display for RateLimitRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at {}: ",
            self.reference,
            self.occurred_at.to_rfc3339()
        )?;
        match &self.cause {
            RateLimitRejectionCause::AboveBurst { cost, burst } => write!(
                formatter,
                "a payload costing {cost} exceeds the burst {burst} of RATE LIMIT clause {}",
                self.clause
            )?,
            RateLimitRejectionCause::MeasureFailed { .. } => write!(
                formatter,
                "the measure of RATE LIMIT clause {} failed for the payload",
                self.clause
            )?,
        }
        if let Some(position) = &self.position {
            write!(formatter, " from {position}")?;
        }
        if let RateLimitRejectionCause::MeasureFailed { reason } = &self.cause {
            write!(formatter, ": {reason}")?;
        }
        Ok(())
    }
}
