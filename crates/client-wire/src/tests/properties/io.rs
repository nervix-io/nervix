//! Current client I/O outcomes carry exact identities, schemas, credits and opaque batch bytes.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use nervix_models::{
    AckWindow, ClientAttachmentId, ClientConsumerLimits, ClientEndpointContract,
    ClientProducerDescription, ClientProducerGrant, ClientProducerPolicy, ClientSubmissionOutcome,
};

use super::WireValues;
use crate::*;

impl WireValues<'_> {
    pub(super) fn io_replies(&mut self) -> Vec<ReplyBody> {
        let mut bodies = Vec::new();
        for window in [
            AckWindow::Sequential,
            AckWindow::Parallel {
                max: self.arbitrary.positive_u64(),
            },
        ] {
            let grant_bytes = self.arbitrary.positive_u64();
            let retry = self.duration();
            let description = ClientProducerDescription {
                attachment: ClientAttachmentId::from_u128(u128::from_be_bytes(self.digest())),
                fields: self.arbitrary.schema_fields(1),
                generation: self.arbitrary.entropy().any_u64(),
                contract: ClientEndpointContract::from_digest(self.digest()),
                policy: ClientProducerPolicy {
                    window,
                    ack_timeout: self.duration(),
                    retry_backoff: retry,
                    retry_max_backoff: retry,
                },
                grant: ClientProducerGrant {
                    batches: self.count32(),
                    bytes: grant_bytes,
                    max_batch_bytes: grant_bytes,
                    max_batch_rows: self.count32(),
                },
                admission: self.arbitrary.entropy().pick([
                    nervix_models::ClientProducerAdmission::Open,
                    nervix_models::ClientProducerAdmission::Suspended,
                ]),
            };
            bodies.push(ReplyBody::OpenIngestor(OpenIngestorOutcome {
                disposition: OpenIngestorDisposition::Opened(Box::new(ProducerOpened {
                    domain: self.arbitrary.name(),
                    ingestor: self.arbitrary.name(),
                    description,
                })),
                message: self.arbitrary.string(),
            }));
            bodies.push(ReplyBody::OpenEmitter(OpenEmitterOutcome {
                disposition: OpenEmitterDisposition::Opened(Box::new(EmitterOpened {
                    domain: self.arbitrary.name(),
                    emitter: self.arbitrary.name(),
                    fields: self.arbitrary.schema_fields(1),
                    generation: self.arbitrary.entropy().any_u64(),
                    contract: ClientEndpointContract::from_digest(self.digest()),
                    window,
                    ack_timeout: self.duration(),
                    retry_backoff: retry,
                    retry_max_backoff: retry,
                    granted: ClientConsumerLimits {
                        batches: self.count32(),
                        bytes: grant_bytes,
                    },
                    max_batch_bytes: grant_bytes.get(),
                    max_batch_rows: self.count32().get(),
                })),
                message: self.arbitrary.string(),
            }));
        }
        for &refusal in crate::producer::ALL_PRODUCER_REFUSALS {
            bodies.push(ReplyBody::OpenIngestor(OpenIngestorOutcome {
                disposition: OpenIngestorDisposition::Refused(refusal),
                message: self.arbitrary.string(),
            }));
        }
        for &refusal in crate::consumer::ALL_EMITTER_OPEN_REFUSALS {
            bodies.push(ReplyBody::OpenEmitter(OpenEmitterOutcome {
                disposition: OpenEmitterDisposition::Refused(refusal),
                message: self.arbitrary.string(),
            }));
        }
        let mut outcomes = vec![ClientSubmissionOutcome::Completed];
        for refusal in crate::producer::all_submission_refusals() {
            outcomes.push(ClientSubmissionOutcome::NotAdmitted(refusal));
        }
        for &failure in crate::producer::ALL_PROCESSING_FAILURES {
            outcomes.push(ClientSubmissionOutcome::ProcessingFailed(failure));
        }
        for &cause in crate::producer::ALL_OUTCOME_UNCERTAINTIES {
            outcomes.push(ClientSubmissionOutcome::OutcomeUnknown(cause));
        }
        for outcome in outcomes {
            bodies.push(ReplyBody::Submission(SubmissionOutcome {
                outcome,
                message: self.arbitrary.string(),
            }));
        }
        for disposition in [
            CloseIngestorDisposition::Closed,
            CloseIngestorDisposition::NotOpen,
        ] {
            bodies.push(ReplyBody::CloseIngestor(CloseIngestorOutcome {
                disposition,
                message: self.arbitrary.string(),
            }));
        }
        for branched in [false, true] {
            let branch_fingerprint = if branched { Some(self.digest()) } else { None };
            bodies.push(ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
                disposition: ReadEmitterDisposition::Batch(EmitterBatchReceived {
                    identity: uuid::Uuid::from_bytes(self.digest()),
                    reference: uuid::Uuid::from_bytes(self.digest()),
                    source_relay: self.arbitrary.name(),
                    branch_fingerprint,
                    batch: self.bytes(1),
                    members: self.count32().get(),
                    execution_now: self.timestamp(),
                }),
                message: self.arbitrary.string(),
            }));
        }
        bodies.push(ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
            disposition: ReadEmitterDisposition::Ended,
            message: self.arbitrary.string(),
        }));
        for &disposition in crate::consumer::ALL_EMITTER_SETTLEMENTS {
            bodies.push(ReplyBody::SettleEmitterBatch(SettleEmitterBatchOutcome {
                disposition,
                message: self.arbitrary.string(),
            }));
        }
        for &disposition in crate::consumer::ALL_EMITTER_CLOSE_DISPOSITIONS {
            bodies.push(ReplyBody::CloseEmitter(CloseEmitterOutcome {
                disposition,
                message: self.arbitrary.string(),
            }));
        }
        bodies
    }
}
