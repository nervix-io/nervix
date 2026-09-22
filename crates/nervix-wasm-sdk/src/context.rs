use std::time::Duration;

use nervix_wasm_protocol::{BranchInit, GuestSnapshot, ProcessorSchema, StateResetRequestAnswer};

use crate::{
    abi,
    envelope::OutputEnvelope,
    error::{GuestError, RejectedSnapshot},
    processor::Processor,
};

/// Branch-instance configuration decoded from the host `BranchInit` payload.
///
/// One guest instance exists per concrete branch, so everything here is
/// branch-local by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchContext {
    init: BranchInit,
}

impl From<BranchInit> for BranchContext {
    fn from(init: BranchInit) -> Self {
        Self { init }
    }
}

impl BranchContext {
    /// Encodes the snapshot of an instance this branch configuration initialized: the
    /// configuration itself, which a restore checks, and the application state its processor
    /// saved.
    pub(crate) fn encode_snapshot(&self, application_state: Vec<u8>) -> Vec<u8> {
        let snapshot = GuestSnapshot {
            init_metadata: self.init.encode(),
            application_state,
        };
        snapshot.encode()
    }

    /// Restores the processor of this branch configuration from a saved snapshot.
    ///
    /// The snapshot must decode and must have been taken under this exact branch configuration;
    /// only then does [`Processor::restore`] receive the application state it carries, including
    /// empty application state.
    pub(crate) fn restore_snapshot<P: Processor>(
        &self,
        saved: &[u8],
    ) -> Result<P, RejectedSnapshot> {
        let snapshot =
            GuestSnapshot::decode(saved).map_err(RejectedSnapshot::UndecodableEnvelope)?;
        let saved_init = BranchInit::decode(&snapshot.init_metadata)
            .map_err(RejectedSnapshot::UndecodableInitMetadata)?;
        if saved_init != self.init {
            return Err(RejectedSnapshot::OtherBranchConfiguration);
        }
        P::restore(self, &snapshot.application_state).map_err(RejectedSnapshot::ApplicationState)
    }

    pub fn domain_name(&self) -> &str {
        &self.init.domain_name
    }

    /// Descriptive host string; do not branch on it without a strict
    /// compatibility rule for the exact Nervix version being targeted.
    pub fn domain_type(&self) -> &str {
        &self.init.domain_type
    }

    /// Serialized concrete branch key for this instance.
    pub fn branch_key(&self) -> Option<&[u8]> {
        self.init.branch_key.as_deref()
    }

    pub fn input_schema(&self) -> &ProcessorSchema {
        &self.init.input_schema
    }

    /// One destination schema per declared `TO` relay, in declaration order.
    pub fn output_schemas(&self) -> &[ProcessorSchema] {
        &self.init.output_schemas
    }
}

/// Domain-clock instant. Unix nanoseconds are the ABI boundary format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DomainTime {
    unix_nanos: i64,
}

impl DomainTime {
    pub const fn from_unix_nanos(unix_nanos: i64) -> Self {
        Self { unix_nanos }
    }

    pub const fn unix_nanos(self) -> i64 {
        self.unix_nanos
    }
}

/// Handle identifying one guest-requested domain-clock timeout.
///
/// The host issues handles per branch instance, so a handle never outlives the instance that
/// requested it and is never part of saved state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeoutHandle(i64);

impl TimeoutHandle {
    pub(crate) const fn new(raw: i64) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> i64 {
        self.0
    }
}

/// Per-callback view over the branch configuration and the guest runtime
/// owned by the ABI adapter.
pub struct GuestContext<'rt> {
    pub(crate) branch: &'rt BranchContext,
    pub(crate) pending_emit: &'rt mut Vec<Vec<u8>>,
    pub(crate) global_error: &'rt mut Vec<u8>,
    pub(crate) error_state: &'rt mut Option<String>,
}

impl GuestContext<'_> {
    pub fn branch(&self) -> &BranchContext {
        self.branch
    }

    /// Reads the current domain-clock time through the host import.
    pub fn domain_time(&self) -> DomainTime {
        DomainTime::from_unix_nanos(abi::host_domain_time_nanos())
    }

    /// Requests a domain-clock timeout; the host later invokes the
    /// processor's `on_timeout` with the returned handle.
    ///
    /// The timeout belongs to this branch instance. An instance recreated from saved state starts
    /// without it, so request it again from the next callback when the restored state needs it.
    pub fn request_timeout(&self, delay: Duration) -> Result<TimeoutHandle, GuestError> {
        let delay_nanos = i64::try_from(delay.as_nanos()).map_err(|_| GuestError::InvalidSize)?;
        let handle = abi::host_timeout_after_nanos(delay_nanos);
        if handle < 0 {
            return Err(GuestError::InvalidSize);
        }
        Ok(TimeoutHandle::new(handle))
    }

    /// Asks the host to replace this branch's complete guest-state lifetime, starting a fresh
    /// instance from nothing instead of from the state saved last.
    ///
    /// The host schedules the replacement for after this callback returns and never calls back
    /// into the guest to perform it. The request is terminal for everything this callback has not
    /// committed: output queued with [`GuestContext::emit`] is discarded, the input the branch
    /// still holds is left unacknowledged for its source to redeliver, no checkpoint is taken, and
    /// this instance is dropped with its pending timeouts. Effects earlier callbacks already
    /// published stand, because their checkpoints completed.
    ///
    /// Requesting the replacement repeatedly, in one callback or across the callbacks of one state
    /// lifetime, replaces that lifetime once. The returned answer says only that the host took the
    /// request; the guest learns the new lifetime is durable by being created and initialized
    /// again. A `GuestContext` exists only inside the callbacks the host accepts requests from, so
    /// the answer a processor sees here is [`StateResetRequestAnswer::Accepted`] unless the host it
    /// runs on disagrees about which operation is in progress.
    ///
    /// Clearing the processor's own fields is an ordinary application-state mutation that the next
    /// checkpoint saves, and needs none of this.
    #[must_use]
    pub fn request_state_reset(&mut self) -> StateResetRequestAnswer {
        let code = abi::host_request_state_reset();
        match StateResetRequestAnswer::from_code(code) {
            Some(answer) => answer,
            None => StateResetRequestAnswer::Refused,
        }
    }

    /// Queues one output envelope for the host to collect through
    /// `nervix_read_emit`.
    pub fn emit(&mut self, output: OutputEnvelope) -> Result<(), GuestError> {
        let encoded = output.encode()?;
        self.pending_emit.push(encoded);
        Ok(())
    }

    /// Reports a global processor error while letting the current callback
    /// succeed. The host applies `ON GLOBAL ERROR` and the guest latches into
    /// error state. The error state belongs to this instance and is never saved, so an instance
    /// recreated from saved state starts without it.
    pub fn report_global_error(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        self.global_error.clear();
        self.global_error.extend_from_slice(reason.as_bytes());
        *self.error_state = Some(reason);
    }
}

#[cfg(test)]
mod tests {
    use nervix_wasm_protocol::{ProcessorField, ProcessorType, SavedStateRejection};

    use super::*;
    use crate::envelope::InputBatch;

    fn init(branch_key: &[u8]) -> BranchInit {
        BranchInit {
            domain_name: "events".to_string(),
            domain_type: "PACED".to_string(),
            branch_key: Some(branch_key.to_vec()),
            input_schema: ProcessorSchema {
                name: "input_events".to_string(),
                fields: vec![ProcessorField {
                    name: "value".to_string(),
                    ty: ProcessorType::I32,
                    optional: false,
                }],
            },
            output_schemas: vec![ProcessorSchema {
                name: "output_events".to_string(),
                fields: Vec::new(),
            }],
        }
    }

    fn branch(branch_key: &[u8]) -> BranchContext {
        BranchContext::from(init(branch_key))
    }

    fn saved_state(processor: &impl Processor) -> Vec<u8> {
        processor
            .save_state()
            .expect("the test processor must save its state")
    }

    /// Counts what it processed and saves the count as eight little-endian bytes.
    #[derive(Debug, PartialEq)]
    struct Counter {
        count: u64,
    }

    impl Processor for Counter {
        fn create(_branch: &BranchContext) -> Result<Self, GuestError> {
            Ok(Self { count: 0 })
        }

        fn process_batch(
            &mut self,
            _ctx: &mut GuestContext<'_>,
            _input: InputBatch,
        ) -> Result<(), GuestError> {
            Ok(())
        }

        fn save_state(&self) -> Result<Vec<u8>, GuestError> {
            Ok(self.count.to_le_bytes().to_vec())
        }

        fn restore(_branch: &BranchContext, state: &[u8]) -> Result<Self, GuestError> {
            let Ok(count) = <[u8; 8]>::try_from(state) else {
                return Err(GuestError::failed("saved count must be exactly 8 bytes"));
            };
            Ok(Self {
                count: u64::from_le_bytes(count),
            })
        }
    }

    /// Keeps no state, so it relies on the default `save_state` and `restore`.
    #[derive(Debug, PartialEq)]
    struct Stateless;

    impl Processor for Stateless {
        fn create(_branch: &BranchContext) -> Result<Self, GuestError> {
            Ok(Self)
        }

        fn process_batch(
            &mut self,
            _ctx: &mut GuestContext<'_>,
            _input: InputBatch,
        ) -> Result<(), GuestError> {
            Ok(())
        }
    }

    #[test]
    fn branch_context_round_trips_init_metadata() {
        let init = init(b"tenant=alpha");

        let branch = BranchContext::from(BranchInit::decode(&init.encode()).expect("must decode"));

        assert_eq!(branch.domain_name(), "events");
        assert_eq!(branch.domain_type(), "PACED");
        assert_eq!(branch.branch_key(), Some(b"tenant=alpha".as_slice()));
        assert_eq!(branch.input_schema(), &init.input_schema);
        assert_eq!(branch.output_schemas(), init.output_schemas.as_slice());
    }

    #[test]
    fn a_snapshot_restores_the_application_state_its_processor_saved() {
        let alpha = branch(b"tenant=alpha");

        let saved = alpha.encode_snapshot(saved_state(&Counter { count: 7 }));
        let restored = alpha
            .restore_snapshot::<Counter>(&saved)
            .expect("the snapshot must restore");

        assert_eq!(restored, Counter { count: 7 });
        assert_eq!(
            GuestSnapshot::decode(&saved).expect("the snapshot must decode"),
            GuestSnapshot {
                init_metadata: init(b"tenant=alpha").encode(),
                application_state: 7_u64.to_le_bytes().to_vec(),
            }
        );
    }

    #[test]
    fn a_stateless_processor_restores_from_the_empty_application_state_it_saved() {
        let alpha = branch(b"tenant=alpha");

        let saved = alpha.encode_snapshot(saved_state(&Stateless));
        let snapshot = GuestSnapshot::decode(&saved).expect("the snapshot must decode");
        let restored = alpha
            .restore_snapshot::<Stateless>(&saved)
            .expect("empty application state must restore");

        assert!(snapshot.application_state.is_empty());
        assert_eq!(restored, Stateless);
    }

    #[test]
    fn bytes_that_are_not_a_snapshot_reject_the_snapshot_envelope() {
        let alpha = branch(b"tenant=alpha");

        let rejected = alpha
            .restore_snapshot::<Counter>(b"not a guest snapshot")
            .expect_err("bytes that are not a snapshot must not restore");

        assert!(matches!(rejected, RejectedSnapshot::UndecodableEnvelope(_)));
        assert_eq!(rejected.verdict(), SavedStateRejection::SnapshotEnvelope);
    }

    #[test]
    fn undecodable_init_metadata_rejects_the_snapshot_envelope() {
        let alpha = branch(b"tenant=alpha");
        let saved = GuestSnapshot {
            init_metadata: b"not a branch init".to_vec(),
            application_state: 7_u64.to_le_bytes().to_vec(),
        }
        .encode();

        let rejected = alpha
            .restore_snapshot::<Counter>(&saved)
            .expect_err("a snapshot without decodable init metadata must not restore");

        assert!(matches!(
            rejected,
            RejectedSnapshot::UndecodableInitMetadata(_)
        ));
        assert_eq!(rejected.verdict(), SavedStateRejection::SnapshotEnvelope);
    }

    #[test]
    fn a_snapshot_of_another_branch_configuration_rejects_the_snapshot_envelope() {
        let saved = branch(b"tenant=alpha").encode_snapshot(saved_state(&Counter { count: 7 }));

        let rejected = branch(b"tenant=beta")
            .restore_snapshot::<Counter>(&saved)
            .expect_err("a snapshot of another branch must not restore");

        assert!(matches!(
            rejected,
            RejectedSnapshot::OtherBranchConfiguration
        ));
        assert_eq!(rejected.verdict(), SavedStateRejection::SnapshotEnvelope);
        assert_eq!(
            rejected.to_string(),
            "saved snapshot was taken under a different branch configuration"
        );
    }

    #[test]
    fn application_state_the_processor_refuses_rejects_the_application_state() {
        let alpha = branch(b"tenant=alpha");
        let saved = GuestSnapshot {
            init_metadata: init(b"tenant=alpha").encode(),
            application_state: vec![1, 2, 3],
        }
        .encode();

        let rejected = alpha
            .restore_snapshot::<Counter>(&saved)
            .expect_err("a truncated count must not restore");

        assert_eq!(rejected.verdict(), SavedStateRejection::ApplicationState);
        assert_eq!(rejected.to_string(), "saved count must be exactly 8 bytes");
    }

    #[test]
    fn a_stateless_processor_rejects_application_state_it_never_saves() {
        let alpha = branch(b"tenant=alpha");
        let saved = GuestSnapshot {
            init_metadata: init(b"tenant=alpha").encode(),
            application_state: vec![1, 2, 3],
        }
        .encode();

        let rejected = alpha
            .restore_snapshot::<Stateless>(&saved)
            .expect_err("a stateless processor must not drop saved state silently");

        assert_eq!(rejected.verdict(), SavedStateRejection::ApplicationState);
    }
}
