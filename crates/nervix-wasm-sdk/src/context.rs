use std::time::Duration;

use nervix_wasm_protocol::{BranchInit, ProcessorSchema};

use crate::{abi, envelope::OutputEnvelope, error::GuestError};

/// Branch-instance configuration decoded from the host `BranchInit` payload.
///
/// One guest instance exists per concrete branch, so everything here is
/// branch-local by construction.
#[derive(Debug, Clone)]
pub struct BranchContext {
    init: BranchInit,
}

impl BranchContext {
    pub(crate) fn from_init_metadata(metadata: &[u8]) -> Result<Self, GuestError> {
        Ok(Self {
            init: BranchInit::decode(metadata)?,
        })
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
    pub(crate) last_domain_time_nanos: &'rt mut i64,
    pub(crate) last_timeout_handle: &'rt mut i64,
}

impl GuestContext<'_> {
    pub fn branch(&self) -> &BranchContext {
        self.branch
    }

    /// Reads the current domain-clock time through the host import.
    pub fn domain_time(&mut self) -> DomainTime {
        let now = abi::host_domain_time_nanos();
        *self.last_domain_time_nanos = now;
        DomainTime::from_unix_nanos(now)
    }

    /// Requests a domain-clock timeout; the host later invokes the
    /// processor's `on_timeout` with the returned handle.
    pub fn request_timeout(&mut self, delay: Duration) -> Result<TimeoutHandle, GuestError> {
        let delay_nanos = i64::try_from(delay.as_nanos()).map_err(|_| GuestError::InvalidSize)?;
        let handle = abi::host_timeout_after_nanos(delay_nanos);
        if handle < 0 {
            return Err(GuestError::InvalidSize);
        }
        *self.last_timeout_handle = handle;
        Ok(TimeoutHandle::new(handle))
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
    /// error state.
    pub fn report_global_error(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        self.global_error.clear();
        self.global_error.extend_from_slice(reason.as_bytes());
        *self.error_state = Some(reason);
    }
}

#[cfg(test)]
mod tests {
    use nervix_wasm_protocol::{ProcessorField, ProcessorType};

    use super::*;

    #[test]
    fn branch_context_round_trips_init_metadata() {
        let init = BranchInit {
            domain_name: "events".to_string(),
            domain_type: "PACED".to_string(),
            branch_key: Some(b"tenant=alpha".to_vec()),
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
        };

        let branch = BranchContext::from_init_metadata(&init.encode()).expect("must decode");

        assert_eq!(branch.domain_name(), "events");
        assert_eq!(branch.domain_type(), "PACED");
        assert_eq!(branch.branch_key(), Some(b"tenant=alpha".as_slice()));
        assert_eq!(branch.input_schema(), &init.input_schema);
        assert_eq!(branch.output_schemas(), init.output_schemas.as_slice());
    }
}
