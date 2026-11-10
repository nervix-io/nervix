//! Receiver-owned relay, bulk chunk and flow-control capacity settings.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Peer receive setting definitions and their executor-relative limits, and the
//!   HTTP/2 bound that window and the connection window derived from it must fit.
//! - **Depends on.** The vocabulary's setting classes and byte quantities, and the bulk pool's
//!   stream partition.
//! - **Must not know.** Command-line parsing, or the memory budget that backs each window.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_execution::OperationLimits;
use nervix_models::{NodeCapabilities, PeerReceiveBounds, Setting, SettingDefinition};
use thiserror::Error;
use ubyte::ByteUnit;

use crate::PoolClass;

mod deadlines;
pub use deadlines::*;
mod liveness;
mod relay;
pub use relay::*;
mod transport;
pub use liveness::*;
pub use transport::*;

/// Why the receiver cannot enforce an advertised capacity claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PeerReceiveSettingsError {
    #[error("{setting} must be greater than zero")]
    Empty { setting: &'static str },
    #[error("{setting} must be greater than zero and at most the executor's {maximum} bytes")]
    Operation { setting: &'static str, maximum: u64 },
    #[error("the bulk receive window cannot be advertised")]
    Window,
}

/// Local requests retained until startup has resolved the executor's actual operation limits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerReceiveOverrides {
    pub relay_batch_bytes: Option<ByteUnit>,
    pub relay_decoded_bytes: Option<ByteUnit>,
    pub bulk_chunk_bytes: Option<ByteUnit>,
    pub bulk_window_bytes: Option<ByteUnit>,
}

impl PeerReceiveOverrides {
    /// Checks relationships that need no detected capacity, before startup performs I/O.
    pub fn validate(&self) -> Result<(), Report<PeerReceiveSettingsError>> {
        for (setting, value) in [
            (RelayBatchBytes::NAME, self.relay_batch_bytes),
            (RelayDecodedBytes::NAME, self.relay_decoded_bytes),
            (BulkChunkBytes::NAME, self.bulk_chunk_bytes),
        ] {
            if let Some(value) = value
                && value.as_u64() == 0
            {
                return Err(Report::new(PeerReceiveSettingsError::Empty { setting }));
            }
        }
        BulkReceiveWindow::resolve(self.bulk_window_bytes)
            .map_err(|error| error.change_context(PeerReceiveSettingsError::Window))?;
        Ok(())
    }

    /// Resolve omitted bounds from the final executor while preserving every explicit request.
    pub fn resolve(
        &self,
        limits: &OperationLimits,
    ) -> Result<PeerReceiveSettings, Report<PeerReceiveSettingsError>> {
        self.validate()?;
        PeerReceiveSettings::resolve(
            self.relay_batch_bytes,
            self.relay_decoded_bytes,
            self.bulk_chunk_bytes,
            self.bulk_window_bytes,
            limits,
        )
    }
}

macro_rules! operation_bound {
    ($definition:ident, $name:literal, $field:ident) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = ByteUnit;
            type Context = OperationLimits;
            type Error = PeerReceiveSettingsError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> =
                Some("the executor's corresponding operation limit");
            fn default_value(limits: &OperationLimits) -> ByteUnit {
                limits.$field
            }
            fn validate(
                value: &ByteUnit,
                limits: &OperationLimits,
            ) -> Result<(), Report<Self::Error>> {
                if value.as_u64() == 0 || *value > limits.$field {
                    return Err(Report::new(PeerReceiveSettingsError::Operation {
                        setting: Self::NAME,
                        maximum: limits.$field.as_u64(),
                    }));
                }
                Ok(())
            }
        }
    };
}

operation_bound!(
    RelayBatchBytes,
    "interconnect-relay-batch-bytes",
    relay_encoded_bytes
);
operation_bound!(
    RelayDecodedBytes,
    "interconnect-relay-decoded-bytes",
    relay_decoded_bytes
);
operation_bound!(
    BulkChunkBytes,
    "interconnect-bulk-chunk-bytes",
    bulk_chunk_bytes
);

/// The receiver's declared bounds, validated independently of public client admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerReceiveSettings {
    pub relay_batch_bytes: Setting<RelayBatchBytes>,
    pub relay_decoded_bytes: Setting<RelayDecodedBytes>,
    pub bulk_chunk_bytes: Setting<BulkChunkBytes>,
    pub bulk_window_bytes: Setting<BulkReceiveWindow>,
}

impl PeerReceiveSettings {
    pub fn resolve(
        relay: Option<ByteUnit>,
        decoded: Option<ByteUnit>,
        chunk: Option<ByteUnit>,
        window: Option<ByteUnit>,
        limits: &OperationLimits,
    ) -> Result<Self, Report<PeerReceiveSettingsError>> {
        Ok(Self {
            relay_batch_bytes: Setting::resolve(relay, limits)?,
            relay_decoded_bytes: Setting::resolve(decoded, limits)?,
            bulk_chunk_bytes: Setting::resolve(chunk, limits)?,
            bulk_window_bytes: BulkReceiveWindow::resolve(window)
                .map_err(|error| error.change_context(PeerReceiveSettingsError::Window))?,
        })
    }

    pub fn bounds(&self) -> PeerReceiveBounds {
        use std::num::NonZeroU64;

        use meticulous::OptionExt as _;
        PeerReceiveBounds {
            relay_batch_bytes: NonZeroU64::new(self.relay_batch_bytes.value().as_u64())
                .assured("validated positive relay bound"),
            relay_decoded_bytes: NonZeroU64::new(self.relay_decoded_bytes.value().as_u64())
                .assured("validated positive decoded bound"),
            bulk_chunk_bytes: NonZeroU64::new(self.bulk_chunk_bytes.value().as_u64())
                .assured("validated positive chunk bound"),
            bulk_window_bytes: NonZeroU64::new(self.bulk_window_bytes.value().as_u64())
                .assured("validated positive window"),
        }
    }
}

impl Default for PeerReceiveSettings {
    fn default() -> Self {
        Self::resolve(None, None, None, None, &OperationLimits::default())
            .assured("default receive settings fit their operation limits")
    }
}

/// The largest flow-control window HTTP/2 can advertise, for one stream or a whole connection.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 caps every flow-control window at 2^31-1 octets"
    )
)]
const MAX_HTTP2_WINDOW_BYTES: u64 = 2_147_483_647;

/// Why a bulk receive window cannot be advertised.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BulkReceiveWindowError {
    #[error("{name} must be greater than zero", name = BulkReceiveWindow::NAME)]
    Empty,
    #[error(
        "{name} ({window}) for each of the {streams} streams of a bulk connection exceeds the \
         {maximum}-byte window HTTP/2 can advertise for the connection",
        name = BulkReceiveWindow::NAME
    )]
    AboveHttp2Window {
        window: ByteUnit,
        streams: u64,
        maximum: u64,
    },
}

/// The bulk data a node accepts ahead of one reader: the HTTP/2 receive window each stream of the
/// node's bulk connections grants the peer that answers it.
///
/// Resource archives, runtime checkpoints and snapshots arrive as bulk responses. A sender moves
/// at most one window of a response per round trip, and on a loaded node every round trip waits
/// for several scheduled tasks on both sides, so a window of a few chunks paces a large response
/// by round trips rather than by bandwidth. The connection grants every one of its streams this
/// window at once, and each stream backs its window with a reservation for as long as it is open.
pub struct BulkReceiveWindow;

impl SettingDefinition for BulkReceiveWindow {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = BulkReceiveWindowError;

    const NAME: &'static str = "interconnect-bulk-receive-window";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Mebibyte(1)
    }

    fn validate(window: &ByteUnit, _: &()) -> Result<(), Report<BulkReceiveWindowError>> {
        if window.as_u64() == 0 {
            return Err(Report::new(BulkReceiveWindowError::Empty));
        }
        let streams = u64::try_from(PoolClass::Bulk.stream_slots_per_connection())
            .assured("a bulk connection carries a handful of streams");
        let refusal = || {
            Report::new(BulkReceiveWindowError::AboveHttp2Window {
                window: *window,
                streams,
                maximum: MAX_HTTP2_WINDOW_BYTES,
            })
        };
        let Some(connection) = window.as_u64().checked_mul(streams) else {
            return Err(refusal());
        };
        if connection > MAX_HTTP2_WINDOW_BYTES {
            return Err(refusal());
        }
        Ok(())
    }
}

impl BulkReceiveWindow {
    /// Resolve the default or a local capacity claim through the same validation boundary.
    pub fn resolve(
        value: Option<ByteUnit>,
    ) -> Result<Setting<Self>, Report<BulkReceiveWindowError>> {
        Setting::resolve(value, &())
    }
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;

    #[test]
    fn omitted_bounds_follow_the_final_executor_but_explicit_defaults_stay_explicit() {
        let limits = OperationLimits {
            relay_encoded_bytes: ByteUnit::Mebibyte(4),
            ..Default::default()
        };
        let derived = PeerReceiveOverrides::default()
            .resolve(&limits)
            .expect("omitted bounds fit the final limits");
        assert_eq!(
            derived.relay_batch_bytes.value().as_u64(),
            limits.relay_encoded_bytes.as_u64()
        );
        let explicit = PeerReceiveOverrides {
            relay_batch_bytes: Some(OperationLimits::default().relay_encoded_bytes),
            ..Default::default()
        };
        let refused = explicit
            .resolve(&limits)
            .expect_err("an explicit 32 MiB bound cannot fit a four MiB executor");
        assert!(matches!(
            refused.current_context(),
            PeerReceiveSettingsError::Operation { .. }
        ));
    }

    #[test]
    fn the_default_window_grants_each_bulk_stream_one_mebibyte() {
        let window = BulkReceiveWindow::resolve(None).assured("the default window is valid");
        assert_eq!(*window.value(), ByteUnit::Mebibyte(1));
    }

    #[test]
    fn a_window_must_fit_http2_for_every_stream_of_its_connection() {
        let streams = u64::try_from(PoolClass::Bulk.stream_slots_per_connection())
            .assured("a bulk connection carries a handful of streams");
        let largest = MAX_HTTP2_WINDOW_BYTES
            .checked_div(streams)
            .assured("a bulk connection carries at least one stream");
        let accepted = BulkReceiveWindow::resolve(Some(ByteUnit::Byte(largest)))
            .assured("the largest window whose connection window fits HTTP/2 is valid");
        assert_eq!(accepted.value().as_u64(), largest);

        let above = ByteUnit::Byte(
            largest
                .checked_add(1)
                .assured("one byte above the largest window fits in u64"),
        );
        let error = BulkReceiveWindow::resolve(Some(above))
            .err()
            .assured("a window whose connection window exceeds HTTP/2 is refused");
        assert_eq!(
            error.current_context(),
            &BulkReceiveWindowError::AboveHttp2Window {
                window: above,
                streams,
                maximum: MAX_HTTP2_WINDOW_BYTES,
            }
        );

        let empty = BulkReceiveWindow::resolve(Some(ByteUnit::Byte(0)))
            .err()
            .assured("an empty window is refused");
        assert_eq!(empty.current_context(), &BulkReceiveWindowError::Empty);
    }
}
