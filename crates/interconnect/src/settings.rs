//! The receive window a node grants the streams of its bulk connections, declared as a setting.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The setting definition of the bulk receive window, its single default, and the
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
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use thiserror::Error;
use ubyte::ByteUnit;

use crate::PoolClass;

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
    /// The window a node grants when nothing overrides it.
    pub(crate) fn default_window() -> Setting<Self> {
        Self::resolve(None).assured("the default bulk receive window satisfies its own validation")
    }

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
