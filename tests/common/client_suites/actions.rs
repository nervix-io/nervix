//! The host suite's declared scenario actions and their ordered confirmations.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Parsing bounded action requests, enforcing their sequence, and confirming only
//!   actions the scenario performed, within the suite's existing deadline.
//! - **Depends on.** Tokio I/O traits and the primitive boundary's deadline.
//! - **Must not know.** The cluster, fault injection implementation, host language or suite tests.

use std::{io, num::NonZeroU64};

use nervix_primitives::time::{Instant, timeout_at};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

const REQUEST_PREFIX: &str = "NERVIX-SUITE-ACTION";
const ACTION_NAME: &str = "refuse-next-subscription-opening";
const CLEAR_ACTION_NAME: &str = "clear-subscription-opening-refusals";
const CLOCK_ACTION_NAME: &str = "refuse-next-clock-attachment";
const CLEAR_CLOCK_ACTION_NAME: &str = "clear-clock-attachment-refusals";

/// A testing-build action the scenario explicitly supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SuiteAction {
    RefuseNextSubscriptionOpening,
    ClearSubscriptionOpeningRefusals,
    RefuseNextClockAttachment,
    ClearClockAttachmentRefusals,
}

impl SuiteAction {
    fn parse(name: &str) -> io::Result<Self> {
        match name {
            ACTION_NAME => Ok(Self::RefuseNextSubscriptionOpening),
            CLEAR_ACTION_NAME => Ok(Self::ClearSubscriptionOpeningRefusals),
            CLOCK_ACTION_NAME => Ok(Self::RefuseNextClockAttachment),
            CLEAR_CLOCK_ACTION_NAME => Ok(Self::ClearClockAttachmentRefusals),
            _ => Err(io::Error::other(format!("unknown suite action: {name}"))),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::RefuseNextSubscriptionOpening => ACTION_NAME,
            Self::ClearSubscriptionOpeningRefusals => CLEAR_ACTION_NAME,
            Self::RefuseNextClockAttachment => CLOCK_ACTION_NAME,
            Self::ClearClockAttachmentRefusals => CLEAR_CLOCK_ACTION_NAME,
        }
    }
}

/// One runner's sequential requests, starting at one for its entire cluster tier.
pub(crate) struct SuiteActions {
    next: Option<NonZeroU64>,
}

impl SuiteActions {
    pub(crate) fn new() -> Self {
        Self {
            next: Some(NonZeroU64::MIN),
        }
    }

    /// Returns false for ordinary output. An invalid request or unconfirmed action ends the run.
    pub(crate) async fn handle<W: AsyncWrite + Unpin>(
        &mut self,
        line: &str,
        input: &mut W,
        deadline: Instant,
        perform: &mut impl FnMut(SuiteAction) -> io::Result<()>,
    ) -> io::Result<bool> {
        if !line.starts_with(REQUEST_PREFIX) {
            return Ok(false);
        }
        // The line reader strips its newline; the complete request is at most 256 ASCII bytes.
        if line.len() >= 256 || !line.is_ascii() {
            return Err(io::Error::other(
                "suite action requests must fit in 256 ASCII bytes",
            ));
        }
        let mut fields = line.split(' ');
        let prefix = fields.next();
        let sequence = fields.next();
        let action = fields.next();
        if prefix != Some(REQUEST_PREFIX) || fields.next().is_some() {
            return Err(io::Error::other(format!(
                "unknown or malformed suite action: {line}"
            )));
        }
        let action = SuiteAction::parse(
            action.ok_or_else(|| io::Error::other("a suite action needs its name"))?,
        )?;
        let sequence = sequence
            .ok_or_else(|| io::Error::other("a suite action needs its sequence"))?
            .parse::<NonZeroU64>()
            .map_err(io::Error::other)?;
        if self.next != Some(sequence) {
            return Err(io::Error::other(format!(
                "suite action sequence {sequence} does not match {:?}",
                self.next
            )));
        }
        perform(action)?;
        let name = action.name();
        let confirmation = format!("NERVIX-SUITE-CONFIRMED {sequence} {name}\n");
        timeout_at(deadline, async {
            input.write_all(confirmation.as_bytes()).await?;
            input.flush().await
        })
        .await
        .map_err(|_| {
            io::Error::other("the suite action confirmation exceeded the run deadline")
        })??;
        self.next = sequence.checked_add(1);
        Ok(true)
    }
}
