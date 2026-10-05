//! Complete current frame values built from bounded, replayable property inputs.
//!
//! Layer: test harness.
//! - **Owns.** Current wire generators, complete equality assertions and schema coverage checks.
//! - **Depends on.** The production wire API and vocabulary generators.
//! - **Must not know.** Runtime Arrow columns, session dispatch, or live services.

use std::{num::NonZeroU32, time::Duration};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    CommandExecutionReference, DomainClockObservation, DomainClockObservedState, DomainClockState,
    DomainTimeRate, PacedDomainClock, Timestamp,
};

use super::fixtures::reference;
use crate::{
    Diagnostic, LeaderEndpoints, LeaderRedirect, RequestId, SourceSpan, SubscriptionHandle,
};

mod events;
mod io;
mod malformed;
mod metadata;
mod replies;
mod requests;
mod rows;
mod streams;
mod wasm;

pub(super) struct WireValues<'a> {
    arbitrary: Arbitrary<'a>,
}

impl<'a> WireValues<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self {
            arbitrary: Arbitrary::new(bytes, Domain::Vocabulary),
        }
    }

    pub(super) fn request_id(&mut self) -> RequestId {
        RequestId::new(self.arbitrary.positive_u64())
    }

    pub(super) fn reference(&mut self) -> CommandExecutionReference {
        reference(&self.arbitrary.name_text())
    }

    pub(super) fn bytes(&mut self, minimum: usize) -> Bytes {
        let count = minimum
            .checked_add(self.arbitrary.entropy().count(128))
            .assured("the minimum is at most one and the payload has at most 129 bytes");
        let mut bytes = Vec::with_capacity(count);
        for _ in 0..count {
            bytes.push(self.arbitrary.entropy().byte());
        }
        Bytes::from(bytes)
    }

    pub(super) fn digest<const N: usize>(&mut self) -> [u8; N] {
        std::array::from_fn(|_| self.arbitrary.entropy().byte())
    }

    pub(super) fn count32(&mut self) -> NonZeroU32 {
        let count = self
            .arbitrary
            .entropy()
            .boundary_biased(1..=u64::from(u32::MAX));
        NonZeroU32::new(u32::try_from(count).verified("the range ends at u32::MAX"))
            .verified("the range starts at one")
    }

    pub(super) fn duration(&mut self) -> Duration {
        Duration::from_nanos(self.arbitrary.positive_u64().get())
    }

    pub(super) fn timestamp(&mut self) -> Timestamp {
        Timestamp::from_unix_nanos(self.arbitrary.entropy().any_i64())
    }

    pub(super) fn subscription(&mut self) -> SubscriptionHandle {
        SubscriptionHandle {
            name: self.arbitrary.name(),
            generation: self.arbitrary.positive_u64(),
        }
    }

    pub(super) fn redirect(&mut self) -> LeaderRedirect {
        let leader = if self.arbitrary.entropy().flag() {
            Some(LeaderEndpoints {
                node: self.arbitrary.name(),
                grpc_uri: self.arbitrary.entropy().pick([
                    None,
                    Some(
                        url::Url::parse("https://node.example:7443/path?q=é")
                            .assured("a literal URL"),
                    ),
                ]),
                web_console_uri: self.arbitrary.entropy().pick([
                    None,
                    Some(
                        url::Url::parse("https://node.example:7440/console/")
                            .assured("a literal URL"),
                    ),
                ]),
            })
        } else {
            None
        };
        LeaderRedirect { leader }
    }

    pub(super) fn diagnostics(&mut self) -> Vec<Diagnostic> {
        let count = self.arbitrary.entropy().count(3);
        let mut diagnostics = Vec::with_capacity(count);
        for _ in 0..count {
            let span = if self.arbitrary.entropy().flag() {
                let start = u32::try_from(
                    self.arbitrary
                        .entropy()
                        .boundary_biased(0..=u64::from(u32::MAX)),
                )
                .verified("a bounded u32 position");
                let end = u32::try_from(
                    self.arbitrary
                        .entropy()
                        .between(u64::from(start)..=u64::from(u32::MAX)),
                )
                .verified("a bounded u32 position");
                Some(SourceSpan::new(start, end).verified("end was drawn at or above start"))
            } else {
                None
            };
            diagnostics.push(Diagnostic {
                message: self.arbitrary.string(),
                span,
            });
        }
        diagnostics
    }

    pub(super) fn clock(&mut self, variant: u8) -> DomainClockObservation {
        let state = match variant % 4 {
            0 => DomainClockObservedState::Stopped,
            1 => DomainClockObservedState::Uninstalled,
            2 => DomainClockObservedState::Unpaced,
            _ => {
                // Every positive finite bit pattern is reachable; never filter invalid rates.
                let bits = self
                    .arbitrary
                    .entropy()
                    .boundary_biased(1..=0x7fef_ffff_ffff_ffff);
                let rate = DomainTimeRate::try_from(f64::from_bits(bits))
                    .verified("these bits encode a positive finite float");
                let mapping = DomainClockState::new(self.timestamp(), self.timestamp(), rate);
                DomainClockObservedState::Paced(PacedDomainClock {
                    period: self.arbitrary.clock_period(),
                    skew: self.arbitrary.clock_skew(),
                    mapping,
                })
            }
        };
        DomainClockObservation {
            generation: self.arbitrary.entropy().any_u64(),
            state,
        }
    }
}
