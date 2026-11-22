//! The relay channel watermarks one peer's protocol owner retains, each for its own channel's
//! retention, and the relay memory they are charged for.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Which channel watermarks one peer retains, when each one's retention ends, the
//!   retired-channel floors at or below which every channel it forgot lies, and the relay memory
//!   charged for them.
//! - **Depends on.** The connection's relay channel identities and watermarks, relay channel
//!   retention and the executor's memory reservations.
//! - **Must not know.** Runtime graphs, branch processing, placement or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        key = "one authenticated peer transport and its process epoch",
        bound = "the relay memory class bounds retained watermarks and floors: each is charged \
                 while it is retained and a grant that would open another watermark waits for \
                 room; only the peer's protocol guard reaches them",
        reason = "the peer owner records, consults and expires channel watermarks under its \
                  protocol guard"
    )
)]

use ahash::{HashMap, HashMapExt as _};
use arch_into::ArchInto as _;
use nervix_execution::Reservation;

use super::*;
use crate::RelayChannelRetention;

/// The channel watermarks one peer's protocol owner retains.
///
/// A watermark is part of what its channel's deliveries need, so nothing forgets one to make room.
/// Each is charged to the relay memory class for as long as it is retained: the grant that opens a
/// channel reserves the charge with the delivery's own memory, and a class without room holds the
/// peer's new channel back, as a capacity refusal its sender retries, instead of costing another
/// channel its position. A watermark keeps the longest retention among the deliveries whose
/// outcomes it recorded, and is forgotten once that retention has passed since it was last
/// recorded or consulted, unless its channel has a batch outstanding.
///
/// Forgetting a channel raises the floor of its retention to that channel's incarnation. A sender
/// process creates its channel incarnations in increasing order, and a forgotten channel was idle
/// for its whole retention, twice the longest timeout its deliveries captured, so every channel its
/// sender created before it whose delivery captured no longer a timeout had ended its delivery,
/// with a margin of that timeout, by then. A request for a channel the set holds nothing for is
/// therefore refused when its incarnation lies at or below the floor of a retention at least as
/// long as the one its own delivery captured: every request for a forgotten channel is, because it
/// concerns one of the deliveries that channel recorded, while an older channel whose delivery
/// captured a longer timeout than a newer forgotten one is not refused for it.
pub(crate) struct RelayChannelWatermarks {
    retained: HashMap<RelayChannelKey, RetainedWatermark>,
    /// The retained channels by when their retention ends, earliest first.
    expiries: BTreeMap<WatermarkExpiry, RelayChannelKey>,
    /// How many retained watermarks keep each retention, so the shortest one the sweep must honour
    /// is known without a scan. It holds one entry per distinct retention, not per watermark.
    retentions: BTreeMap<RelayChannelRetention, usize>,
    next_order: u64,
    /// The retired-channel floors since the set was last cleared: for a retention of channels it
    /// forgot, the greatest incarnation it forgot at that retention. A floor stays only while every
    /// longer retention's floor lies below it, so the floors fall as their retention grows, and the
    /// first floor at or above a retention is the highest floor of every retention at least as long.
    floors: BTreeMap<RelayChannelRetention, [u8; 16]>,
    /// The relay memory every retained watermark and floor is charged for, or `None` while the set
    /// retains neither.
    charge: Option<Reservation>,
}

// A forgotten watermark returns its charge, out of which the floor that forgetting it may add
// keeps its own.
const _: () = assert!(
    size_of::<RelayChannelKey>() + size_of::<RetainedWatermark>()
        > size_of::<RelayChannelRetention>() + size_of::<[u8; 16]>(),
    "a watermark's charge holds the charge of the floor that forgetting it adds"
);

struct RetainedWatermark {
    watermark: RelayChannelWatermark,
    retention: RelayChannelRetention,
    expiry: WatermarkExpiry,
}

/// When a retained watermark's retention ends, with the order it was last recorded or consulted
/// in, so two watermarks whose retention ends at the same instant keep distinct places.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WatermarkExpiry {
    at: Instant,
    order: u64,
}

/// How forgetting a channel changed the floors.
struct FloorRaise {
    /// Whether it set the floor of the channel's retention.
    set: bool,
    /// How many floors, the one it replaced included, the floor it set now covers, and which it
    /// removed.
    removed: usize,
}

impl RelayChannelWatermarks {
    pub(super) fn new() -> Self {
        Self {
            retained: HashMap::new(),
            expiries: BTreeMap::new(),
            retentions: BTreeMap::new(),
            next_order: 0,
            floors: BTreeMap::new(),
            charge: None,
        }
    }

    /// What retaining one watermark is charged to the relay memory class, besides the text of a
    /// rejection reason it keeps: its entry in the channel table and its entry in the expiry order,
    /// each with its own copy of the channel key.
    pub(crate) fn watermark_charge() -> u64 {
        let table_entry = size_of::<RelayChannelKey>() + size_of::<RetainedWatermark>();
        let expiry_entry = size_of::<WatermarkExpiry>() + size_of::<RelayChannelKey>();
        let bytes: u64 = (table_entry + expiry_entry).arch_into();
        bytes
    }

    /// What one retired-channel floor is charged to the relay memory class: its retention and
    /// incarnation.
    pub(crate) fn floor_charge() -> u64 {
        let bytes: u64 = (size_of::<RelayChannelRetention>() + size_of::<[u8; 16]>()).arch_into();
        bytes
    }

    pub(super) fn len(&self) -> usize {
        self.retained.len()
    }

    /// The shortest retention a retained watermark keeps, or `None` while none is retained.
    pub(super) fn shortest_retention(&self) -> Option<RelayChannelRetention> {
        let shortest = self.retentions.keys().next()?;
        Some(*shortest)
    }

    /// The watermark retained for `channel`, without counting this as a consultation.
    pub(super) fn get(&self, channel: &RelayChannelKey) -> Option<&RelayChannelWatermark> {
        let retained = self.retained.get(channel)?;
        Some(&retained.watermark)
    }

    /// Consult the watermark retained for `channel` at `now`: its retention starts again then.
    pub(super) fn consult(
        &mut self,
        channel: &RelayChannelKey,
        now: Instant,
    ) -> Option<&RelayChannelWatermark> {
        let order = self.take_order();
        let retained = self.retained.get_mut(channel)?;
        self.expiries.remove(&retained.expiry);
        retained.expiry = WatermarkExpiry::after(now, retained.retention, order);
        self.expiries.insert(retained.expiry, channel.clone());
        Some(&retained.watermark)
    }

    /// Record `status` as the outcome at `attempt`'s position at `now`, unless its channel already
    /// retains a later one. `retention` is the one the outcome's delivery captured: the watermark
    /// keeps the longest retention of the deliveries it recorded, from `now`.
    ///
    /// `note` is the charge the delivery's grant, or the cancellation that fenced its position,
    /// reserved for its channel's watermark. A channel the set retains nothing for takes it; a
    /// channel whose watermark the set already retains holds its charge, so a `note` given for it
    /// returns to the relay memory class, and an attempt registered while that watermark was
    /// retained gives none.
    pub(super) fn record(
        &mut self,
        attempt: &RelayAttemptKey,
        status: RelayAdmissionStatus,
        retention: RelayChannelRetention,
        note: Option<Reservation>,
        now: Instant,
    ) {
        let order = self.take_order();
        let Some(retained) = self.retained.get(&attempt.channel) else {
            let expiry = WatermarkExpiry::after(now, retention, order);
            let note = note.verified(
                "an attempt gives up its watermark's charge only while its channel's watermark is \
                 kept, and an outstanding attempt keeps that watermark from expiring",
            );
            self.take_charge(note);
            let watermark = RelayChannelWatermark::new(attempt.sequence, self.charged(status));
            self.keep_retention(retention);
            self.expiries.insert(expiry, attempt.channel.clone());
            self.retained.insert(
                attempt.channel.clone(),
                RetainedWatermark {
                    watermark,
                    retention,
                    expiry,
                },
            );
            return;
        };
        if attempt.sequence < retained.watermark.sequence {
            return;
        }
        let replaced_reason = Self::reason_bytes(&retained.watermark.status);
        let replaced_retention = retained.retention;
        let replaced_expiry = retained.expiry;
        // A request about any delivery the channel recorded can still arrive while its retention
        // lasts, and a forgotten channel's floor must refuse it, so the watermark keeps the longest.
        let retention = replaced_retention.max(retention);
        let expiry = WatermarkExpiry::after(now, retention, order);
        self.release(replaced_reason);
        let watermark = RelayChannelWatermark::new(attempt.sequence, self.charged(status));
        self.forget_retention(replaced_retention);
        self.keep_retention(retention);
        self.expiries.remove(&replaced_expiry);
        self.expiries.insert(expiry, attempt.channel.clone());
        let retained = self
            .retained
            .get_mut(&attempt.channel)
            .verified("the channel's watermark was found above under the same guard");
        *retained = RetainedWatermark {
            watermark,
            retention,
            expiry,
        };
    }

    /// Whether a request about a delivery on `channel` that captured `retention` names a channel
    /// the set forgot, or one its sender created before a channel the set forgot after keeping it
    /// at least as long: the set retains nothing for it, and its incarnation lies at or below the
    /// floor of a retention at least `retention`.
    pub(super) fn is_forgotten(
        &self,
        channel: &RelayChannelKey,
        retention: RelayChannelRetention,
    ) -> bool {
        if self.retained.contains_key(channel) {
            return false;
        }
        // The floors fall as their retention grows, so the first one at or above `retention` is
        // the highest of them.
        let Some((_, floor)) = self.floors.range(retention..).next() else {
            return false;
        };
        channel.channel_incarnation <= *floor
    }

    /// Forget every watermark whose retention has ended by `now`, except those of the channels
    /// `in_use` reports an attempt outstanding on, and return their charge to the relay memory
    /// class.
    ///
    /// The scan stops at the first watermark whose retention has not ended, because every later
    /// one in the expiry order ends later. Before that it passes over at most one retained
    /// watermark per outstanding attempt.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the peer owner supplies a synchronous lookup of its own \
                                   attempts under the protocol guard it already holds; its \
                                   effects belong to that owner")
    )]
    pub(super) fn expire(&mut self, now: Instant, in_use: impl Fn(&RelayChannelKey) -> bool) {
        let mut expired = Vec::new();
        for (expiry, channel) in &self.expiries {
            if expiry.at > now {
                break;
            }
            if !in_use(channel) {
                expired.push(*expiry);
            }
        }
        for expiry in expired {
            let channel = self
                .expiries
                .remove(&expiry)
                .verified("the expiry scan above read this place");
            let retained = self
                .retained
                .remove(&channel)
                .verified("every place in the expiry order names a retained watermark");
            self.forget_retention(retained.retention);
            let reason = Self::reason_bytes(&retained.watermark.status);
            let watermark = Self::watermark_charge()
                .checked_add(reason)
                .verified("a retained watermark's charge fits the relay memory class");
            let raise = self.raise_floor(&channel, retained.retention);
            // The floors the raise removed return their charge with the watermark's, and the floor
            // it set keeps its own out of the watermark's.
            let removed: u64 = raise.removed.arch_into();
            let removed_floors = Self::floor_charge()
                .checked_mul(removed)
                .verified("the removed floors were charged within the relay memory class");
            let mut released = watermark
                .checked_add(removed_floors)
                .verified("the set held the charge of the watermark and floors it releases");
            if raise.set {
                released = released.checked_sub(Self::floor_charge()).assured(
                    "a watermark's charge exceeds a floor's, as the assertion beside the set \
                     states",
                );
            }
            self.release(released);
        }
    }

    /// Forget every watermark and floor, as when the peer binds another process epoch, and return
    /// their whole charge to the relay memory class.
    pub(super) fn clear(&mut self) {
        self.retained.clear();
        self.expiries.clear();
        self.retentions.clear();
        self.floors.clear();
        self.charge = None;
    }

    fn take_order(&mut self) -> u64 {
        let order = self.next_order;
        self.next_order = order
            .checked_add(1)
            .assured("a peer cannot record or consult 2^64 channel watermarks");
        order
    }

    /// The rejection reason's text a retained `status` keeps, which its watermark is charged for on
    /// top of its own entries.
    fn reason_bytes(status: &RelayAdmissionStatus) -> u64 {
        let RelayAdmissionStatus::Rejected(reason) = status else {
            return 0;
        };
        reason.capacity().arch_into()
    }

    /// Add `note`, the charge of a watermark the set now retains, to the set's charge.
    fn take_charge(&mut self, note: Reservation) {
        let charge = match self.charge.take() {
            Some(charge) => charge
                .merge(note)
                .verified("one memory class never charges more than its capacity, which fits u32"),
            None => note,
        };
        self.charge = Some(charge);
    }

    /// Charge `status`'s rejection reason on top of the set's charge. When the relay memory class
    /// has no room for that text, the watermark keeps the rejection without it: the outcome is
    /// what reconciling the channel needs, and its reason only explains it.
    fn charged(&mut self, status: RelayAdmissionStatus) -> RelayAdmissionStatus {
        let reason = Self::reason_bytes(&status);
        if reason == 0 {
            return status;
        }
        let charge = self
            .charge
            .as_mut()
            .verified("a retained watermark's own charge is held before its reason is charged");
        let Some(target) = charge.bytes().checked_add(reason) else {
            return RelayAdmissionStatus::Rejected(String::new());
        };
        match charge.grow_to(target) {
            Ok(()) => status,
            Err(refusal) => {
                debug!(
                    error = %format_args!("{refusal:#}"),
                    "the relay memory class has no room for a rejection reason; the channel's \
                     watermark keeps the rejection without its text"
                );
                RelayAdmissionStatus::Rejected(String::new())
            }
        }
    }

    /// Return `bytes` of the set's charge to the relay memory class.
    fn release(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let charge = self
            .charge
            .take()
            .verified("the set holds the charge of every watermark it retains");
        let (released, kept) = charge
            .split(bytes)
            .verified("the set holds the charge of every watermark it retains");
        drop(released);
        if kept.bytes() > 0 {
            self.charge = Some(kept);
        }
    }

    fn keep_retention(&mut self, retention: RelayChannelRetention) {
        let count = self.retentions.entry(retention).or_insert(0);
        *count = count
            .checked_add(1)
            .assured("a peer cannot retain usize::MAX watermarks");
    }

    fn forget_retention(&mut self, retention: RelayChannelRetention) {
        let count = self
            .retentions
            .get_mut(&retention)
            .verified("every retained watermark's retention is counted");
        *count = count
            .checked_sub(1)
            .verified("every retained watermark's retention is counted");
        if *count == 0 {
            self.retentions.remove(&retention);
        }
    }

    /// Raise the floor of `retention`, the forgotten `channel`'s, to its incarnation, unless the
    /// floor of a retention at least as long already lies at or above it. The floors of retentions
    /// no longer than `retention` that lie at or below the raised one refuse nothing it does not,
    /// and go.
    fn raise_floor(
        &mut self,
        channel: &RelayChannelKey,
        retention: RelayChannelRetention,
    ) -> FloorRaise {
        let incarnation = channel.channel_incarnation;
        if let Some((_, floor)) = self.floors.range(retention..).next()
            && *floor >= incarnation
        {
            return FloorRaise {
                set: false,
                removed: 0,
            };
        }
        // Below `retention` the floors rise as their retention falls, so the ones the raised floor
        // covers are the run that ends at the first floor above it.
        let mut covered = Vec::new();
        for (covered_retention, floor) in self.floors.range(..=retention).rev() {
            if *floor > incarnation {
                break;
            }
            covered.push(*covered_retention);
        }
        for covered_retention in &covered {
            self.floors.remove(covered_retention);
        }
        self.floors.insert(retention, incarnation);
        FloorRaise {
            set: true,
            removed: covered.len(),
        }
    }
}

impl WatermarkExpiry {
    /// The end of `retention` counted from `now`, at its place in the recording order.
    fn after(now: Instant, retention: RelayChannelRetention, order: u64) -> Self {
        Self {
            at: now.checked_add(retention.duration()).assured(
                "a validated retention is at most twice u32::MAX seconds, which the monotonic \
                 clock holds",
            ),
            order,
        }
    }
}

#[cfg(test)]
impl RelayChannelWatermarks {
    /// The relay memory the set holds charged for its watermarks.
    pub(super) fn charged_bytes(&self) -> u64 {
        match &self.charge {
            Some(charge) => charge.bytes(),
            None => 0,
        }
    }

    /// What the set's watermarks and floors call for: each watermark's entries and the rejection
    /// reason it keeps, and each floor's entry.
    pub(super) fn charge_owed(&self) -> u64 {
        let mut owed = 0_u64;
        for retained in self.retained.values() {
            let reason = Self::reason_bytes(&retained.watermark.status);
            let charge = Self::watermark_charge()
                .checked_add(reason)
                .assured("a test watermark's charge fits u64 bytes");
            owed = owed
                .checked_add(charge)
                .assured("a test's watermarks fit u64 bytes");
        }
        let floors: u64 = self.floors.len().arch_into();
        let floors = Self::floor_charge()
            .checked_mul(floors)
            .assured("a test's floors fit u64 bytes");
        owed.checked_add(floors)
            .assured("a test's watermarks and floors fit u64 bytes")
    }

    /// The retired-channel floors, by retention.
    pub(super) fn floors(&self) -> &BTreeMap<RelayChannelRetention, [u8; 16]> {
        &self.floors
    }
}

#[cfg(test)]
#[path = "relay_watermarks_tests.rs"]
mod tests;
