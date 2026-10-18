//! The bounded set of relay channel watermarks one peer's protocol owner retains.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Which channel watermarks one peer retains, the order they were last recorded or
//!   consulted in, and the floor at or below which every channel it forgot lies.
//! - **Depends on.** The connection's relay channel identities and watermarks.
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
        bound = "the configured watermark capacity bounds retained watermarks; only the peer's \
                 protocol guard reaches them",
        reason = "the peer owner records, consults, forgets and expires channel watermarks under \
                  its protocol guard"
    )
)]

use ahash::{HashMap, HashMapExt as _};

use super::*;

/// The channel watermarks one peer's protocol owner retains: at most `capacity` of them.
///
/// Recording the watermark of another channel when the set is full first forgets the watermark
/// recorded or consulted least recently among the channels with no attempt outstanding, so a new
/// channel never waits for a position. Its capacity exceeds the attempts a peer may hold, and a
/// channel holds at most one attempt, so a full set always holds such a watermark.
///
/// Forgetting a channel, whether for room or because its retention passed, raises the set's floor
/// to that channel's incarnation. A sender process creates its channel incarnations in increasing
/// order, so every channel the set forgot is at or below the floor, and a channel at or below the
/// floor that the set holds nothing for is never taken for a new one.
pub(super) struct RelayChannelWatermarks {
    capacity: usize,
    retained: HashMap<RelayChannelKey, RetainedWatermark>,
    /// The retained channels by the position they were last recorded or consulted at, least recent
    /// first. Positions only increase and every recording or consultation takes the time with its
    /// position, so this is also the order of their reconciliation times.
    order: BTreeMap<u64, RelayChannelKey>,
    next_position: u64,
    /// The greatest incarnation of a channel this set forgot since it was last cleared.
    floor: Option<[u8; 16]>,
}

struct RetainedWatermark {
    watermark: RelayChannelWatermark,
    position: u64,
}

impl RelayChannelWatermarks {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            retained: HashMap::new(),
            order: BTreeMap::new(),
            next_position: 0,
            floor: None,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.retained.len()
    }

    /// The watermark retained for `channel`, without counting this as a consultation.
    pub(super) fn get(&self, channel: &RelayChannelKey) -> Option<&RelayChannelWatermark> {
        let retained = self.retained.get(channel)?;
        Some(&retained.watermark)
    }

    /// Consult the watermark retained for `channel`: its retention starts again and it becomes the
    /// most recently used.
    pub(super) fn consult(&mut self, channel: &RelayChannelKey) -> Option<&RelayChannelWatermark> {
        let retained = self.retained.get_mut(channel)?;
        let position = self.next_position;
        self.next_position = position
            .checked_add(1)
            .assured("a peer cannot record or consult 2^64 channel watermarks");
        self.order.remove(&retained.position);
        self.order.insert(position, channel.clone());
        retained.position = position;
        retained.watermark.mark_reconciled();
        Some(&retained.watermark)
    }

    /// Record `status` as the outcome at `attempt`'s position, unless its channel already retains a
    /// later one. `in_use` tells which channels have an attempt outstanding, whose watermarks stay.
    /// Returns whether the set forgot another channel's watermark to make room.
    pub(super) fn record(
        &mut self,
        attempt: &RelayAttemptKey,
        status: RelayAdmissionStatus,
        in_use: impl Fn(&RelayChannelKey) -> bool,
    ) -> bool {
        let mut forgot = false;
        match self.retained.get(&attempt.channel) {
            Some(retained) if attempt.sequence < retained.watermark.sequence => return false,
            Some(_) => {}
            None => {
                if self.retained.len() >= self.capacity {
                    forgot = self.forget_least_recent(in_use);
                }
            }
        }
        let position = self.next_position;
        self.next_position = position
            .checked_add(1)
            .assured("a peer cannot record or consult 2^64 channel watermarks");
        let replaced = self.retained.insert(
            attempt.channel.clone(),
            RetainedWatermark {
                watermark: RelayChannelWatermark::new(attempt.sequence, status),
                position,
            },
        );
        if let Some(replaced) = replaced {
            self.order.remove(&replaced.position);
        }
        self.order.insert(position, attempt.channel.clone());
        forgot
    }

    /// Whether `channel` is one the set forgot, or one its sender created before a channel the set
    /// forgot: it retains nothing for it and its incarnation is at or below the floor.
    pub(super) fn is_forgotten(&self, channel: &RelayChannelKey) -> bool {
        if self.retained.contains_key(channel) {
            return false;
        }
        let Some(floor) = &self.floor else {
            return false;
        };
        channel.channel_incarnation <= *floor
    }

    /// Forget every watermark not recorded or consulted for the retention, except those of the
    /// channels `in_use` reports an attempt outstanding on.
    ///
    /// The scan stops at the first watermark still within its retention, because every later
    /// position was recorded or consulted later. Before that it passes over at most one retained
    /// watermark per outstanding attempt.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the peer owner supplies a synchronous lookup of its own \
                                   attempts under the protocol guard it already holds; its \
                                   effects belong to that owner")
    )]
    pub(super) fn expire(&mut self, now: Instant, in_use: impl Fn(&RelayChannelKey) -> bool) {
        let mut expired = Vec::new();
        for (position, channel) in &self.order {
            let retained = self
                .retained
                .get(channel)
                .verified("every reconciliation position names a retained watermark");
            let idle = now
                .checked_duration_since(retained.watermark.last_reconciled_at())
                .unwrap_or(Duration::ZERO);
            if idle < RELAY_CHANNEL_RETENTION {
                break;
            }
            if !in_use(channel) {
                expired.push(*position);
            }
        }
        for position in expired {
            let channel = self
                .order
                .remove(&position)
                .verified("the expiry scan above read this position");
            self.retained.remove(&channel);
            self.raise_floor(&channel);
        }
    }

    /// Forget every watermark and the floor, as when the peer binds another process epoch.
    pub(super) fn clear(&mut self) {
        self.retained.clear();
        self.order.clear();
        self.floor = None;
    }

    /// Forget the watermark recorded or consulted least recently among the channels `in_use`
    /// reports nothing outstanding on. Returns whether there was one.
    ///
    /// The scan passes over at most one retained watermark per outstanding attempt.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the peer owner supplies a synchronous lookup of its own \
                                   attempts under the protocol guard it already holds; its \
                                   effects belong to that owner")
    )]
    fn forget_least_recent(&mut self, in_use: impl Fn(&RelayChannelKey) -> bool) -> bool {
        let mut chosen = None;
        for (position, channel) in &self.order {
            if !in_use(channel) {
                chosen = Some(*position);
                break;
            }
        }
        let Some(position) = chosen else {
            return false;
        };
        let channel = self
            .order
            .remove(&position)
            .verified("the scan above read this position");
        self.retained
            .remove(&channel)
            .verified("every reconciliation position names a retained watermark");
        self.raise_floor(&channel);
        true
    }

    fn raise_floor(&mut self, channel: &RelayChannelKey) {
        if let Some(floor) = &self.floor
            && *floor >= channel.channel_incarnation
        {
            return;
        }
        self.floor = Some(channel.channel_incarnation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The documented cost of one retained watermark, about 180 bytes, is its key, its watermark
    /// with its position and its place in the reconciliation order, with the tables' own overhead
    /// on top of these.
    #[test]
    fn a_retained_watermark_stays_within_its_documented_size() {
        let inline = size_of::<RelayChannelKey>()
            + size_of::<RetainedWatermark>()
            + size_of::<(u64, RelayChannelKey)>();
        assert!(
            inline <= 160,
            "a retained watermark takes {inline} bytes before table overhead"
        );
    }
}
