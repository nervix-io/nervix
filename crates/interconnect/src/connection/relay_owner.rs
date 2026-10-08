//! Bounded relay protocol ownership for one authenticated peer transport.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Volatile peer-local grants, admission correlations, channel ordering and retirement.
//! - **Depends on.** The connection's retained identity, admission permits and primitive boundary.
//! - **Must not know.** Runtime graphs, branch processing, placement or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        key = "one authenticated peer transport and its process epochs",
        bound = "configured incoming capacity bounds attempts, grants and outbound correlations; \
                 no guard crosses await",
        reason = "the retained peer owner serializes admission identity and channel ordering \
                  transitions"
    )
)]

use std::collections::hash_map::Entry as MapEntry;

use ahash::{HashMap, HashMapExt as _};
use nervix_primitives::sync::blocking::Mutex;

use super::*;

impl TransportState {
    pub(super) fn bind_relay_owner(
        &self,
        node: &ClusterNodeName,
        epoch: u64,
        binding_sequence: u64,
    ) -> Result<StdArc<RelayPeerOwner>, Report<TransportError>> {
        loop {
            let current = self.relay_owners.load_full();
            if self.admission_closed.is_cancelled() {
                return Err(Report::new(TransportError::RelayIndeterminate));
            }
            let member = current
                .live_nodes
                .as_ref()
                .is_none_or(|live| live.contains(node));
            if let Some(owner) = current.owners.get(node) {
                owner.bind_epoch(epoch, binding_sequence, member);
                return Ok(owner.clone());
            }
            if self.admission_closed.is_cancelled()
                || current.owners.len() >= self.options.max_peers
            {
                return Err(Report::new(TransportError::PoolExhausted));
            }
            let owner = StdArc::new(RelayPeerOwner::new(self.options.incoming_queue_capacity));
            assert!(
                owner.bind_epoch(epoch, binding_sequence, member),
                "a newly created owner accepts its first process binding"
            );
            let mut next = (*current).clone();
            next.owners.insert(node.clone(), owner.clone());
            let observed = self
                .relay_owners
                .compare_and_swap(&current, StdArc::new(next));
            if StdArc::ptr_eq(&current, &observed) {
                return Ok(owner);
            }
        }
    }

    pub(super) fn retire_departed_relay_owners(&self, live: &BTreeSet<ClusterNodeName>) {
        loop {
            let current = self.relay_owners.load_full();
            let mut next = (*current).clone();
            next.live_nodes = Some(live.clone());
            for (node, owner) in current.owners.iter() {
                if live.contains(node) {
                    owner.activate();
                } else if !owner.awaiting_membership() {
                    owner.end();
                    next.owners.remove(node);
                }
            }
            let observed = self
                .relay_owners
                .compare_and_swap(&current, StdArc::new(next));
            if StdArc::ptr_eq(&current, &observed) {
                return;
            }
        }
    }

    pub(super) fn prune_ended_relay_owners(&self) {
        loop {
            let current = self.relay_owners.load_full();
            let mut next = (*current).clone();
            next.owners.retain(|_, owner| !owner.closed.is_cancelled());
            if next.owners.len() == current.owners.len() {
                return;
            }
            let observed = self
                .relay_owners
                .compare_and_swap(&current, StdArc::new(next));
            if StdArc::ptr_eq(&current, &observed) {
                return;
            }
        }
    }

    pub(super) fn retire_relay_record(
        &self,
        record: &StdArc<RelayAdmissionRecord>,
        status: RelayAdmissionStatus,
    ) {
        if let Some(owner) = record.owner.upgrade() {
            owner.retire(record, status);
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct RelayOwnersPublication {
    pub(super) owners: BTreeMap<ClusterNodeName, StdArc<RelayPeerOwner>>,
    live_nodes: Option<BTreeSet<ClusterNodeName>>,
}

enum RelayPeerRun {
    Starting,
    AwaitingMembership { epoch: u64, binding_sequence: u64 },
    Bound { epoch: u64, binding_sequence: u64 },
    Ended,
}

pub(super) struct RelayPeerOwner {
    state: Mutex<RelayPeerProtocol>,
    capacity: usize,
    created_at: Instant,
    pub(super) closed: CancellationToken,
}

struct RelayPeerProtocol {
    run: RelayPeerRun,
    // Ordered retirement makes cancellation and permit release deterministic under exploration.
    grants: BTreeMap<u64, RelayGrant>,
    attempts: BTreeMap<RelayAttemptKey, StdArc<RelayAdmissionRecord>>,
    active_channels: HashMap<RelayChannelKey, u64>,
    admissions: HashMap<RelayAdmissionKey, StdArc<RelayAdmissionRecord>>,
    watermarks: HashMap<RelayChannelKey, RelayChannelWatermark>,
    outbound_attempts: HashMap<OutboundRelayKey, OutboundRelayAttempt>,
    outbound_admissions: HashMap<RelayAdmissionKey, OutboundRelayKey>,
}

struct OutboundRelayAttempt {
    receiver_epoch: u64,
    last_progress: Instant,
}

impl RelayPeerOwner {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(RelayPeerProtocol {
                run: RelayPeerRun::Starting,
                grants: BTreeMap::new(),
                attempts: BTreeMap::new(),
                active_channels: HashMap::new(),
                admissions: HashMap::new(),
                watermarks: HashMap::new(),
                outbound_attempts: HashMap::new(),
                outbound_admissions: HashMap::new(),
            }),
            capacity,
            created_at: Instant::now(),
            closed: CancellationToken::new(),
        }
    }

    /// Only connection binding changes the authenticated epoch. Existing connections retain this
    /// owner and their own epoch, so requests from a preceding connection cannot reinstall it.
    pub(super) fn bind_epoch(&self, epoch: u64, binding_sequence: u64, member: bool) -> bool {
        let mut state = self.state.lock();
        match &mut state.run {
            RelayPeerRun::Ended => return false,
            RelayPeerRun::Bound {
                epoch: current,
                binding_sequence: bound,
            }
            | RelayPeerRun::AwaitingMembership {
                epoch: current,
                binding_sequence: bound,
            } if *current == epoch => {
                *bound = (*bound).max(binding_sequence);
                if member {
                    state.activate();
                }
                return true;
            }
            RelayPeerRun::Bound {
                binding_sequence: bound,
                ..
            }
            | RelayPeerRun::AwaitingMembership {
                binding_sequence: bound,
                ..
            } if binding_sequence < *bound => return false,
            RelayPeerRun::Starting
            | RelayPeerRun::Bound { .. }
            | RelayPeerRun::AwaitingMembership { .. } => {}
        }
        state.end_inbound();
        state.run = if member {
            RelayPeerRun::Bound {
                epoch,
                binding_sequence,
            }
        } else {
            RelayPeerRun::AwaitingMembership {
                epoch,
                binding_sequence,
            }
        };
        true
    }

    fn activate(&self) {
        self.state.lock().activate();
    }

    fn awaiting_membership(&self) -> bool {
        matches!(
            self.state.lock().run,
            RelayPeerRun::AwaitingMembership { .. }
        )
    }

    pub(super) fn end(&self) {
        let mut state = self.state.lock();
        state.run = RelayPeerRun::Ended;
        self.closed.cancel();
        state.end_inbound();
        state.outbound_attempts.clear();
        state.outbound_admissions.clear();
    }

    pub(super) fn accepts_epoch(&self, epoch: u64) -> bool {
        let state = self.state.lock();
        state.accepts_epoch(epoch)
    }

    pub(super) fn attempt(
        &self,
        attempt: &RelayAttemptKey,
    ) -> Option<StdArc<RelayAdmissionRecord>> {
        self.state.lock().attempts.get(attempt).cloned()
    }

    pub(super) fn retired_status(&self, attempt: &RelayAttemptKey) -> Option<RelayAdmissionStatus> {
        self.state.lock().retired_status(attempt)
    }

    pub(super) fn can_follow(&self, attempt: &RelayAttemptKey) -> bool {
        self.state.lock().can_follow(attempt)
    }

    pub(super) fn channel_busy(&self, attempt: &RelayAttemptKey) -> bool {
        let mut state = self.state.lock();
        let Some(sequence) = state.active_channels.get(&attempt.channel).copied() else {
            return false;
        };
        let active = RelayAttemptKey {
            channel: attempt.channel.clone(),
            sequence,
        };
        if let Some(record) = state.attempts.get(&active)
            && record.is_unadmitted()
        {
            return true;
        }
        state.active_channels.remove(&attempt.channel);
        false
    }

    pub(super) fn has_admission(&self, key: &RelayAdmissionKey) -> bool {
        self.state.lock().admissions.contains_key(key)
    }

    pub(super) fn has_grant(&self, grant_id: u64) -> bool {
        self.state.lock().grants.contains_key(&grant_id)
    }

    pub(super) fn register(&self, grant_id: u64, grant: RelayGrant) -> RelayGrantRegistration {
        let mut state = self.state.lock();
        let record = &grant.admission;
        if !state.accepts_epoch(record.attempt.channel.sender_epoch) {
            record.cancel();
            return RelayGrantRegistration::Retired(RelayGrantDisposition::Cancelled);
        }
        if let Some(existing) = state.attempts.get(&record.attempt) {
            return RelayGrantRegistration::Existing(existing.clone());
        }
        if let Some(status) = state.retired_status(&record.attempt) {
            return RelayGrantRegistration::Retired(status.into());
        }
        if !state.can_follow(&record.attempt) {
            return RelayGrantRegistration::InvalidSequence;
        }
        if state.active_channels.contains_key(&record.attempt.channel) {
            return RelayGrantRegistration::ChannelBusy;
        }
        if state.admissions.contains_key(&record.admission_key) {
            return RelayGrantRegistration::AdmissionBusy;
        }
        // A retained watermark consumes a channel position too. Evicting it to accept another
        // channel would let a delayed sequence-zero request be admitted again.
        if state.grants.contains_key(&grant_id)
            || state.attempts.len() >= self.capacity
            || (!state.watermarks.contains_key(&record.attempt.channel)
                && state.watermarks.len() >= self.capacity)
        {
            return RelayGrantRegistration::ChannelBusy;
        }
        state
            .active_channels
            .insert(record.attempt.channel.clone(), record.attempt.sequence);
        state
            .admissions
            .insert(record.admission_key.clone(), StdArc::clone(record));
        state
            .attempts
            .insert(record.attempt.clone(), StdArc::clone(record));
        state.grants.insert(grant_id, grant);
        RelayGrantRegistration::Registered
    }

    pub(super) fn claim_grant(
        &self,
        grant_id: u64,
        sender_epoch: u64,
        receiver_epoch: u64,
        now: Instant,
    ) -> Option<RelayGrant> {
        let mut state = self.state.lock();
        if !state.accepts_epoch(sender_epoch) {
            return None;
        }
        let grant = state.grants.get(&grant_id)?;
        if grant.admission.attempt.channel.sender_epoch != sender_epoch
            || grant.admission.attempt.channel.receiver_epoch != receiver_epoch
            || now >= grant.expires_at
        {
            return None;
        }
        state.grants.remove(&grant_id)
    }

    pub(super) fn expire_grant(&self, grant_id: u64, now: Instant) {
        let mut state = self.state.lock();
        let Some(grant) = state.grants.get(&grant_id) else {
            return;
        };
        if now < grant.expires_at {
            return;
        }
        let grant = state
            .grants
            .remove(&grant_id)
            .verified("the grant was found under this owner's guard");
        let status = grant.admission.cancel();
        state.retire(&grant.admission, status);
    }

    pub(super) fn control(
        &self,
        attempt: &RelayAttemptKey,
        cancel: bool,
    ) -> (RelayAdmissionStatus, Option<StdArc<RelayAdmissionRecord>>) {
        let mut state = self.state.lock();
        if !state.accepts_epoch(attempt.channel.sender_epoch) {
            return (RelayAdmissionStatus::Indeterminate, None);
        }
        if let Some(record) = state.attempts.get(attempt).cloned() {
            let status = if cancel {
                let grant = record.reserved_grant_id();
                let status = record.cancel();
                if let Some(grant) = grant {
                    state.grants.remove(&grant);
                }
                status
            } else {
                record.status()
            };
            return (status, Some(record));
        }
        if let Some(status) = state.retired_status(attempt) {
            return (status, None);
        }
        if cancel
            && !state.active_channels.contains_key(&attempt.channel)
            && state.can_follow(attempt)
        {
            if !state.watermarks.contains_key(&attempt.channel)
                && state.watermarks.len() >= self.capacity
            {
                return (RelayAdmissionStatus::Unknown, None);
            }
            state.record_watermark(attempt, RelayAdmissionStatus::Cancelled);
            return (RelayAdmissionStatus::Cancelled, None);
        }
        (RelayAdmissionStatus::Unknown, None)
    }

    pub(super) fn retire(
        &self,
        record: &StdArc<RelayAdmissionRecord>,
        status: RelayAdmissionStatus,
    ) {
        self.state.lock().retire(record, status);
    }

    pub(super) fn completed_admission(
        &self,
        key: &RelayAdmissionKey,
        outcome: &RemoteAckOutcome,
    ) -> Option<StdArc<RelayAdmissionRecord>> {
        let state = self.state.lock();
        let record = state.admissions.get(key)?.clone();
        record.report_progress();
        match outcome {
            RemoteAckOutcome::NoAck(reason) => record.reject(reason.clone()),
            RemoteAckOutcome::Ack => record.mark_admitted(),
            RemoteAckOutcome::Alive | RemoteAckOutcome::Progress { .. } => return None,
        }
        Some(record)
    }

    pub(super) fn progress_registrations(&self) -> BTreeSet<RemoteAckRegistration> {
        self.state
            .lock()
            .attempts
            .values()
            .filter_map(|record| record.progress_registration())
            .collect()
    }

    pub(super) fn sweep(&self, now: Instant) {
        let mut state = self.state.lock();
        if matches!(state.run, RelayPeerRun::AwaitingMembership { .. })
            && now
                .checked_duration_since(self.created_at)
                .assured("the peer sweep follows owner creation")
                >= RELAY_CHANNEL_RETENTION
        {
            state.run = RelayPeerRun::Ended;
            self.closed.cancel();
            state.end_inbound();
            state.outbound_attempts.clear();
            state.outbound_admissions.clear();
        }
        let active = &state.active_channels;
        let retained = &state.watermarks;
        let expired = retained
            .iter()
            .filter(|(channel, watermark)| {
                !active.contains_key(*channel)
                    && now
                        .checked_duration_since(watermark.last_reconciled_at())
                        .unwrap_or(Duration::ZERO)
                        >= RELAY_CHANNEL_RETENTION
            })
            .map(|(channel, _)| channel.clone())
            .collect::<Vec<_>>();
        for channel in expired {
            state.watermarks.remove(&channel);
        }
        // Terminal outcomes whose replies were lost remain available throughout reconciliation.
        // A live sender must reconcile within the runtime's five-minute total admission bound.
        let expired = state
            .attempts
            .values()
            .filter_map(|entry| {
                if now
                    .checked_duration_since(entry.last_progress())
                    .assured("a relay sweep follows the attempt's progress")
                    >= RELAY_CHANNEL_RETENTION
                {
                    Some(entry.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for record in expired {
            let status = record.cancel();
            state.retire(&record, status);
        }
        let mut expired_outbound = Vec::new();
        for (key, attempt) in &state.outbound_attempts {
            if now
                .checked_duration_since(attempt.last_progress)
                .assured("a relay sweep follows outbound progress")
                >= RELAY_CHANNEL_RETENTION
            {
                expired_outbound.push(key.clone());
            }
        }
        for key in expired_outbound {
            state.retire_outbound(&key);
        }
    }

    pub(super) fn register_outbound(
        &self,
        key: &OutboundRelayKey,
        epoch: u64,
        admission: &RelayAdmissionKey,
    ) -> Result<(), Report<TransportError>> {
        let mut state = self.state.lock();
        if !state.accepts_epoch(epoch) {
            return Err(Report::new(TransportError::RelayIndeterminate));
        }
        if let Some(registered) = state.outbound_attempts.get(key) {
            if registered.receiver_epoch != epoch {
                state.retire_outbound(key);
                return Err(Report::new(TransportError::RelayIndeterminate));
            }
        } else if state.outbound_attempts.len() >= self.outbound_capacity() {
            return Err(Report::new(TransportError::PoolExhausted));
        }
        if !state.outbound_admissions.contains_key(admission)
            && state.outbound_admissions.len() >= self.outbound_capacity()
        {
            return Err(Report::new(TransportError::PoolExhausted));
        }
        if let Some(registered) = state.outbound_admissions.get(admission)
            && registered != key
        {
            return Err(Report::new(TransportError::RelayGrant(
                "relay admission acknowledgement names another delivery".to_string(),
            )));
        }
        state.outbound_attempts.insert(
            key.clone(),
            OutboundRelayAttempt {
                receiver_epoch: epoch,
                last_progress: Instant::now(),
            },
        );
        state
            .outbound_admissions
            .insert(admission.clone(), key.clone());
        Ok(())
    }

    fn outbound_capacity(&self) -> usize {
        self.capacity
            .checked_mul(2)
            .assured("one outgoing delivery and its next admission fit the configured capacity")
    }

    pub(super) fn outbound_epoch(&self, key: &OutboundRelayKey) -> Option<u64> {
        let mut state = self.state.lock();
        let attempt = state.outbound_attempts.get_mut(key)?;
        attempt.last_progress = Instant::now();
        Some(attempt.receiver_epoch)
    }

    pub(super) fn retire_outbound(&self, key: &OutboundRelayKey) {
        self.state.lock().retire_outbound(key);
    }

    pub(super) fn retire_outbound_admission(&self, key: &RelayAdmissionKey) {
        let mut state = self.state.lock();
        if let Some(delivery) = state.outbound_admissions.remove(key) {
            state.outbound_attempts.remove(&delivery);
        }
    }

    pub(super) fn snapshot(&self) -> RelayOwnerSnapshot {
        let state = self.state.lock();
        let mut oldest = Duration::ZERO;
        for entry in state.attempts.values() {
            oldest = oldest.max(entry.reserved_at.elapsed());
        }
        RelayOwnerSnapshot {
            channels: state.active_channels.len(),
            attempts: state.attempts.len(),
            grants: state.grants.len(),
            oldest,
        }
    }
}

pub(super) struct RelayOwnerSnapshot {
    pub(super) channels: usize,
    pub(super) attempts: usize,
    pub(super) grants: usize,
    pub(super) oldest: Duration,
}

impl RelayPeerProtocol {
    fn activate(&mut self) {
        if let RelayPeerRun::AwaitingMembership {
            epoch,
            binding_sequence,
        } = self.run
        {
            self.run = RelayPeerRun::Bound {
                epoch,
                binding_sequence,
            };
        }
    }

    fn accepts_epoch(&self, epoch: u64) -> bool {
        matches!(self.run, RelayPeerRun::Bound { epoch: current, .. } if current == epoch)
    }

    fn end_inbound(&mut self) {
        for entry in self.attempts.values() {
            entry.cancel();
            entry.release_capacity();
        }
        self.grants.clear();
        self.attempts.clear();
        self.active_channels.clear();
        self.admissions.clear();
        self.watermarks.clear();
    }

    fn retired_status(&mut self, attempt: &RelayAttemptKey) -> Option<RelayAdmissionStatus> {
        let watermark = self.watermarks.get_mut(&attempt.channel)?;
        watermark.mark_reconciled();
        if attempt.sequence < watermark.sequence {
            return Some(RelayAdmissionStatus::Retired);
        }
        if attempt.sequence == watermark.sequence {
            return Some(watermark.status.clone());
        }
        None
    }

    fn can_follow(&self, attempt: &RelayAttemptKey) -> bool {
        let Some(watermark) = self.watermarks.get(&attempt.channel) else {
            return attempt.sequence == 0;
        };
        watermark.sequence.checked_add(1) == Some(attempt.sequence)
    }

    fn record_watermark(&mut self, attempt: &RelayAttemptKey, status: RelayAdmissionStatus) {
        match self.watermarks.entry(attempt.channel.clone()) {
            MapEntry::Occupied(mut entry) => {
                if attempt.sequence >= entry.get().sequence {
                    entry.insert(RelayChannelWatermark::new(attempt.sequence, status));
                }
            }
            MapEntry::Vacant(entry) => {
                entry.insert(RelayChannelWatermark::new(attempt.sequence, status));
            }
        }
    }

    fn retire(&mut self, record: &StdArc<RelayAdmissionRecord>, status: RelayAdmissionStatus) {
        let Some(current) = self.attempts.get(&record.attempt) else {
            return;
        };
        if !StdArc::ptr_eq(current, record) {
            return;
        }
        self.record_watermark(&record.attempt, status);
        self.attempts.remove(&record.attempt);
        if self.active_channels.get(&record.attempt.channel) == Some(&record.attempt.sequence) {
            self.active_channels.remove(&record.attempt.channel);
        }
        if let Some(current) = self.admissions.get(&record.admission_key)
            && StdArc::ptr_eq(current, record)
        {
            self.admissions.remove(&record.admission_key);
        }
        record.release_capacity();
    }

    fn retire_outbound(&mut self, key: &OutboundRelayKey) {
        self.outbound_attempts.remove(key);
        self.outbound_admissions
            .retain(|_, delivery| delivery != key);
    }
}

impl From<RelayAdmissionStatus> for RelayGrantDisposition {
    fn from(status: RelayAdmissionStatus) -> Self {
        match status {
            RelayAdmissionStatus::Admitted => Self::Admitted,
            RelayAdmissionStatus::Rejected(reason) => Self::Rejected(reason),
            RelayAdmissionStatus::Cancelled => Self::Cancelled,
            RelayAdmissionStatus::Reserved
            | RelayAdmissionStatus::BodyReceived
            | RelayAdmissionStatus::Retired
            | RelayAdmissionStatus::Unknown
            | RelayAdmissionStatus::Indeterminate => Self::Retired,
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
#[path = "relay_owner_tests.rs"]
mod tests;
