//! Layer: engines and infrastructure.
//! - **Owns.** Sharded key retention, global capacity, TTL and LRU admission leases.
//! - **Depends on.** ExpiryMap, selected blocking locks and the GCRA cell.
//! - **Must not know.** Payloads, graphs, wall clocks, timers or persistence.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        key = "one unit allowance clause and a 128-bit key fingerprint",
        bound = "MAX KEYS bounds retained entries; a configured number of shards is locked in \
                 ascending order with no await",
        reason = "a key lease fences governor admission, refund and final LRU publication"
    )
)]

use std::time::Duration;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_expiry_map::ExpiryMap;
use nervix_primitives::sync::{
    atomic::{AtomicU64, Ordering},
    blocking::{Mutex, MutexGuard},
};

use crate::{AdmissionError, KeyFingerprint, KeyedOptions, cell::DirectCell};

type Shard = ExpiryMap<KeyFingerprint, Entry>;

struct Entry {
    cell: DirectCell,
    last_seen: Duration,
    ordinal: u64,
}

pub(crate) struct KeyedStore {
    options: KeyedOptions,
    shards: Vec<Mutex<Shard>>,
    visits: AtomicU64,
    evictions: AtomicU64,
}

impl KeyedStore {
    pub(crate) fn new(options: KeyedOptions) -> Self {
        let shards = (0..options.shards.get())
            .map(|_| Mutex::new(ExpiryMap::new()))
            .collect();
        Self {
            options,
            shards,
            visits: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    fn shard(&self, key: KeyFingerprint) -> usize {
        let fingerprint = u128::from_be_bytes(key.0);
        let index = fingerprint
            % u128::try_from(self.shards.len()).verified("a positive shard count fits u128");
        usize::try_from(index).verified("the remainder is smaller than the usize shard count")
    }

    fn locks(&self) -> Vec<MutexGuard<'_, Shard>> {
        // One-shard users never acquire another lock. Global operations always use this order.
        self.shards.iter().map(Mutex::lock).collect()
    }

    fn next_ordinal(&self) -> Result<u64, Report<AdmissionError>> {
        let mut before = self.visits.load(Ordering::Relaxed);
        loop {
            let next = before
                .checked_add(1)
                .ok_or_else(|| Report::new(AdmissionError::VisitRange))?;
            match self.visits.compare_exchange_weak(
                before,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(before),
                Err(current) => before = current,
            }
        }
    }

    fn expire(&self, shard: &mut Shard, now: Duration) {
        while let Some((_, entry)) = shard.oldest() {
            if now.saturating_sub(entry.last_seen) < self.options.ttl {
                break;
            }
            shard
                .remove_oldest()
                .verified("oldest is present under this shard's exclusive guard");
        }
    }

    pub(crate) fn prepare(
        &self,
        key: KeyFingerprint,
        now: Duration,
    ) -> Result<Option<KeyLease<'_>>, Report<AdmissionError>> {
        if self.options.max_keys == 0 {
            return Ok(None);
        }
        let index = self.shard(key);
        let mut shard = self.shards[index].lock();
        self.expire(&mut shard, now);
        if shard.contains_key(&key) {
            let ordinal = self.next_ordinal()?;
            return Ok(Some(KeyLease {
                key,
                now,
                ordinal,
                evictions: &self.evictions,
                entry: LeaseEntry::Present(shard),
            }));
        }
        drop(shard);
        let mut shards = self.locks();
        for shard in &mut shards {
            self.expire(shard, now);
        }
        // A concurrent writer may have installed this key after the single-shard guard ended.
        if shards[index].contains_key(&key) {
            let ordinal = self.next_ordinal()?;
            return Ok(Some(KeyLease {
                key,
                now,
                ordinal,
                evictions: &self.evictions,
                entry: LeaseEntry::Across { shards, index },
            }));
        }
        let mut active = 0usize;
        for shard in &shards {
            active = active
                .checked_add(shard.len())
                .verified("the retained entries fit the address space");
        }
        let mut victim: Option<usize> = None;
        if active >= self.options.max_keys {
            // Only the oldest entry of each explicitly bounded shard participates in this minimum.
            let mut oldest: Option<u64> = None;
            for (index, shard) in shards.iter().enumerate() {
                if let Some((_, entry)) = shard.oldest()
                    && (oldest.is_none() || Some(entry.ordinal) < oldest)
                {
                    oldest = Some(entry.ordinal);
                    victim = Some(index);
                }
            }
        }
        let ordinal = self.next_ordinal()?;
        Ok(Some(KeyLease {
            key,
            now,
            ordinal,
            evictions: &self.evictions,
            entry: LeaseEntry::Vacant {
                shards,
                index,
                cell: DirectCell::new(0),
                victim,
            },
        }))
    }

    pub(crate) fn statistics(&self, now: Duration) -> crate::KeyStatistics {
        let mut shards = self.locks();
        let mut active = 0usize;
        for shard in &mut shards {
            self.expire(shard, now);
            active = active
                .checked_add(shard.len())
                .verified("the retained entries fit the address space");
        }
        crate::KeyStatistics {
            active,
            evicted: u128::from(self.evictions.load(Ordering::Relaxed)),
        }
    }

    pub(crate) fn generation(&self, now: Duration) -> Vec<crate::generation::KeyGeneration> {
        let shards = self.locks();
        let mut keys = Vec::new();
        for shard in &shards {
            for (key, entry) in shard.iter() {
                let age = now.saturating_sub(entry.last_seen);
                let horizon = Duration::from_nanos(entry.cell.horizon());
                if age < self.options.ttl && horizon > now {
                    keys.push(crate::generation::KeyGeneration {
                        key: *key,
                        remaining: horizon.saturating_sub(now),
                        age,
                        ordinal: entry.ordinal,
                    });
                }
            }
        }
        keys.sort_unstable_by_key(|entry| entry.ordinal);
        keys
    }

    pub(crate) fn restore(
        &self,
        keys: &[crate::generation::KeyGeneration],
        now: Duration,
    ) -> Result<(), Report<AdmissionError>> {
        let mut shards = self.locks();
        let mut next_visit = 0;
        for key in keys {
            let horizon = now
                .checked_add(key.remaining)
                .ok_or_else(|| Report::new(AdmissionError::ClockRange))?;
            let horizon = u64::try_from(horizon.as_nanos())
                .map_err(|_| Report::new(AdmissionError::ClockRange))?;
            let last_seen = now
                .checked_sub(key.age)
                .verified("the restored epoch precedes every retained key's last use");
            let entry = Entry {
                cell: DirectCell::new(horizon),
                last_seen,
                ordinal: key.ordinal,
            };
            assert!(
                shards[self.shard(key.key)].insert(key.key, entry),
                "generation validation proves unique keys and the destination is fresh"
            );
            next_visit = key
                .ordinal
                .checked_add(1)
                .verified("generation validation refuses the final ordinal");
        }
        self.visits.store(next_visit, Ordering::Relaxed);
        Ok(())
    }
}

enum LeaseEntry<'a> {
    Present(MutexGuard<'a, Shard>),
    Across {
        shards: Vec<MutexGuard<'a, Shard>>,
        index: usize,
    },
    Vacant {
        shards: Vec<MutexGuard<'a, Shard>>,
        index: usize,
        cell: DirectCell,
        victim: Option<usize>,
    },
}

pub(crate) struct KeyLease<'a> {
    key: KeyFingerprint,
    now: Duration,
    ordinal: u64,
    evictions: &'a AtomicU64,
    entry: LeaseEntry<'a>,
}

impl KeyLease<'_> {
    pub(crate) fn cell(&self) -> &DirectCell {
        match &self.entry {
            LeaseEntry::Present(shard) => {
                &shard
                    .get(&self.key)
                    .verified("the lease holds this key's shard")
                    .cell
            }
            LeaseEntry::Across { shards, index } => {
                &shards[*index]
                    .get(&self.key)
                    .verified("the lease holds every shard")
                    .cell
            }
            LeaseEntry::Vacant { cell, .. } => cell,
        }
    }

    fn touch(shard: &mut Shard, key: KeyFingerprint, now: Duration, ordinal: u64) {
        let mut entry = shard
            .remove(&key)
            .verified("the lease owns this retained entry");
        entry.last_seen = entry.last_seen.max(now);
        entry.ordinal = ordinal;
        assert!(
            shard.insert(key, entry),
            "the lease just removed its own key"
        );
    }

    pub(crate) fn commit(&mut self) {
        match &mut self.entry {
            LeaseEntry::Present(shard) => Self::touch(shard, self.key, self.now, self.ordinal),
            LeaseEntry::Across { shards, index } => {
                Self::touch(&mut shards[*index], self.key, self.now, self.ordinal)
            }
            LeaseEntry::Vacant {
                shards,
                index,
                cell,
                victim,
            } => {
                if let Some(victim) = victim {
                    shards[*victim]
                        .remove_oldest()
                        .verified("a full store selected an oldest retained entry");
                    // Every eviction follows a successful visit reservation, which refuses before u64 overflows.
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
                let entry = Entry {
                    cell: DirectCell::new(cell.horizon()),
                    last_seen: self.now,
                    ordinal: self.ordinal,
                };
                assert!(
                    shards[*index].insert(self.key, entry),
                    "all shards stayed exclusively held through admission"
                );
            }
        }
    }
}
