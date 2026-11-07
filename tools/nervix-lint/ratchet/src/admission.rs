//! Repository tooling, outside the product layer order.
//! Owns: the shared admission lease representation and pure memory-admission decision.
//! Depends on: typed memory observations and current lease values.
//! Must not know: compiler diagnostics, Cargo scheduling or product execution.

use std::{collections::BTreeMap, path::PathBuf};

use meticulous::OptionExt as _;
use serde::{Deserialize, Serialize};

pub const GIB: u64 = 1 << 30;
pub const RESPONSIVE_NS: i128 = 10_000_000_000;

/// One identity for the crate and target kind used by admission and measurement.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Compiled {
    pub crate_name: String,
    pub kind: String,
}

impl Compiled {
    pub fn new(crate_name: String, kinds: &[String]) -> Self {
        Self {
            crate_name,
            kind: target_kind(kinds).to_owned(),
        }
    }
}

pub fn target_kind(kinds: &[String]) -> &str {
    for kind in kinds {
        if matches!(
            kind.as_str(),
            "lib" | "rlib" | "cdylib" | "staticlib" | "proc-macro"
        ) {
            return "lib";
        }
    }
    match kinds.first() {
        Some(kind) => kind,
        None => "unknown",
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    #[serde(rename = "crate")]
    pub crate_name: String,
    pub kind: String,
    pub bytes: u64,
}

/// The wrapper receives this policy, with the same directory and declaration as the runner.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub directory: PathBuf,
    pub threshold_bytes: u64,
    pub wait_limit_seconds: f64,
    #[serde(with = "reservation_map")]
    pub reservations: BTreeMap<Compiled, u64>,
}

impl Policy {
    pub fn reservation(&self, compiled: &Compiled) -> Option<u64> {
        self.reservations.get(compiled).copied()
    }

    pub fn keyed_reservations(&self) -> &BTreeMap<Compiled, u64> {
        &self.reservations
    }
}

mod reservation_map {
    use serde::{Deserializer, Serializer};

    use super::*;

    pub fn serialize<S: Serializer>(
        entries: &BTreeMap<Compiled, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut values = Vec::new();
        for (compiled, bytes) in entries {
            values.push(Reservation {
                crate_name: compiled.crate_name.clone(),
                kind: compiled.kind.clone(),
                bytes: *bytes,
            });
        }
        values.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Compiled, u64>, D::Error> {
        let values = Vec::<Reservation>::deserialize(deserializer)?;
        let mut entries = BTreeMap::new();
        for value in values {
            entries.insert(
                Compiled {
                    crate_name: value.crate_name,
                    kind: value.kind,
                },
                value.bytes,
            );
        }
        Ok(entries)
    }
}

/// The current shared lease shape, also used by other worktrees on this host.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub identifier: String,
    pub pid: u32,
    pub crate_name: String,
    pub kind: String,
    pub configuration: String,
    pub root: String,
    pub reservation_bytes: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    pub cgroup: Option<String>,
    pub enqueued_ns: i128,
    pub polled_ns: i128,
    #[serde(deserialize_with = "Option::deserialize")]
    pub admitted_ns: Option<i128>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub driver_pid: Option<u32>,
}

impl Lease {
    fn queued_before(&self, other: &Self) -> bool {
        self.enqueued_ns
            .cmp(&other.enqueued_ns)
            .then_with(|| self.identifier.cmp(&other.identifier))
            .is_lt()
    }

    pub fn describe(&self) -> String {
        format!(
            "{} ({}) in the {} analysis of {}, pid {}",
            self.crate_name, self.kind, self.configuration, self.root, self.pid
        )
    }

    fn outstanding_bytes(&self, resident: &impl Fn(u32) -> Option<u64>) -> u64 {
        let Some(pid) = self.driver_pid else {
            return self.reservation_bytes;
        };
        let Some(used) = resident(pid) else {
            return 0;
        };
        // Admission charges only the remaining possible growth, clamped to zero past the bound.
        self.reservation_bytes.saturating_sub(used)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Domain {
    pub cgroup: Option<String>,
    pub available_bytes: u64,
}

impl Domain {
    fn contains(&self, lease: &Lease) -> bool {
        let Some(domain) = &self.cgroup else {
            return true;
        };
        let Some(group) = &lease.cgroup else {
            return false;
        };
        group == domain
            || group
                .strip_prefix(domain)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Decision<'a> {
    pub admitted: bool,
    pub fits: bool,
    pub ahead: usize,
    pub holders: Vec<&'a Lease>,
    pub left_bytes: Option<i128>,
}

impl<'a> Decision<'a> {
    /// Decides from observations alone; no filesystem, clock, process or queue access.
    pub fn decide(
        waiter: &Lease,
        leases: &'a [Lease],
        domains: &[Domain],
        resident: impl Fn(u32) -> Option<u64>,
        now_ns: i128,
    ) -> Self {
        let mut holders = Vec::new();
        let mut ahead = 0_usize;
        for lease in leases {
            if lease.identifier == waiter.identifier {
                continue;
            }
            if lease.admitted_ns.is_some() {
                holders.push(lease);
                continue;
            }
            if !lease.queued_before(waiter) {
                continue;
            }
            let elapsed = now_ns
                .checked_sub(lease.polled_ns)
                .assured("two physical observations fit the signed nanosecond coordinate");
            if elapsed > RESPONSIVE_NS {
                continue;
            }
            ahead = ahead
                .checked_add(1)
                .assured("a live lease occupies memory before it is counted");
        }
        let mut left_bytes = None;
        for domain in domains {
            let mut growth = 0_i128;
            for holder in &holders {
                if domain.contains(holder) {
                    growth = growth
                        .checked_add(i128::from(holder.outstanding_bytes(&resident)))
                        .assured("in-memory lease reservations fit the signed memory coordinate");
                }
            }
            let left = i128::from(domain.available_bytes)
                .checked_sub(growth)
                .assured(
                    "the signed memory coordinate represents available memory minus reserved \
                     growth",
                );
            left_bytes = Some(match left_bytes {
                Some(previous) => left.min(previous),
                None => left,
            });
        }
        let fits = match left_bytes {
            Some(left) => i128::from(waiter.reservation_bytes) <= left,
            None => false,
        };
        Self {
            admitted: ahead == 0 && (fits || holders.is_empty()),
            fits,
            ahead,
            holders,
            left_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionOutcome {
    Fit,
    Alone,
    WaitLimit,
    QueueLocked,
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionRecord {
    pub reservation_bytes: u64,
    pub outcome: AdmissionOutcome,
    pub waited_seconds: f64,
    #[serde(deserialize_with = "Option::deserialize")]
    pub left_bytes: Option<i128>,
    pub holders: usize,
    #[serde(deserialize_with = "Option::deserialize")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests;
