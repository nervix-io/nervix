//! What one connector entity declares about how much its connector holds and how long it waits.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The declaration of every buffer a connector holds between its transport and the
//!   host, with its capacity and what happens when it is full; of every network wait, with what
//!   ends it; of the limits a driver imposes that no Nervix key changes; and the one meaning of
//!   each client deadline key.
//! - **Depends on.** Durations.
//! - **Must not know.** Any one connector, the driver that holds a buffer, or how a host or a
//!   connector applies a bound.
//!
//! The capability matrix renders these declarations. Each deadline key an entity's waits name is
//! also a runtime capability, so the capability inventory names a scenario in which a peer that
//! stops answering during a wait that key bounds surfaces as the entity's transient status within
//! that bound.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::time::Duration;

use strum::{AsRefStr, EnumIter, IntoStaticStr};

use crate::ClientDeadline;

/// How long establishing a connection may take when its client leaves `connect_timeout_ms` out: for
/// a connection a connector dials for itself, name resolution, the address attempts, TLS and the
/// protocol handshake; for one a listener accepted, its TLS handshake.
///
/// A policy input: generous for a peer across a wide area network, and short enough that a silent
/// name server or an address that drops every packet costs one attempt of the retry policy rather
/// than the operating system's connect timeout, and that a peer which never completes its handshake
/// holds a listener's connection no longer.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

const _: () = assert!(
    !DEFAULT_CONNECT_TIMEOUT.is_zero()
        && DEFAULT_CONNECT_TIMEOUT.as_nanos() <= ClientDeadline::LONGEST.as_nanos(),
    "the default connection deadline follows the rule every configured deadline follows"
);

/// How much one entity's connector holds and how long it waits, whatever its statement says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredBounds {
    /// Every buffer the connector holds between its transport and the host, in the order data
    /// passes through them.
    pub buffers: &'static [DeclaredBuffer],
    /// Every network wait the connector performs, with what ends it.
    pub waits: &'static [DeclaredWait],
    /// The limits its driver imposes that no Nervix key changes, each as the matrix states it.
    pub driver_limits: &'static [&'static str],
}

impl DeclaredBounds {
    /// The deadline keys the entity's waits name, each once, in the order of [`DeadlineKey`].
    ///
    /// An entity declares a handful of waits in one constant, so scanning them for each of the
    /// declared keys is bounded by the declaration.
    pub fn deadline_keys(&self) -> Vec<DeadlineKey> {
        let mut keys = Vec::new();
        for key in <DeadlineKey as strum::IntoEnumIterator>::iter() {
            let named = self.waits.iter().any(|wait| wait.bound.key() == Some(key));
            if named {
                keys.push(key);
            }
        }
        keys
    }
}

/// One buffer a connector holds between its transport and the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredBuffer {
    /// What the buffer holds, as the matrix names it.
    pub holds: &'static str,
    /// How much it holds, and what happens to more.
    pub bound: BufferBound,
}

/// How much one buffer holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferBound {
    /// The buffer holds at most `capacity`, and `overflow` says what happens to more.
    Bounded {
        capacity: BufferCapacity,
        overflow: BufferOverflow,
    },
    /// Nothing bounds the buffer yet: it grows with whatever the peer sends.
    Unbounded,
}

/// What sets the capacity of a bounded buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferCapacity {
    /// A fixed number of items, `count` of `unit`, that Nervix sets.
    Items { count: u64, unit: &'static str },
    /// A fixed number of bytes that Nervix sets.
    Bytes(u64),
    /// The statement clause or client key that sets it, as a statement writes it.
    Setting(&'static str),
    /// The driver's own default, which no Nervix key sets, as the matrix states it.
    Driver(&'static str),
}

/// What happens to what does not fit in a full buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum BufferOverflow {
    /// Whatever fills the buffer waits for room, so a connector stops reading and its transport's
    /// own flow control holds the peer back, or a publish waits for the buffer to drain.
    Backpressure,
    /// What does not fit is dropped and counted.
    DropAndCount,
    /// What does not fit is dropped without being counted: a gap until its connector counts it.
    DropUncounted,
    /// A hand-over that does not fit fails the publish, and the host retries it on its policy.
    FailPublish,
    /// The connector tells its peer the capacity, and the peer discards what does not fit instead
    /// of sending it, so it never reaches the connector, which cannot count it: a maximum packet
    /// size a protocol lets a client advertise when it connects.
    PeerDiscards,
    /// The request is refused immediately; its sender owns retry.
    RefuseRequest,
    /// A response that exceeds the bound fails the poll that read it: the ingestor reports the
    /// failure as its transient error, and the next poll reads again.
    FailPoll,
    /// An incoming message or frame above the bound fails the source read and ends its
    /// connection. The host reconnects on its declared retry policy.
    FailRead,
}

/// One network wait a connector performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredWait {
    /// The wait, as the matrix names it.
    pub wait: &'static str,
    /// What ends it.
    pub bound: WaitBound,
}

/// What ends one network wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitBound {
    /// A client deadline key, with the meaning [`DeadlineKey`] gives it for every connector.
    Key {
        key: DeadlineKey,
        omitted: OmittedDeadline,
    },
    /// A statement clause, as a statement writes it: `ACK TIMEOUT`, a signaling protocol's
    /// `TIMEOUT`.
    Clause(&'static str),
    /// A protocol interval another client key sets, such as MQTT's `keep_alive_ms`, and how the
    /// wait follows from it, including what applies where the client leaves the key out. Unlike a
    /// [`DeadlineKey`], the key keeps its protocol's own meaning.
    ClientKey {
        key: &'static str,
        rule: &'static str,
    },
    /// A fixed physical bound that Nervix sets.
    Fixed(Duration),
    /// The driver's own default, which no Nervix key sets, as the matrix states it.
    Driver(&'static str),
    /// Nothing ends the wait yet: a peer that stops answering holds it until the host stops the
    /// entity.
    Unbounded,
}

impl WaitBound {
    /// The deadline key that ends the wait, where one does.
    pub fn key(&self) -> Option<DeadlineKey> {
        match self {
            Self::Key { key, .. } => Some(*key),
            Self::Clause(_)
            | Self::ClientKey { .. }
            | Self::Fixed(_)
            | Self::Driver(_)
            | Self::Unbounded => None,
        }
    }
}

/// A client `CONFIG` key that sets a deadline, with the one meaning every connector gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, EnumIter, IntoStaticStr)]
pub enum DeadlineKey {
    /// `timeout_ms`: the whole of one request or operation, name resolution, connection setup,
    /// TLS and the answer included.
    #[strum(serialize = "timeout_ms")]
    Timeout,
    /// `connect_timeout_ms`: establishing a connection, name resolution, the address attempts,
    /// TLS and the protocol's own handshake included.
    #[strum(serialize = "connect_timeout_ms")]
    ConnectTimeout,
    /// `idle_timeout_ms`: silence while waiting for peer activity on an established connection,
    /// including its liveness probe; expiry closes the connection. Each entity declares its
    /// activity and whether cancellation or handoff backpressure retains the activity deadline.
    #[strum(serialize = "idle_timeout_ms")]
    IdleTimeout,
}

impl DeadlineKey {
    /// The key, as a client's `CONFIG` writes it.
    pub fn key(self) -> &'static str {
        self.into()
    }
}

/// What bounds a wait whose client leaves its deadline key out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OmittedDeadline {
    /// The client cannot leave the key out.
    Required,
    /// The wait takes this deadline.
    Default(Duration),
    /// A bound derived from another declared client value, with its omission rule.
    Derived(&'static str),
    /// Nothing bounds the wait.
    Unbounded,
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use super::*;
    use crate::RuntimeCapability;

    /// Waits that name every key, one of them twice and the idle one first, beside one a clause
    /// bounds.
    const KEYED: DeclaredBounds = DeclaredBounds {
        buffers: &[],
        waits: &[
            DeclaredWait {
                wait: "reading a connection",
                bound: WaitBound::Key {
                    key: DeadlineKey::IdleTimeout,
                    omitted: OmittedDeadline::Unbounded,
                },
            },
            DeclaredWait {
                wait: "a connection",
                bound: WaitBound::Key {
                    key: DeadlineKey::ConnectTimeout,
                    omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
                },
            },
            DeclaredWait {
                wait: "a request",
                bound: WaitBound::Key {
                    key: DeadlineKey::Timeout,
                    omitted: OmittedDeadline::Unbounded,
                },
            },
            DeclaredWait {
                wait: "a reconnection",
                bound: WaitBound::Key {
                    key: DeadlineKey::ConnectTimeout,
                    omitted: OmittedDeadline::Required,
                },
            },
            DeclaredWait {
                wait: "an acknowledgement",
                bound: WaitBound::Clause("ACK TIMEOUT"),
            },
        ],
        driver_limits: &[],
    };

    #[test]
    fn each_deadline_key_a_wait_names_is_listed_once_in_key_order() {
        assert_eq!(
            KEYED.deadline_keys(),
            vec![
                DeadlineKey::Timeout,
                DeadlineKey::ConnectTimeout,
                DeadlineKey::IdleTimeout
            ]
        );
    }

    #[test]
    fn waits_another_bound_ends_name_no_deadline_key() {
        let unkeyed = DeclaredBounds {
            buffers: &[DeclaredBuffer {
                holds: "messages read ahead",
                bound: BufferBound::Unbounded,
            }],
            waits: &[
                DeclaredWait {
                    wait: "the next message",
                    bound: WaitBound::Unbounded,
                },
                DeclaredWait {
                    wait: "a connection",
                    bound: WaitBound::Fixed(DEFAULT_CONNECT_TIMEOUT),
                },
                DeclaredWait {
                    wait: "an answer",
                    bound: WaitBound::Driver("500 ms"),
                },
            ],
            driver_limits: &["one request at a time"],
        };
        assert_eq!(unkeyed.deadline_keys(), Vec::new());
    }

    #[test]
    fn each_named_key_is_one_stalled_peer_check_written_as_its_client_key() {
        assert_eq!(DeadlineKey::Timeout.key(), "timeout_ms");
        assert_eq!(DeadlineKey::ConnectTimeout.key(), "connect_timeout_ms");
        assert_eq!(DeadlineKey::IdleTimeout.key(), "idle_timeout_ms");
        let checks: Vec<String> = RuntimeCapability::of_bounds(KEYED)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            checks,
            vec![
                "STALLED PEER timeout_ms".to_string(),
                "STALLED PEER connect_timeout_ms".to_string(),
                "STALLED PEER idle_timeout_ms".to_string()
            ]
        );
    }
}
