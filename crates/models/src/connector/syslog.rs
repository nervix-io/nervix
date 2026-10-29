//! The syslog connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The syslog client Model, the listener source Model with its delivery mode, the
//!   collector emitter Model, and what each entity declares.
//!   Client declarations also own public configuration keys, types, confidentiality and the side
//!   that reads each key, and the frame limits both sides of the wire share.
//! - **Depends on.** The shared source and emitter declarations and the names a body references.
//! - **Must not know.** How Syslog frames are decoded or encoded, how a listener is bound or how
//!   a collector is written to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DEFAULT_CONNECT_TIMEOUT, DeadlineKey,
        DeclaredBounds, DeclaredBuffer, DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::{connector_emitters, connector_ingestors},
};
use crate::{
    AcknowledgementConfirmation, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterClientReference, EmitterDeclaration,
    EmitterEntityBody, EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind,
    IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration, IngestorEntityBody,
    IngestorEntityCapabilities, IngestorMetadataScope, IngestorReference, IngestorReferenceKind,
    PositionClass,
};

configured_client!(Syslog: unpooled, config CLIENT_CONFIG, CreateClientSyslog => SyslogConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
///
/// A listener binds `addr` and a collector sends to it. Only a listener bounds what it admits with
/// `max_message_size` and its idle connections with `idle_timeout_ms`; a listener reads each
/// frame's framing from its first byte, so only a collector writes the `framing` it names and
/// bounds its writes with `timeout_ms`. Both sides present the TLS identity in `tls_cert_file` and
/// `tls_key_file`, and verify their peer's certificate against `tls_ca_file`.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::String),
            K::CONNECT_TIMEOUT,
            K::optional("framing", V::Choice(&["octet-counting", "non-transparent"]))
                .read_by_emitters(),
            K::IDLE_TIMEOUT.read_by_ingestors(),
            K::optional("max_message_size", V::PositiveInteger).read_by_ingestors(),
            K::required("protocol", V::Choice(&["udp", "tcp", "tls"])),
            K::TIMEOUT.read_by_emitters(),
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
        ],
        &[],
        &[],
    )
};

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 791 bounds an IPv4 datagram's total length at 65,535 bytes"
    )
)]
const IPV4_DATAGRAM_BYTES: u64 = 65_535;

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 791 fixes an IPv4 header without options at 20 bytes"
    )
)]
const IPV4_HEADER_BYTES: u64 = 20;

#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "RFC 768 fixes the UDP header at 8 bytes")
)]
const UDP_HEADER_BYTES: u64 = 8;

/// The most decimal digits in an RFC 6587 octet count that either side reads or writes.
const OCTET_COUNT_DIGITS: usize = 10;

/// The frames both sides of a syslog transport carry: a collector writes only a payload that fits
/// one, and a listener decodes no frame larger than its octet count admits.
pub const SYSLOG_FRAME_LIMITS: SyslogFrameLimits = SyslogFrameLimits::new(
    IPV4_DATAGRAM_BYTES - IPV4_HEADER_BYTES - UDP_HEADER_BYTES,
    OCTET_COUNT_DIGITS,
    b'\n',
);

/// The largest syslog frames, declared once for both sides of the wire.
///
/// A UDP datagram carries one message (RFC 5426). A TCP or TLS frame begins with its payload's
/// octet count (RFC 6587, RFC 5425), or under non-transparent framing ends at a trailer byte its
/// payload therefore cannot carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyslogFrameLimits {
    datagram_payload: u64,
    octet_count_digits: usize,
    octet_count_prefix: usize,
    largest_octet_count: u64,
    non_transparent_trailer: u8,
}

impl SyslogFrameLimits {
    /// Fails constant evaluation when `octet_count_digits` decimal digits outgrow a 64-bit count.
    const fn new(
        datagram_payload: u64,
        octet_count_digits: usize,
        non_transparent_trailer: u8,
    ) -> Self {
        let mut largest_octet_count: u64 = 0;
        let mut digits = 0;
        while digits < octet_count_digits {
            largest_octet_count = match largest_octet_count.checked_mul(10) {
                Some(shifted) => match shifted.checked_add(9) {
                    Some(count) => count,
                    None => panic!("an octet count's digits must fit a 64-bit count"),
                },
                None => panic!("an octet count's digits must fit a 64-bit count"),
            };
            digits += 1;
        }
        // The space that ends an octet count follows its digits.
        let octet_count_prefix = match octet_count_digits.checked_add(1) {
            Some(prefix) => prefix,
            None => panic!("an octet count and its space must fit a usize"),
        };
        Self {
            datagram_payload,
            octet_count_digits,
            octet_count_prefix,
            largest_octet_count,
            non_transparent_trailer,
        }
    }

    /// The most payload bytes one UDP datagram carries: an IPv4 datagram less its IPv4 and UDP
    /// headers, which a collector applies whichever address family its target resolves to.
    pub const fn datagram_payload(self) -> u64 {
        self.datagram_payload
    }

    /// The most decimal digits an octet count has.
    pub const fn octet_count_digits(self) -> usize {
        self.octet_count_digits
    }

    /// The most bytes an octet count and the space after it occupy before a frame's payload.
    pub const fn octet_count_prefix(self) -> usize {
        self.octet_count_prefix
    }

    /// The largest octet count those digits write, which is the most payload bytes one
    /// octet-counted frame carries.
    pub const fn largest_octet_count(self) -> u64 {
        self.largest_octet_count
    }

    /// The byte that ends a non-transparent frame.
    pub const fn non_transparent_trailer(self) -> u8 {
        self.non_transparent_trailer
    }
}

/// The listener a syslog client's configuration binds, which every node of the cluster serves.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyslogListenerIngestor {
    pub client: ClientName,
    pub mode: SyslogIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a SYSLOG LISTENER holds and how long it waits.
const SYSLOG_LISTENER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "UDP datagrams admitted after a fixed 65,535-byte receive scratch buffer",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("max_message_size"),
                overflow: BufferOverflow::DropAndCount,
            },
        },
        DeclaredBuffer {
            holds: "TCP and TLS frame assembly across unlimited connections (max_message_size + \
                    11 bytes per connection; an oversized frame closes only that connection)",
            bound: BufferBound::Unbounded,
        },
        DeclaredBuffer {
            holds: "completed TCP and TLS frames waiting for the intake queue (one frame per \
                    connection, at most max_message_size bytes; unlimited connections)",
            bound: BufferBound::Unbounded,
        },
        DeclaredBuffer {
            holds: "frames read from TCP and TLS connections",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 64,
                    unit: "frames",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
    ],
    waits: &[
        DeclaredWait {
            wait: "the TLS handshake of an accepted connection",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "reading a connection",
            bound: WaitBound::Key {
                key: DeadlineKey::IdleTimeout,
                omitted: OmittedDeadline::Unbounded,
            },
        },
    ],
    driver_limits: &[
        "Datagrams wait in the kernel's UDP receive buffer, which drops what does not fit.",
    ],
};

/// What the connector of a SYSLOG COLLECTOR holds and how long it waits.
const SYSLOG_COLLECTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, address attempts and TLS",
            bound: WaitBound::Key {
                key: DeadlineKey::ConnectTimeout,
                omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
            },
        },
        DeclaredWait {
            wait: "writing and flushing a frame",
            bound: WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Unbounded,
            },
        },
    ],
    driver_limits: &[],
};

impl IngestorDeclaration for SyslogListenerIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        headers: None,
        metadata: IngestorMetadataScope::Syslog,
        executes_on_every_cluster_node: true,
        reference: IngestorReferenceKind::Client(ConnectorKind::Syslog),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        quiesce: &[
            DeclaredQuiesce::SUSPEND,
            DeclaredQuiesce::BUFFER,
            DeclaredQuiesce::DROP,
        ],
        locator: None,
        options: &[],
        bounds: SYSLOG_LISTENER_BOUNDS,
    };
}

impl IngestorEntityBody for SyslogListenerIngestor {
    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Client(&self.client)
    }

    fn quiesce(&self) -> &IngestQuiesceMode {
        &self.quiesce
    }

    fn quiesce_mut(&mut self) -> &mut IngestQuiesceMode {
        &mut self.quiesce
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

/// How a listener's messages are delivered. Syslog senders expect no acknowledgement, so none
/// waits on one.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum SyslogIngestMode {
    NoAck,
}

impl SyslogIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_ingestors! {
    connector Syslog = "SYSLOG";
    /// What a syslog source reads.
    pub enum SyslogIngestor;
    /// The alterations of the ingestors that read a syslog entity.
    pub enum SyslogListenerIngestorAlteration;
    /// The entities a syslog source reads.
    pub enum SyslogIngestorEntity {
        Listener(SyslogListenerIngestor) = "LISTENER", "Syslog";
    }
}

/// The collector a syslog client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyslogCollectorEmitter {
    pub mode: SyslogCollectorPublishingMode,
    pub client: ClientName,
}

impl EmitterDeclaration for SyslogCollectorEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::Syslog),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: SyslogCollectorPublishingMode::KINDS,
        locator: None,
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: SYSLOG_COLLECTOR_BOUNDS,
    };
}

impl EmitterEntityBody for SyslogCollectorEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Syslog,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Syslog = "SYSLOG";
    /// What a syslog emitter writes.
    pub enum SyslogEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum SyslogCollectorEmitterMode;
    /// The entities a syslog emitter writes.
    pub enum SyslogEmitterEntity {
        Collector(SyslogCollectorEmitter, SyslogCollectorPublishingMode) = "COLLECTOR";
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "qualifies the Syslog declarations' transport bounds, key readers and frame \
                  limits"
    )
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** The listener declaration's admission, retention and overflow contract, the side
    //!   that reads each client key and the declared frame limits.
    //! - **Depends on.** The Syslog vocabulary and shared buffer declarations.
    //! - **Must not know.** Socket I/O, frame decoding or host execution.

    use meticulous::OptionExt as _;

    use super::*;
    use crate::{ClientConfigReaders, CreateClientSyslog};

    #[test]
    fn client_declares_which_side_reads_each_key() {
        let readers: Vec<(&str, ClientConfigReaders)> = CreateClientSyslog::<u64>::DECLARATION
            .config
            .keys()
            .iter()
            .map(|key| (key.name, key.readers))
            .collect();
        assert_eq!(
            readers,
            vec![
                ("addr", ClientConfigReaders::Every),
                ("connect_timeout_ms", ClientConfigReaders::Every),
                ("framing", ClientConfigReaders::Emitters),
                ("idle_timeout_ms", ClientConfigReaders::Ingestors),
                ("max_message_size", ClientConfigReaders::Ingestors),
                ("protocol", ClientConfigReaders::Every),
                ("timeout_ms", ClientConfigReaders::Emitters),
                ("tls_ca_file", ClientConfigReaders::Every),
                ("tls_cert_file", ClientConfigReaders::Every),
                ("tls_key_file", ClientConfigReaders::Every),
            ]
        );
    }

    #[test]
    fn frame_limits_bound_datagrams_octet_counts_and_non_transparent_payloads() {
        assert_eq!(SYSLOG_FRAME_LIMITS.datagram_payload(), 65_507);
        assert_eq!(SYSLOG_FRAME_LIMITS.octet_count_digits(), 10);
        assert_eq!(SYSLOG_FRAME_LIMITS.octet_count_prefix(), 11);
        assert_eq!(SYSLOG_FRAME_LIMITS.largest_octet_count(), 9_999_999_999);
        assert_eq!(SYSLOG_FRAME_LIMITS.non_transparent_trailer(), b'\n');

        // The largest count is the largest the declared digits write: one more needs a digit more.
        let largest = SYSLOG_FRAME_LIMITS.largest_octet_count();
        let beyond = largest
            .checked_add(1)
            .assured("ten digits are far below a 64-bit count");
        assert_eq!(
            largest.to_string().len(),
            SYSLOG_FRAME_LIMITS.octet_count_digits()
        );
        assert_eq!(
            beyond.to_string().len(),
            SYSLOG_FRAME_LIMITS
                .octet_count_digits()
                .checked_add(1)
                .assured("ten digits and one more fit a usize")
        );
    }

    #[test]
    fn listener_declares_datagram_admission_and_all_stream_buffers_in_transport_order() {
        let buffers = SyslogListenerIngestor::CAPABILITIES.bounds.buffers;
        assert_eq!(buffers.len(), 4);
        assert_eq!(
            buffers[0].bound,
            BufferBound::Bounded {
                capacity: BufferCapacity::Setting("max_message_size"),
                overflow: BufferOverflow::DropAndCount,
            }
        );
        assert_eq!(buffers[1].bound, BufferBound::Unbounded);
        assert!(
            buffers[1]
                .holds
                .contains("max_message_size + 11 bytes per connection")
        );
        assert!(
            buffers[1]
                .holds
                .contains("oversized frame closes only that connection")
        );
        assert_eq!(buffers[2].bound, BufferBound::Unbounded);
        assert!(buffers[2].holds.contains("one frame per connection"));
        assert!(buffers[2].holds.contains("max_message_size"));
        assert_eq!(
            buffers[3].bound,
            BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 64,
                    unit: "frames",
                },
                overflow: BufferOverflow::Backpressure,
            }
        );
    }
}

super::publishing::emitter_modes! {
    pub enum SyslogCollectorPublishingMode {
        NoAck { retry_policy: crate::RetryPolicy } => NoAck,
    }
}
