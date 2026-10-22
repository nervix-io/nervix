//! The Prometheus connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Prometheus client Model with the `CONFIG` keys it declares and the API root
//!   its address names, the query source Model with its delivery mode and response bound, and what
//!   each declares.
//! - **Depends on.** The shared source and client declarations, the HTTP transport's request
//!   timeout, the URL Standard parser, the domain clock's period, the byte size a limit holds and
//!   the names a body references.
//! - **Must not know.** The Prometheus HTTP API's endpoints and answers, how a client is built, or
//!   how a query is evaluated on its cadence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{collections::BTreeMap, str::FromStr as _};

use error_stack::{Report, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeadlineKey, DeclaredBounds, DeclaredBuffer,
        DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::connector_ingestors,
};
use crate::{
    AcknowledgementConfirmation, ByteSizeUnit, ClientConfigEntry, ClientDeadline,
    ClientDeadlineError, ClientName, ConnectorKind, DeclaredDelivery, DeclaredQuiesce,
    DomainClockPeriod, IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration,
    IngestorEntityBody, IngestorEntityCapabilities, IngestorMetadataScope, IngestorReference,
    IngestorReferenceKind, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
    PayloadSizeLimit, PositionClass,
};

configured_client!(Prometheus: unpooled, config Nervix, CreateClientPrometheus => PrometheusConfigEntry);

/// The keys a Prometheus client's `CONFIG { … }` block may set. Every one is Nervix's own, so a
/// client that sets any other key is refused when it is created.
///
/// A key is matched without regard to ASCII case, as the client reads it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::AsRefStr,
    strum::Display,
    strum::EnumIter,
    strum::EnumString,
)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum PrometheusClientKey {
    /// The [`PrometheusApiRoot`] every query is sent beneath. Required.
    Addr,
    /// The [`ClientDeadline`] that bounds each query. Required.
    TimeoutMs,
    /// A PEM certificate authority that HTTPS trusts in addition to the client's own roots.
    TlsCaFile,
    /// The PEM certificate chain the client presents over HTTPS, which [`Self::TlsKeyFile`]
    /// accompanies.
    TlsCertFile,
    /// The PEM private key of [`Self::TlsCertFile`].
    TlsKeyFile,
}

/// Why a Prometheus client's `CONFIG { … }` block cannot configure a query source. Each error
/// names the key at fault and never quotes a value, which can name a credential's file.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PrometheusClientConfigError {
    #[error("'{key}' is not a key this client reads")]
    UndeclaredKey { key: String },
    #[error("'{key}' is set more than once")]
    RepeatedKey { key: PrometheusClientKey },
    #[error("'{key}' is required")]
    MissingKey { key: PrometheusClientKey },
    #[error("'{}' is not an API root: {issue}", PrometheusClientKey::Addr)]
    InvalidAddress { issue: PrometheusApiRootError },
    #[error(
        "'{}' is not a usable request timeout: {issue}",
        PrometheusClientKey::TimeoutMs
    )]
    InvalidTimeout { issue: ClientDeadlineError },
    #[error(
        "'{}' and '{}' must be set together",
        PrometheusClientKey::TlsCertFile,
        PrometheusClientKey::TlsKeyFile
    )]
    UnpairedTlsIdentity,
}

/// Why a Prometheus client's `addr` is not the root of an HTTP API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PrometheusApiRootError {
    #[error("it is not a URL")]
    NotUrl,
    #[error("the URL's scheme is not http or https")]
    Scheme,
    #[error("the URL has a query string")]
    Query,
    #[error("the URL has a fragment")]
    Fragment,
}

/// The root of the Prometheus HTTP API a client's `addr` names: an `http` or `https` URL whose
/// path is the prefix the API is served under, such as `/monitoring` behind a reverse proxy.
///
/// It carries no query string, which every query replaces with its own, and no fragment, which no
/// request carries, so a query goes to exactly the root the client names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrometheusApiRoot(Url);

impl PrometheusApiRoot {
    /// The root `addr` names, as the URL Standard parses it.
    pub fn parse(addr: &str) -> Result<Self, Report<PrometheusApiRootError>> {
        let url = Url::parse(addr).change_context(PrometheusApiRootError::NotUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Report::new(PrometheusApiRootError::Scheme));
        }
        if url.query().is_some() {
            return Err(Report::new(PrometheusApiRootError::Query));
        }
        if url.fragment().is_some() {
            return Err(Report::new(PrometheusApiRootError::Fragment));
        }
        Ok(Self(url))
    }

    /// The API root a Prometheus client's `CONFIG { … }` names, once the block sets only keys
    /// [`PrometheusClientKey`] declares, each at most once: `addr` and `timeout_ms` present and
    /// valid, and `tls_cert_file` and `tls_key_file` together or not at all.
    ///
    /// The registry holds a client to this when it is created, and a source holds the client's
    /// rendered configuration to it again when it opens.
    pub fn of_client_config(
        config: &[ClientConfigEntry],
    ) -> Result<Self, Report<PrometheusClientConfigError>> {
        let mut values = BTreeMap::new();
        for entry in config {
            let Ok(key) = PrometheusClientKey::from_str(&entry.key) else {
                return Err(Report::new(PrometheusClientConfigError::UndeclaredKey {
                    key: entry.key.clone(),
                }));
            };
            if values.insert(key, entry.value.as_str()).is_some() {
                return Err(Report::new(PrometheusClientConfigError::RepeatedKey {
                    key,
                }));
            }
        }

        let Some(addr) = values.get(&PrometheusClientKey::Addr) else {
            return Err(Report::new(PrometheusClientConfigError::MissingKey {
                key: PrometheusClientKey::Addr,
            }));
        };
        let root = match Self::parse(addr) {
            Ok(root) => root,
            Err(report) => {
                // The key's error quotes the rule the value breaks, so its one message states the
                // whole reason, and the parse's report stays beneath it.
                let issue = *report.current_context();
                return Err(
                    report.change_context(PrometheusClientConfigError::InvalidAddress { issue })
                );
            }
        };

        let Some(timeout) = values.get(&PrometheusClientKey::TimeoutMs) else {
            return Err(Report::new(PrometheusClientConfigError::MissingKey {
                key: PrometheusClientKey::TimeoutMs,
            }));
        };
        if let Err(report) = ClientDeadline::parse(timeout) {
            let issue = *report.current_context();
            return Err(
                report.change_context(PrometheusClientConfigError::InvalidTimeout { issue })
            );
        }

        let presents_certificate = values.contains_key(&PrometheusClientKey::TlsCertFile);
        let presents_key = values.contains_key(&PrometheusClientKey::TlsKeyFile);
        if presents_certificate != presents_key {
            return Err(Report::new(
                PrometheusClientConfigError::UnpairedTlsIdentity,
            ));
        }
        Ok(root)
    }

    /// The URL of the root.
    pub fn url(&self) -> &Url {
        &self.0
    }
}

/// The most bytes of one query response a poll reads where its statement leaves `MAX BODY SIZE`
/// out.
///
/// A policy input: room for tens of thousands of samples with their labels, while the response a
/// poll holds for decoding stays a small share of a node's memory.
pub const PROMETHEUS_DEFAULT_MAX_BODY_SIZE: PayloadSizeLimit =
    match PayloadSizeLimit::new(nonzero_ext::nonzero!(16u64), ByteSizeUnit::MiB) {
        Some(limit) => limit,
        None => panic!("16 MiB fits in a 64-bit byte count"),
    };

/// One PromQL query a Prometheus client evaluates on a cadence of domain time.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PrometheusQueryIngestor {
    pub client: ClientName,
    pub query: String,
    /// `MAX BODY SIZE <bytes>`, where the statement writes it. A statement that leaves it out reads
    /// at most [`PROMETHEUS_DEFAULT_MAX_BODY_SIZE`].
    pub max_body_size: Option<PayloadSizeLimit>,
    pub every: DomainClockPeriod,
    pub mode: PrometheusIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl PrometheusQueryIngestor {
    /// The most bytes of one query response a poll reads: the statement's `MAX BODY SIZE`, or the
    /// declared default where it writes none.
    pub fn max_body_size_in_effect(&self) -> PayloadSizeLimit {
        self.max_body_size
            .unwrap_or(PROMETHEUS_DEFAULT_MAX_BODY_SIZE)
    }
}

/// What the connector of a PROMETHEUS QUERY holds and how long it waits.
const PROMETHEUS_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[
        DeclaredBuffer {
            holds: "the query response",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Setting("MAX BODY SIZE"),
                overflow: BufferOverflow::FailPoll,
            },
        },
        DeclaredBuffer {
            holds: "samples the host takes in as one intake batch",
            bound: BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: 1024,
                    unit: "samples",
                },
                overflow: BufferOverflow::Backpressure,
            },
        },
    ],
    waits: &[DeclaredWait {
        wait: "a query: name resolution, connection, TLS and the response",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Required,
        },
    }],
    driver_limits: &["Response heads above 100 headers or 417,792 bytes are refused."],
};

/// A query answer carries each sample's labels, which its payload holds, and no transport headers
/// of its own, so a query source reads none.
impl IngestorDeclaration for PrometheusQueryIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        headers: None,
        metadata: IngestorMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Prometheus),
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::NO_ACK],
        quiesce: &[DeclaredQuiesce::SUSPEND, DeclaredQuiesce::BUFFER],
        locator: None,
        options: &[
            OptionDeclaration {
                keywords: "QUERY",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Text,
                    example: "QUERY 'up'",
                },
            },
            OptionDeclaration {
                keywords: "MAX BODY SIZE",
                presence: OptionPresence::Optional {
                    default: Some("16MiB"),
                },
                form: OptionForm::Value {
                    value: OptionValue::Size,
                    example: "MAX BODY SIZE 16MiB",
                },
            },
            OptionDeclaration {
                keywords: "EVERY",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Duration,
                    example: "EVERY 15s",
                },
            },
        ],
        bounds: PROMETHEUS_SOURCE_BOUNDS,
    };
}

impl IngestorEntityBody for PrometheusQueryIngestor {
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

/// How a query's results are delivered. A result is read again on the next evaluation, so none
/// waits on an acknowledgement.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PrometheusIngestMode {
    NoAck,
}

impl PrometheusIngestMode {
    pub const fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::NoAck => IngestAcknowledgement::Unacknowledged,
        }
    }
}

connector_ingestors! {
    connector Prometheus = "PROMETHEUS";
    /// What a Prometheus source reads.
    pub enum PrometheusIngestor;
    /// The alterations of the ingestors that read a Prometheus entity.
    pub enum PrometheusIngestorAlteration;
    /// The entities a Prometheus source reads.
    pub enum PrometheusIngestorEntity {
        Query(PrometheusQueryIngestor) = "QUERY", "Prometheus";
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use strum::IntoEnumIterator as _;

    use super::*;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    /// The smallest configuration a Prometheus client can be created with.
    fn minimal() -> Vec<ClientConfigEntry> {
        vec![
            entry("addr", "http://prometheus.example.com:9090"),
            entry("timeout_ms", "5000"),
        ]
    }

    fn refusal(config: &[ClientConfigEntry]) -> Report<PrometheusClientConfigError> {
        let Err(report) = PrometheusApiRoot::of_client_config(config) else {
            panic!("the configuration {config:?} must be refused");
        };
        report
    }

    /// The default the declaration publishes for `MAX BODY SIZE` is the bound a statement that
    /// leaves it out reads with.
    #[test]
    fn the_declared_response_bound_default_is_the_one_in_effect() {
        let [_, max_body_size, _] = PrometheusQueryIngestor::CAPABILITIES.options else {
            panic!("a query source declares QUERY, MAX BODY SIZE and EVERY");
        };
        assert_eq!(max_body_size.keywords, "MAX BODY SIZE");
        let OptionPresence::Optional {
            default: Some(default),
        } = &max_body_size.presence
        else {
            panic!("MAX BODY SIZE is optional with a declared default");
        };
        let declared = default
            .parse::<PayloadSizeLimit>()
            .assured("the declared default is a size literal");
        assert_eq!(declared, PROMETHEUS_DEFAULT_MAX_BODY_SIZE);
    }

    #[test]
    fn every_declared_key_is_read_as_written_in_any_ascii_case() {
        for key in PrometheusClientKey::iter() {
            let written = key.as_ref();
            assert_eq!(PrometheusClientKey::from_str(written), Ok(key));
            assert_eq!(
                PrometheusClientKey::from_str(&written.to_ascii_uppercase()),
                Ok(key)
            );
            assert_eq!(key.to_string(), written);
        }
        assert!(PrometheusClientKey::from_str("url").is_err());
        assert!(PrometheusClientKey::from_str("timeout").is_err());
    }

    #[test]
    fn an_api_root_is_an_http_or_https_url_without_a_query_or_fragment() {
        for addr in [
            "http://prometheus:9090",
            "https://prometheus.example.com",
            "https://prometheus.example.com/monitoring/",
            "http://[::1]:9090/prometheus",
            "http://user:secret@prometheus:9090",
        ] {
            let root = PrometheusApiRoot::parse(addr).assured(
                "each table entry is an http or https URL with neither query nor fragment",
            );
            assert_eq!(
                root.url(),
                &Url::parse(addr).assured("the same text parsed above")
            );
        }

        for (addr, expected) in [
            ("prometheus:9090", PrometheusApiRootError::Scheme),
            (
                "ftp://prometheus.example.com",
                PrometheusApiRootError::Scheme,
            ),
            ("127.0.0.1:9090", PrometheusApiRootError::NotUrl),
            ("http://", PrometheusApiRootError::NotUrl),
            ("", PrometheusApiRootError::NotUrl),
            (
                "http://prometheus:9090/?stale=true",
                PrometheusApiRootError::Query,
            ),
            ("http://prometheus:9090/?", PrometheusApiRootError::Query),
            (
                "http://prometheus:9090/#graph",
                PrometheusApiRootError::Fragment,
            ),
        ] {
            let Err(report) = PrometheusApiRoot::parse(addr) else {
                panic!("'{addr}' must not be an API root");
            };
            assert_eq!(report.current_context(), &expected, "{addr}");
        }
    }

    #[test]
    fn a_client_config_names_its_api_root_once_every_key_is_declared_and_valid() {
        let root = PrometheusApiRoot::of_client_config(&minimal())
            .assured("the minimal configuration sets addr and a positive timeout_ms");
        assert_eq!(root.url().as_str(), "http://prometheus.example.com:9090/");

        let full = [
            entry("ADDR", "https://prometheus.example.com/monitoring"),
            entry("Timeout_Ms", "30000"),
            entry("tls_ca_file", "{{ certs }}/ca.pem"),
            entry("tls_cert_file", "{{ certs }}/client.pem"),
            entry("tls_key_file", "{{ certs }}/client-key.pem"),
        ];
        let root = PrometheusApiRoot::of_client_config(&full)
            .assured("every declared key is set once, in any case, with a valid value");
        assert_eq!(
            root.url().as_str(),
            "https://prometheus.example.com/monitoring"
        );
    }

    #[test]
    fn a_client_config_refuses_a_key_it_does_not_declare_or_sets_twice() {
        let mut undeclared = minimal();
        undeclared.push(entry("timeout", "5000"));
        assert_eq!(
            refusal(&undeclared).current_context(),
            &PrometheusClientConfigError::UndeclaredKey {
                key: "timeout".to_string(),
            }
        );

        let mut repeated = minimal();
        repeated.push(entry("TIMEOUT_MS", "1000"));
        assert_eq!(
            refusal(&repeated).current_context(),
            &PrometheusClientConfigError::RepeatedKey {
                key: PrometheusClientKey::TimeoutMs,
            }
        );
    }

    #[test]
    fn a_client_config_requires_addr_and_timeout_ms() {
        assert_eq!(
            refusal(&[entry("timeout_ms", "5000")]).current_context(),
            &PrometheusClientConfigError::MissingKey {
                key: PrometheusClientKey::Addr,
            }
        );
        assert_eq!(
            refusal(&[entry("addr", "http://prometheus:9090")]).current_context(),
            &PrometheusClientConfigError::MissingKey {
                key: PrometheusClientKey::TimeoutMs,
            }
        );
    }

    #[test]
    fn a_client_config_keeps_the_rule_an_invalid_value_breaks_beneath_its_key() {
        let address = refusal(&[
            entry("addr", "http://prometheus:9090/?stale=true"),
            entry("timeout_ms", "5000"),
        ]);
        assert_eq!(
            address.current_context(),
            &PrometheusClientConfigError::InvalidAddress {
                issue: PrometheusApiRootError::Query,
            }
        );
        assert_eq!(
            address.downcast_ref::<PrometheusApiRootError>(),
            Some(&PrometheusApiRootError::Query)
        );
        assert_eq!(
            address.to_string(),
            "'addr' is not an API root: the URL has a query string"
        );

        let timeout = refusal(&[
            entry("addr", "http://prometheus:9090"),
            entry("timeout_ms", "0"),
        ]);
        assert_eq!(
            timeout.current_context(),
            &PrometheusClientConfigError::InvalidTimeout {
                issue: ClientDeadlineError::NotPositiveWholeMilliseconds,
            }
        );
        assert_eq!(
            timeout.downcast_ref::<ClientDeadlineError>(),
            Some(&ClientDeadlineError::NotPositiveWholeMilliseconds)
        );
    }

    #[test]
    fn a_client_config_presents_a_tls_identity_only_as_a_pair() {
        for half in ["tls_cert_file", "tls_key_file"] {
            let mut config = minimal();
            config.push(entry(half, "/etc/nervix/tls/client.pem"));
            assert_eq!(
                refusal(&config).current_context(),
                &PrometheusClientConfigError::UnpairedTlsIdentity,
                "{half}"
            );
        }
        let mut trust_only = minimal();
        trust_only.push(entry("tls_ca_file", "/etc/nervix/tls/ca.pem"));
        PrometheusApiRoot::of_client_config(&trust_only)
            .assured("a CA file alone trusts an authority without presenting an identity");
    }
}
