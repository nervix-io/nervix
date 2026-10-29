//! The transport headers and typed metadata an ingestor message carries into the host.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The header trait an ingestor message implements over its own borrowed headers, the
//!   retained copy of headers that must outlive their message, and the typed metadata row an ingestor
//!   reads per message, one shape for each metadata scope the vocabulary declares.
//! - **Depends on.** The vocabulary's metadata scopes and the standard library.
//! - **Must not know.** The Arrow columns the host projects these rows into, or the VM namespaces
//!   programs read them through.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_models::{IngestorMetadataScope, Timestamp};
use nervix_primitives::sync::Arc;

/// Transport headers of one ingestor message, visited in the order its entity's declared header model
/// keeps them: as the message carries them for `ordered_repeats`, each name's values together for
/// `repeats_by_name`, one value per name for `last_wins`, and names in ascending byte order for
/// `sorted_by_name`.
///
/// Connectors implement this over their own borrowed message so a group's header builders
/// are appended from the ingestor directly, without an owned header vector per message.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the ingestor host visits transport headers for each admitted record"
    )
)]
pub trait IngestMessageHeaders: Send + Sync {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies a synchronous header visitor; local \
                                   visitor bodies remain analyzed")
    )]
    fn visit(&self, visit: &mut dyn FnMut(&str, &str));
}

/// A ingestor message that carries no transport headers.
pub struct NoIngestHeaders;

impl IngestMessageHeaders for NoIngestHeaders {
    fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
}

/// Transport headers copied out of an ingestor message so they outlive it.
///
/// Only the paths that append after their ingestor message is gone retain headers: the
/// quiesce buffer replays payloads long after the message was dropped, and a WebSocket
/// session carries its handshake headers into every later frame. Every other connector
/// appends from the borrowed message instead.
#[derive(Clone, Debug)]
pub struct RetainedIngestHeaders(Vec<(String, String)>);

impl RetainedIngestHeaders {
    /// Copies the headers an ingestor message carries right now.
    pub fn capture(headers: &dyn IngestMessageHeaders) -> Self {
        let mut retained = Vec::new();
        headers.visit(&mut |name, value| retained.push((name.to_string(), value.to_string())));
        Self(retained)
    }

    /// The headers of an ingestor that carries none.
    pub fn none() -> Self {
        Self(Vec::new())
    }
}

impl IngestMessageHeaders for RetainedIngestHeaders {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in &self.0 {
            visit(name, value);
        }
    }
}

/// One message's ingest metadata, read from its ingestor message.
///
/// The variant must match the metadata scope the ingestor started with; the fields borrow the
/// ingestor message so appending a row copies bytes into Arrow buffers and nothing else, and copying a
/// row copies only those borrows.
#[derive(Clone, Copy)]
pub enum IngestMetadataRow<'a> {
    Http {
        status: u16,
        polled_at: Timestamp,
        headers: &'a dyn IngestMessageHeaders,
    },
    Websockets {
        frame_type: nervix_models::WebsocketsFrameType,
        // A buffered frame can outlive the exact connection whose upgrade supplied these fields.
        headers: &'a Arc<RetainedIngestHeaders>,
    },
    Kafka {
        topic: &'a str,
        partition: i32,
        offset: i64,
        headers: &'a dyn IngestMessageHeaders,
    },
    Syslog {
        peer_addr: std::net::SocketAddr,
    },
    Headers {
        headers: &'a dyn IngestMessageHeaders,
    },
}

impl IngestMetadataRow<'_> {
    /// The metadata scope this row carries the fields of, which is the one builder set it can be
    /// appended into.
    pub fn scope(&self) -> IngestorMetadataScope {
        match self {
            Self::Http { .. } => IngestorMetadataScope::Http,
            Self::Websockets { .. } => IngestorMetadataScope::Websockets,
            Self::Kafka { .. } => IngestorMetadataScope::Kafka,
            Self::Syslog { .. } => IngestorMetadataScope::Syslog,
            Self::Headers { .. } => IngestorMetadataScope::Headers,
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::IngestorMetadataScope;

    use super::{IngestMetadataRow, NoIngestHeaders};

    #[test]
    fn a_metadata_row_names_the_scope_whose_fields_it_carries() {
        let kafka = IngestMetadataRow::Kafka {
            topic: "orders",
            partition: 3,
            offset: 42,
            headers: &NoIngestHeaders,
        };
        assert_eq!(kafka.scope(), IngestorMetadataScope::Kafka);

        let syslog = IngestMetadataRow::Syslog {
            peer_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 514)),
        };
        assert_eq!(syslog.scope(), IngestorMetadataScope::Syslog);

        let headers = IngestMetadataRow::Headers {
            headers: &NoIngestHeaders,
        };
        assert_eq!(headers.scope(), IngestorMetadataScope::Headers);
    }
}
