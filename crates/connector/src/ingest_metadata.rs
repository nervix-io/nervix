//! The transport headers and typed metadata a source message carries into the host.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The header trait a source message implements over its own borrowed headers, the
//!   retained copy of headers that must outlive their message, and the typed metadata row a source
//!   reads per message, with its Kafka, syslog and header scopes.
//! - **Depends on.** The standard library alone.
//! - **Must not know.** The Arrow columns the host projects these rows into, or the VM namespaces
//!   programs read them through.

/// Transport headers of one source message, visited in arrival order.
///
/// Connectors implement this over their own borrowed message so a group's header builders
/// are appended from the source directly, without an owned header vector per message.
pub trait IngestMessageHeaders: Send + Sync {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str));
}

/// A source message that carries no transport headers.
pub struct NoIngestHeaders;

impl IngestMessageHeaders for NoIngestHeaders {
    fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
}

/// Transport headers copied out of a source message so they outlive it.
///
/// Only the paths that append after their source message is gone retain headers: the
/// quiesce buffer replays payloads long after the message was dropped, and a WebSocket
/// session carries its handshake headers into every later frame. Every other connector
/// appends from the borrowed message instead.
#[derive(Clone, Debug)]
pub struct RetainedIngestHeaders(Vec<(String, String)>);

impl RetainedIngestHeaders {
    /// Copies the headers a source message carries right now.
    pub fn capture(headers: &dyn IngestMessageHeaders) -> Self {
        let mut retained = Vec::new();
        headers.visit(&mut |name, value| retained.push((name.to_string(), value.to_string())));
        Self(retained)
    }

    /// The headers of a source that carries none.
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

/// One message's ingest metadata, read from its source message.
///
/// The variant must match the metadata kind the ingestor started with; the fields borrow the
/// source message so appending a row copies bytes into Arrow buffers and nothing else.
pub enum IngestMetadataRow<'a> {
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
