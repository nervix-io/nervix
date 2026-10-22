//! The WebSocket connections a web console page made, as the page itself made and let go of them.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The record a console page's connection recorder keeps, read into typed values, and
//!   what a record shows: whether the page ever held two session connections at once, which
//!   consoles its session connections reached around its leader redirect, which consoles its
//!   backup downloads and restores reached, and whether its session connected again after a loss.
//! - **Depends on.** The console WebSocket paths of the client wire contract and URL parsing.
//! - **Must not know.** How the console drives its session, or how a node serves it.
//!
//! The recorder numbers what it sees with one counter the page's event loop advances, so a record
//! is ordered by the page's own actions however loaded the machine is. The console lets go of a
//! session connection before it constructs the next one, because a console has one session and
//! connects it again only once the previous connection ended. A second session would construct its
//! connection while the first is still held, so that order alone tells a reconnect of the console's
//! session from a second session.

use std::fmt;

use nervix_client_wire::websocket::{
    CONSOLE_BACKUP_DOWNLOAD_PATH, CONSOLE_RESTORE_PATH, CONSOLE_SESSION_PATH,
};
use serde::Deserialize;
use url::Url;

/// The console a connection reached or a node serves, by the host and port of its address.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub(crate) struct ConsoleAddress {
    host: String,
    port: u16,
}

/// Why an address names no console.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConsoleAddressError {
    #[error("'{address}' is not a URL")]
    NotUrl {
        address: String,
        #[source]
        source: url::ParseError,
    },
    #[error("'{address}' names no host")]
    NoHost { address: String },
    #[error("'{address}' names no port")]
    NoPort { address: String },
}

impl TryFrom<String> for ConsoleAddress {
    type Error = ConsoleAddressError;

    /// The console `address` names: a node's page address, or the origin a page connection was
    /// opened to.
    fn try_from(address: String) -> Result<Self, Self::Error> {
        let url = match Url::parse(&address) {
            Ok(url) => url,
            Err(source) => return Err(ConsoleAddressError::NotUrl { address, source }),
        };
        let Some(host) = url.host_str() else {
            return Err(ConsoleAddressError::NoHost { address });
        };
        let Some(port) = url.port_or_known_default() else {
            return Err(ConsoleAddressError::NoPort { address });
        };
        Ok(Self {
            host: host.to_string(),
            port,
        })
    }
}

impl fmt::Display for ConsoleAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.host, self.port)
    }
}

/// What a console WebSocket carries, by the path the page opened it at.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub(crate) enum ConnectionPurpose {
    /// The console's session.
    Session,
    /// A backup archive download.
    BackupDownload,
    /// A restore's archive stream.
    Restore,
    /// A WebSocket at a path no console call is served at.
    Other(String),
}

impl From<String> for ConnectionPurpose {
    fn from(path: String) -> Self {
        match path.as_str() {
            CONSOLE_SESSION_PATH => Self::Session,
            CONSOLE_BACKUP_DOWNLOAD_PATH => Self::BackupDownload,
            CONSOLE_RESTORE_PATH => Self::Restore,
            _ => Self::Other(path),
        }
    }
}

impl fmt::Display for ConnectionPurpose {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session => formatter.write_str("session"),
            Self::BackupDownload => formatter.write_str("backup download"),
            Self::Restore => formatter.write_str("restore"),
            Self::Other(path) => write!(formatter, "WebSocket at {path}"),
        }
    }
}

/// One WebSocket the page constructed. Its moments are numbers of the recorder's counter.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RecordedConnection {
    /// The console the page opened the connection to.
    address: ConsoleAddress,
    #[serde(rename = "path")]
    purpose: ConnectionPurpose,
    /// When the page constructed the WebSocket.
    opened: u64,
    /// When the connection opened, or `None` while it never did.
    connected: Option<u64>,
    /// When the page closed the WebSocket or saw it close, or `None` while the page holds it.
    released: Option<u64>,
}

/// Everything the recorder saw, in the order the page did it.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsoleConnectionRecord {
    /// Every WebSocket the page constructed, in the order it constructed them. A scenario's console
    /// opens a handful, so the checks below read the whole record.
    connections: Vec<RecordedConnection>,
    /// When a scenario made the page lose its session connection, in the order it did.
    losses: Vec<u64>,
}

/// The two kinds of stream the backup controls open beside the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackupStream {
    Download,
    Restore,
}

impl fmt::Display for BackupStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Download => formatter.write_str("backup download"),
            Self::Restore => formatter.write_str("restore"),
        }
    }
}

/// How a record contradicts what the console guarantees of its connections. A connection is named
/// by its place in the record, the first being 1.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ConnectionRecordViolation {
    #[error(
        "the page constructed session connection {later} while it still held session connection \
         {earlier}"
    )]
    SessionsHeldAtOnce { earlier: usize, later: usize },
    #[error("the page made no session connection")]
    NoSessionConnection,
    #[error("the first session connection, {connection}, reached {reached} instead of {expected}")]
    SessionStartedElsewhere {
        connection: usize,
        reached: ConsoleAddress,
        expected: ConsoleAddress,
    },
    #[error("no session connection reached {leader}, the console it was to be redirected to")]
    NeverRedirected { leader: ConsoleAddress },
    #[error(
        "session connection {connection} reached {reached}, which is neither {origin}, where the \
         session started, nor {leader}, where it was redirected"
    )]
    SessionReachedStranger {
        connection: usize,
        reached: ConsoleAddress,
        origin: ConsoleAddress,
        leader: ConsoleAddress,
    },
    #[error(
        "session connection {connection} reached {reached} after the session was redirected to \
         {leader}"
    )]
    SessionLeftLeader {
        connection: usize,
        reached: ConsoleAddress,
        leader: ConsoleAddress,
    },
    #[error("the page opened no {stream} connection")]
    NoBackupStream { stream: BackupStream },
    #[error("{stream} connection {connection} reached {reached} instead of {leader}")]
    BackupStreamElsewhere {
        stream: BackupStream,
        connection: usize,
        reached: ConsoleAddress,
        leader: ConsoleAddress,
    },
    #[error("the scenario never made the page lose its session connection")]
    NoLoss,
}

impl ConsoleConnectionRecord {
    /// Every connection with its place in the record, the first being 1.
    fn numbered(&self) -> impl Iterator<Item = (usize, &RecordedConnection)> {
        (1_usize..).zip(&self.connections)
    }

    /// Checks that the page held at most one session connection at any moment: it let go of each
    /// session connection before it constructed the next.
    pub(crate) fn one_session_connection_at_a_time(&self) -> Result<(), ConnectionRecordViolation> {
        let mut held: Option<(usize, &RecordedConnection)> = None;
        for (number, connection) in self.numbered() {
            if connection.purpose != ConnectionPurpose::Session {
                continue;
            }
            if let Some((earlier, previous)) = held {
                let released_first = match previous.released {
                    Some(released) => released < connection.opened,
                    None => false,
                };
                if !released_first {
                    return Err(ConnectionRecordViolation::SessionsHeldAtOnce {
                        earlier,
                        later: number,
                    });
                }
            }
            held = Some((number, connection));
        }
        Ok(())
    }

    /// Checks that the session's connections reached `origin`, where the page was opened, until
    /// the console followed its leader redirect, and reached `leader` from then on, reconnects
    /// included.
    pub(crate) fn session_followed_redirect(
        &self,
        origin: &ConsoleAddress,
        leader: &ConsoleAddress,
    ) -> Result<(), ConnectionRecordViolation> {
        let mut redirected = false;
        let mut first = true;
        for (number, connection) in self.numbered() {
            if connection.purpose != ConnectionPurpose::Session {
                continue;
            }
            let reached = &connection.address;
            if first && reached != origin {
                return Err(ConnectionRecordViolation::SessionStartedElsewhere {
                    connection: number,
                    reached: reached.clone(),
                    expected: origin.clone(),
                });
            }
            first = false;
            if reached == leader {
                redirected = true;
                continue;
            }
            if redirected {
                return Err(ConnectionRecordViolation::SessionLeftLeader {
                    connection: number,
                    reached: reached.clone(),
                    leader: leader.clone(),
                });
            }
            if reached != origin {
                return Err(ConnectionRecordViolation::SessionReachedStranger {
                    connection: number,
                    reached: reached.clone(),
                    origin: origin.clone(),
                    leader: leader.clone(),
                });
            }
        }
        if first {
            return Err(ConnectionRecordViolation::NoSessionConnection);
        }
        if !redirected {
            return Err(ConnectionRecordViolation::NeverRedirected {
                leader: leader.clone(),
            });
        }
        Ok(())
    }

    /// Checks that the backup controls opened at least one download and one restore connection,
    /// and that every one of them reached `leader`.
    pub(crate) fn backup_streams_reached(
        &self,
        leader: &ConsoleAddress,
    ) -> Result<(), ConnectionRecordViolation> {
        let mut downloaded = false;
        let mut restored = false;
        for (number, connection) in self.numbered() {
            let stream = match connection.purpose {
                ConnectionPurpose::BackupDownload => BackupStream::Download,
                ConnectionPurpose::Restore => BackupStream::Restore,
                ConnectionPurpose::Session | ConnectionPurpose::Other(_) => continue,
            };
            if connection.address != *leader {
                return Err(ConnectionRecordViolation::BackupStreamElsewhere {
                    stream,
                    connection: number,
                    reached: connection.address.clone(),
                    leader: leader.clone(),
                });
            }
            match stream {
                BackupStream::Download => downloaded = true,
                BackupStream::Restore => restored = true,
            }
        }
        if !downloaded {
            return Err(ConnectionRecordViolation::NoBackupStream {
                stream: BackupStream::Download,
            });
        }
        if !restored {
            return Err(ConnectionRecordViolation::NoBackupStream {
                stream: BackupStream::Restore,
            });
        }
        Ok(())
    }

    /// Whether a session connection the page constructed after the latest loss has opened to
    /// `console`.
    pub(crate) fn session_reconnected_after_loss(
        &self,
        console: &ConsoleAddress,
    ) -> Result<bool, ConnectionRecordViolation> {
        let Some(loss) = self.losses.last() else {
            return Err(ConnectionRecordViolation::NoLoss);
        };
        for connection in &self.connections {
            let reconnected = connection.purpose == ConnectionPurpose::Session
                && connection.address == *console
                && connection.opened > *loss
                && connection.connected.is_some();
            if reconnected {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl fmt::Display for ConsoleConnectionRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (number, connection) in self.numbered() {
            write!(
                formatter,
                "\n  {number}: {} to {}, constructed at {}",
                connection.purpose, connection.address, connection.opened
            )?;
            match connection.connected {
                Some(connected) => write!(formatter, ", opened at {connected}")?,
                None => formatter.write_str(", never opened")?,
            }
            match connection.released {
                Some(released) => write!(formatter, ", let go at {released}")?,
                None => formatter.write_str(", held")?,
            }
        }
        for loss in &self.losses {
            write!(formatter, "\n  session connection lost at {loss}")?;
        }
        Ok(())
    }
}
