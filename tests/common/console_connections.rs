//! The WebSocket connections a web console page made, as the page itself made and let go of them,
//! and the leaders the nodes reported to the page on them.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The record a console page's connection recorder keeps, read into typed values, the
//!   leaders the frames of that record report, and what a record shows: whether the page ever held
//!   two session connections at once, whether its session started where the page was opened and
//!   followed its leader redirect, whether every later connection reached a node reported to the
//!   page as the leader, and whether its session connected again where it lost its connection.
//! - **Depends on.** The console WebSocket paths, codecs and messages of the client wire contract,
//!   and URL parsing.
//! - **Must not know.** How the console drives its session, or how a node serves it.
//!
//! The recorder numbers what it sees with one counter the page's event loop advances, so a record
//! is ordered by the page's own actions however loaded the machine is. The console lets go of a
//! session connection before it constructs the next one, because a console has one session and
//! connects it again only once the previous connection ended. A second session would construct its
//! connection while the first is still held, so that order alone tells a reconnect of the console's
//! session from a second session.
//!
//! The nodes tell the page which node leads on the page's own connections. The leader opens every
//! console session it serves by reporting that it leads. Any other node ends a console session with
//! a redirect that names the leader's console, or no console while it knows no leader, and answers a
//! command, a backup download or a restore it cannot serve because it does not lead with such a
//! redirect too. Each report therefore comes first or last on its connection, where the recorder
//! keeps the frames, since the console lets a session go as soon as a redirect names another
//! leader. The recorder numbers each frame as the page receives it, before the page can act on it,
//! so a report comes before every connection the page makes because of it. The console makes a
//! connection where a report sent it, or again where it connected before, so once its session
//! followed its first redirect, every connection it makes reaches a node reported to it as the
//! leader before it made that connection. However often leadership moves, a move only adds the new
//! leader to those reports, while a connection to the page's origin or to any node no report named
//! still breaks the check.

use std::{collections::BTreeMap, fmt};

use bytes::Bytes;
use error_stack::Report;
use nervix_client_wire::{
    BackupDownloadMessage, CommandDisposition, LeaderRedirect, Leadership, NodeEndpoints, Reply,
    ReplyBody, RestoreDisposition, RestoreReply, ServerEvent, ServerMessage, SessionEndReason,
    SessionLimits,
    websocket::{
        CONSOLE_BACKUP_DOWNLOAD_PATH, CONSOLE_RESTORE_PATH, CONSOLE_SESSION_PATH,
        ClientBackupDownloadWebSocketCodec, ClientRestoreWebSocketCodec, ClientWebSocketCodec,
        WebSocketData,
    },
};
use serde::Deserialize;
use url::Url;

/// The console a connection reached or a node serves, by the host and port of its address.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
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

impl TryFrom<&Url> for ConsoleAddress {
    type Error = ConsoleAddressError;

    /// The console `url` names: a node's page address, the origin a page connection was opened to,
    /// or the web console a redirect names.
    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let Some(host) = url.host_str() else {
            return Err(ConsoleAddressError::NoHost {
                address: url.to_string(),
            });
        };
        let Some(port) = url.port_or_known_default() else {
            return Err(ConsoleAddressError::NoPort {
                address: url.to_string(),
            });
        };
        Ok(Self {
            host: host.to_string(),
            port,
        })
    }
}

impl TryFrom<String> for ConsoleAddress {
    type Error = ConsoleAddressError;

    /// The console `address` names.
    fn try_from(address: String) -> Result<Self, Self::Error> {
        let url = match Url::parse(&address) {
            Ok(url) => url,
            Err(source) => return Err(ConsoleAddressError::NotUrl { address, source }),
        };
        Self::try_from(&url)
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

impl ConnectionPurpose {
    /// Which node a frame this connection's call received reports leads, or `None` when it reports
    /// nothing about it.
    fn reported_leader(
        &self,
        frame: &ReceivedFrame,
    ) -> Result<Option<ReportedLeader>, Report<FrameReadFailure>> {
        let data = WebSocketData::Binary(Bytes::copy_from_slice(&frame.bytes));
        match self {
            Self::Session => ReportedLeader::in_session_frame(data),
            Self::BackupDownload => ReportedLeader::in_download_frame(data),
            Self::Restore => ReportedLeader::in_restore_reply(data),
            Self::Other(_) => Ok(None),
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

/// One binary frame a connection delivered, as the page received it.
#[derive(Debug, Clone, Deserialize)]
struct ReceivedFrame {
    /// When the page received the frame.
    received: u64,
    bytes: Vec<u8>,
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
    /// The binary frames the connection delivered first and last that the recorder keeps, in the
    /// order the page received them. Every report of the leader is among them: it is far smaller
    /// than the recorder's size bound, and it comes at one end of its connection.
    frames: Vec<ReceivedFrame>,
}

impl RecordedConnection {
    /// The console of the node `frame` reports leads, or `None` when it reports nothing about it.
    fn reported_leader(
        &self,
        frame: &ReceivedFrame,
    ) -> Result<Option<ConsoleAddress>, Report<FrameReadFailure>> {
        let reported = self.purpose.reported_leader(frame)?;
        let Some(reported) = reported else {
            return Ok(None);
        };
        // The leader that serves the session reports itself: the console this connection reached.
        let ReportedLeader::Redirect(console) = reported else {
            return Ok(Some(self.address.clone()));
        };
        match ConsoleAddress::try_from(&console) {
            Ok(leader) => Ok(Some(leader)),
            Err(error) => Err(Report::new(error).change_context(FrameReadFailure::NoConsole)),
        }
    }

    /// Whether this is a session connection the page held at `moment`: it had constructed the
    /// connection and not yet let go of it.
    fn session_held_at(&self, moment: u64) -> bool {
        if self.purpose != ConnectionPurpose::Session || self.opened > moment {
            return false;
        }
        match self.released {
            Some(released) => released > moment,
            None => true,
        }
    }
}

/// Which node a frame reports leads.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReportedLeader {
    /// The node the connection reached: the leader that serves the console's session.
    Serving,
    /// The node whose web console a redirect names.
    Redirect(Url),
}

impl ReportedLeader {
    /// The leader `leader` describes, when it advertises a web console.
    fn at_console(leader: NodeEndpoints) -> Option<Self> {
        let console = leader.web_console_uri?;
        Some(Self::Redirect(console))
    }

    /// The leader `redirect` names, when it names a leader that advertises a web console.
    fn named_by(redirect: LeaderRedirect) -> Option<Self> {
        let leader = redirect.leader?;
        Self::at_console(leader)
    }

    /// The leader a command's outcome names, when the node could not run the command because it
    /// does not lead.
    fn of_disposition(disposition: CommandDisposition) -> Option<Self> {
        let CommandDisposition::NotLeader(redirect) = disposition else {
            return None;
        };
        Self::named_by(redirect)
    }

    /// The leader a leadership observation names: the serving node itself, or the remote leader.
    fn of_leadership(leadership: Leadership) -> Option<Self> {
        match leadership {
            Leadership::ServingNode(_) => Some(Self::Serving),
            Leadership::Remote(leader) => Self::at_console(leader),
            Leadership::Unknown => None,
        }
    }

    /// The leader the reason a session ends names, when the node ended it to redirect the session.
    fn of_session_end(reason: SessionEndReason) -> Option<Self> {
        match reason {
            SessionEndReason::LeaderRedirect(redirect) => Self::named_by(redirect),
            SessionEndReason::ServerShuttingDown | SessionEndReason::ProtocolViolated { .. } => {
                None
            }
        }
    }

    /// The leader a session message names: the leader's report that it serves the session, the
    /// redirect that ends a session another node may not serve, or the redirect a command the node
    /// could not run is answered with.
    fn of_session_message(message: ServerMessage) -> Option<Self> {
        match message {
            ServerMessage::Event(ServerEvent::Leadership(observed)) => {
                Self::of_leadership(observed.leadership)
            }
            ServerMessage::Event(ServerEvent::SessionEnding(ending)) => {
                Self::of_session_end(ending.reason)
            }
            ServerMessage::Reply(Reply {
                body: ReplyBody::Command(outcome),
                ..
            }) => Self::of_disposition(outcome.disposition),
            // Every other event and reply, and a part of a reply too large for one frame, names no
            // leader.
            ServerMessage::Event(_) | ServerMessage::Reply(_) | ServerMessage::TransferPart(_) => {
                None
            }
        }
    }

    /// What a frame of a console session reports.
    fn in_session_frame(data: WebSocketData) -> Result<Option<Self>, Report<FrameReadFailure>> {
        let codec = ClientWebSocketCodec::new(SessionLimits::DEFAULT);
        let frame = codec
            .decode(data)
            .map_err(|error| error.change_context(FrameReadFailure::NotFrame))?;
        let message = ServerMessage::decode(&frame)
            .map_err(|error| error.change_context(FrameReadFailure::NotMessage))?;
        Ok(Self::of_session_message(message))
    }

    /// What a frame of a backup download reports: the redirect a node that does not lead and does
    /// not retain the archive answers with.
    fn in_download_frame(data: WebSocketData) -> Result<Option<Self>, Report<FrameReadFailure>> {
        let codec = ClientBackupDownloadWebSocketCodec::new(SessionLimits::DEFAULT);
        let frame = codec
            .decode(data)
            .map_err(|error| error.change_context(FrameReadFailure::NotFrame))?;
        let message = BackupDownloadMessage::decode(&frame)
            .map_err(|error| error.change_context(FrameReadFailure::NotMessage))?;
        let BackupDownloadMessage::NotLeader(redirect) = message else {
            return Ok(None);
        };
        Ok(Self::named_by(redirect))
    }

    /// What the reply to a restore stream reports: the redirect of a restore the node could not run
    /// because it does not lead.
    fn in_restore_reply(data: WebSocketData) -> Result<Option<Self>, Report<FrameReadFailure>> {
        let codec = ClientRestoreWebSocketCodec::new(SessionLimits::DEFAULT);
        let frame = codec
            .decode(data)
            .map_err(|error| error.change_context(FrameReadFailure::NotFrame))?;
        let reply = RestoreReply::decode(&frame)
            .map_err(|error| error.change_context(FrameReadFailure::NotMessage))?;
        let RestoreDisposition::Outcome(outcome) = reply.disposition else {
            return Ok(None);
        };
        Ok(Self::of_disposition(outcome.disposition))
    }
}

/// Why a frame the page received does not read as a frame of its connection's call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FrameReadFailure {
    #[error("the payload is not a frame")]
    NotFrame,
    #[error("the frame is not a message of the connection's call")]
    NotMessage,
    #[error("the frame names the leader's web console by an address that names no console")]
    NoConsole,
}

/// A frame of the record that does not read as a frame of its connection's call, so the record
/// cannot tell which node it reported leads. The connection is named by its place in the record,
/// the first being 1.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "{purpose} connection {connection} received a frame at {received} that does not read as a \
     frame of its call"
)]
pub(crate) struct UnreadableFrame {
    pub(crate) connection: usize,
    pub(crate) purpose: ConnectionPurpose,
    pub(crate) received: u64,
}

/// The console of each node reported to the page as the leader, with when the page first received
/// such a report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReportedLeaders {
    first_reported: BTreeMap<ConsoleAddress, u64>,
}

impl ReportedLeaders {
    /// Notes that the page received a report naming `leader` at `received`.
    fn report(&mut self, leader: ConsoleAddress, received: u64) {
        let first = self.first_reported.entry(leader).or_insert(received);
        if received < *first {
            *first = received;
        }
    }

    /// Whether a node had reported `console` as the leader's before `moment`.
    fn reported_before(&self, console: &ConsoleAddress, moment: u64) -> bool {
        let Some(first) = self.first_reported.get(console) else {
            return false;
        };
        *first < moment
    }
}

impl fmt::Display for ReportedLeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.first_reported.is_empty() {
            return formatter.write_str("\n  no node was reported as the leader");
        }
        for (console, first) in &self.first_reported {
            write!(formatter, "\n  {console} first reported at {first}")?;
        }
        Ok(())
    }
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
        "session connection {connection} reached {reached}, which no node had reported as the \
         leader before the page made that connection"
    )]
    SessionReachedUnreportedNode {
        connection: usize,
        reached: ConsoleAddress,
    },
    #[error("the page opened no {stream} connection")]
    NoBackupStream { stream: BackupStream },
    #[error(
        "{stream} connection {connection} reached {reached}, which no node had reported as the \
         leader before the page made that connection"
    )]
    BackupStreamReachedUnreportedNode {
        stream: BackupStream,
        connection: usize,
        reached: ConsoleAddress,
    },
    #[error("the scenario never made the page lose its session connection")]
    NoLoss,
    #[error("the page held no session connection when the scenario made it lose one, at {loss}")]
    NoSessionHeldAtLoss { loss: u64 },
    #[error(
        "no session connection the page made after it lost its connection to {lost} opened there"
    )]
    NotReconnected { lost: ConsoleAddress },
}

impl ConsoleConnectionRecord {
    /// Every connection with its place in the record, the first being 1.
    fn numbered(&self) -> impl Iterator<Item = (usize, &RecordedConnection)> {
        (1_usize..).zip(&self.connections)
    }

    /// The consoles of the nodes reported to the page as the leader on its connections, each with
    /// when the page first received such a report.
    pub(crate) fn reported_leaders(&self) -> Result<ReportedLeaders, Report<UnreadableFrame>> {
        let mut leaders = ReportedLeaders::default();
        for (number, connection) in self.numbered() {
            for frame in &connection.frames {
                let reported = connection.reported_leader(frame).map_err(|error| {
                    error.change_context(UnreadableFrame {
                        connection: number,
                        purpose: connection.purpose.clone(),
                        received: frame.received,
                    })
                })?;
                if let Some(leader) = reported {
                    leaders.report(leader, frame.received);
                }
            }
        }
        Ok(leaders)
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

    /// Checks that the session's connections reached `origin`, where the page was opened, until the
    /// console followed its leader redirect to `leader`, and that from that redirect on, reconnects
    /// included, every session connection reached a node `leaders` reported as the leader before
    /// the page made the connection.
    pub(crate) fn session_followed_reported_leaders(
        &self,
        leaders: &ReportedLeaders,
        origin: &ConsoleAddress,
        leader: &ConsoleAddress,
    ) -> Result<(), ConnectionRecordViolation> {
        let mut first = true;
        let mut redirected = false;
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
            if !redirected {
                if reached == origin {
                    continue;
                }
                if reached != leader {
                    return Err(ConnectionRecordViolation::SessionReachedStranger {
                        connection: number,
                        reached: reached.clone(),
                        origin: origin.clone(),
                        leader: leader.clone(),
                    });
                }
                redirected = true;
            }
            if !leaders.reported_before(reached, connection.opened) {
                return Err(ConnectionRecordViolation::SessionReachedUnreportedNode {
                    connection: number,
                    reached: reached.clone(),
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
    /// and that each of them reached a node `leaders` reported as the leader before the page made
    /// the connection.
    pub(crate) fn backup_streams_reached_reported_leaders(
        &self,
        leaders: &ReportedLeaders,
    ) -> Result<(), ConnectionRecordViolation> {
        let mut downloaded = false;
        let mut restored = false;
        for (number, connection) in self.numbered() {
            let stream = match connection.purpose {
                ConnectionPurpose::BackupDownload => BackupStream::Download,
                ConnectionPurpose::Restore => BackupStream::Restore,
                ConnectionPurpose::Session | ConnectionPurpose::Other(_) => continue,
            };
            if !leaders.reported_before(&connection.address, connection.opened) {
                return Err(
                    ConnectionRecordViolation::BackupStreamReachedUnreportedNode {
                        stream,
                        connection: number,
                        reached: connection.address.clone(),
                    },
                );
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

    /// Checks that a session connection the page constructed after the latest loss has opened to
    /// the console of the session connection the page lost: the console connects its session again
    /// where its redirects took it.
    pub(crate) fn session_reconnected_where_it_was_lost(
        &self,
    ) -> Result<(), ConnectionRecordViolation> {
        let Some(loss) = self.losses.last() else {
            return Err(ConnectionRecordViolation::NoLoss);
        };
        let Some(lost) = self.session_connection_held_at(*loss) else {
            return Err(ConnectionRecordViolation::NoSessionHeldAtLoss { loss: *loss });
        };
        for connection in &self.connections {
            let reconnected = connection.purpose == ConnectionPurpose::Session
                && connection.address == lost.address
                && connection.opened > *loss
                && connection.connected.is_some();
            if reconnected {
                return Ok(());
            }
        }
        Err(ConnectionRecordViolation::NotReconnected {
            lost: lost.address.clone(),
        })
    }

    /// The session connection the page held at `moment`, or `None` when it held none then. The
    /// page holds at most one at a time, which `one_session_connection_at_a_time` checks.
    fn session_connection_held_at(&self, moment: u64) -> Option<&RecordedConnection> {
        self.connections
            .iter()
            .find(|connection| connection.session_held_at(moment))
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
