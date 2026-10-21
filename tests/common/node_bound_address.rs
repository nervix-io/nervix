//! The fixture addresses a scenario draws for listeners its own nodes bind, kept bindable from the
//! draw until the scenario names them.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The port a node-bound fixture address uses, the socket that holds that port from its
//!   draw, the check when a scenario first names the address with the move to a fresh port when
//!   another socket took it, what a move reports of the taken port's holders, and giving back every
//!   port the address drew.
//! - **Depends on.** The port pool, the boundary's TCP and UDP sockets, and on Linux the kernel's
//!   socket tables under `/proc`.
//! - **Must not know.** Nodes, clusters, statements, or scenario steps.
//!
//! # Why a drawn port needs keeping
//!
//! The pool closes the socket it read a drawn port from. A node binds a ZeroMQ PULL or syslog
//! listener address only when its ingestor starts after `START`, seconds later, and until then the
//! operating system may give the port to any other socket on the host: a harness listener bound to
//! port zero, an outgoing connection, or a port Docker publishes for a container. A holder that
//! keeps the port fails every bind the ingestor retries, and the ingestor's reconnect delay soon
//! outgrows the step that waits for it.
//!
//! # The hold
//!
//! From the draw until the scenario gives the port back, a TCP socket bound to the port on the
//! wildcard address with `SO_REUSEADDR`, which never listens, holds it. Linux picks no port a bound
//! socket holds for another socket bound to port zero or for an outgoing connection, and it refuses
//! an explicit bind of the port without `SO_REUSEADDR`. A listener that sets `SO_REUSEADDR`, as
//! every Tokio listener and so a node's ZeroMQ and syslog stream listeners do, still binds and
//! listens beside the hold on any loopback address, after a restart as much as the first time.
//!
//! UDP has no such hold: a node's syslog UDP socket binds without `SO_REUSEADDR`, so any UDP socket
//! on the port, a hold among them, would refuse it.
//!
//! # The check at first use
//!
//! The hold stops neither a listener that binds the port explicitly with `SO_REUSEADDR`, as
//! Docker's proxy does for a port it publishes, nor a UDP socket. So the first time a scenario
//! names the address, in the statement that creates the client its node binds, the port is bound
//! once the way that node will bind it and closed again. A bind refused with `AddrInUse` means
//! another socket took the port: the address moves to a freshly drawn and held port before the
//! statement names it, and the move reports what the kernel lists on the taken port. Any other
//! failure of that bind is returned, and fails the scenario. Once named, the address never moves,
//! because a model carries it and its node may already hold the port.

use std::{
    collections::BTreeSet,
    fmt, fs, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use error_stack::Report;
use futures_util::FutureExt as _;
use nervix_primitives::net::{TcpSocket, UdpSocket};
use thiserror::Error;

use super::port_pool::{next_port, release_test_ports};

/// The loopback address statements and senders name. A syslog listener in a cluster binds each
/// node's own loopback address instead, with the same port.
const HOST: Ipv4Addr = Ipv4Addr::LOCALHOST;

/// The most ports one address draws, its first included. A drawn port is free when the pool hands
/// it out and held at once, so even a second taken port is remote: the limit only ends an address
/// whose every port is refused for one reason. A policy input.
pub(crate) const DRAW_LIMIT: usize = 8;

/// What a node binds at a fixture address, which decides how the address is written and which
/// transports its first use checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NodeBoundListener {
    /// A ZeroMQ PULL socket its client binds, over TCP.
    ZeroMqPull,
    /// A syslog listener, over UDP or over TCP, as its client declares.
    Syslog,
}

impl NodeBoundListener {
    /// The address a statement names for this listener on `port`.
    fn address(self, port: u16) -> String {
        match self {
            Self::ZeroMqPull => format!("tcp://{HOST}:{port}"),
            Self::Syslog => format!("{HOST}:{port}"),
        }
    }

    /// Whether its node may bind the port over UDP as well as over TCP.
    fn may_bind_udp(self) -> bool {
        match self {
            Self::ZeroMqPull => false,
            Self::Syslog => true,
        }
    }
}

impl fmt::Display for NodeBoundListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMqPull => formatter.write_str("ZeroMQ ingest"),
            Self::Syslog => formatter.write_str("syslog ingest"),
        }
    }
}

/// Why a node-bound address has no port its node can bind.
#[derive(Debug, Error)]
pub(crate) enum NodeBoundAddressError {
    #[error("the port pool drew no port for the {listener} address")]
    Draw { listener: NodeBoundListener },
    #[error("port {port} drawn for the {listener} address could not be held")]
    Hold {
        listener: NodeBoundListener,
        port: u16,
    },
    #[error("binding port {port} of the {listener} address as its node would failed")]
    Bind {
        listener: NodeBoundListener,
        port: u16,
    },
    #[error("another socket took every port the {listener} address drew: {drawn:?}")]
    Taken {
        listener: NodeBoundListener,
        drawn: Vec<u16>,
    },
}

/// One fixture address a scenario's nodes bind, from its draw until the scenario gives its ports
/// back.
pub(crate) struct NodeBoundAddress {
    listener: NodeBoundListener,
    current: HeldPort,
    /// Every port the address drew, those it moved off included. They go back together at the
    /// scenario's end, because the socket that took a port may still hold it.
    drawn: Vec<u16>,
    naming: Naming,
}

/// Whether a scenario has named an address yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Naming {
    /// Nothing names the address, so it may still move.
    Unnamed,
    /// A statement or a sender named it: a model carries it and its node may hold the port, so it
    /// stays where it is.
    Named,
}

/// The address a scenario names, with every move its first use made.
pub(crate) struct NamedAddress {
    pub(crate) address: String,
    pub(crate) moves: Vec<AddressMove>,
}

/// One move of an address off a port another socket took.
pub(crate) struct AddressMove {
    pub(crate) from: u16,
    pub(crate) to: u16,
    /// How binding the taken port as its node would was refused.
    pub(crate) refusal: io::Error,
    /// What the kernel listed on the taken port once the address had let it go.
    pub(crate) holders: PortHolders,
}

impl fmt::Display for AddressMove {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "moved from port {} to port {}: binding port {} as its node would failed with '{}'; {}",
            self.from, self.to, self.from, self.refusal, self.holders
        )
    }
}

/// A drawn port and the socket that holds it.
struct HeldPort {
    port: u16,
    /// Bound to the port on the wildcard address with `SO_REUSEADDR`, and never listening. Its only
    /// work is to exist until the address lets the port go.
    _hold: TcpSocket,
}

impl HeldPort {
    /// Draws ports into `drawn` until one can be held, within [`DRAW_LIMIT`] ports in all.
    fn draw(
        listener: NodeBoundListener,
        drawn: &mut Vec<u16>,
    ) -> Result<Self, Report<NodeBoundAddressError>> {
        while drawn.len() < DRAW_LIMIT {
            let port = next_port().map_err(|source| {
                Report::new(source).change_context(NodeBoundAddressError::Draw { listener })
            })?;
            drawn.push(port);
            match hold(port) {
                Ok(hold) => return Ok(Self { port, _hold: hold }),
                // Another socket took the port between the pool's draw and the hold, so the next
                // draw finds another.
                Err(refusal) if refusal.kind() == io::ErrorKind::AddrInUse => {}
                Err(source) => {
                    return Err(Report::new(source)
                        .change_context(NodeBoundAddressError::Hold { listener, port }));
                }
            }
        }
        Err(Report::new(NodeBoundAddressError::Taken {
            listener,
            drawn: drawn.clone(),
        }))
    }
}

/// Binds a socket to `port` on the wildcard address with `SO_REUSEADDR`, without listening.
fn hold(port: u16) -> io::Result<TcpSocket> {
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))?;
    Ok(socket)
}

impl NodeBoundAddress {
    /// Draws a port for `listener` and holds it. A draw that fails gives every port it took back
    /// to the pool.
    pub(crate) fn draw(listener: NodeBoundListener) -> Result<Self, Report<NodeBoundAddressError>> {
        let mut drawn = Vec::new();
        match HeldPort::draw(listener, &mut drawn) {
            Ok(current) => Ok(Self {
                listener,
                current,
                drawn,
                naming: Naming::Unnamed,
            }),
            Err(error) => {
                release_test_ports(&drawn);
                Err(error)
            }
        }
    }

    /// The port the address holds now, which a scenario reads without naming the address.
    pub(crate) fn port(&self) -> u16 {
        self.current.port
    }

    /// The address as a statement or a sender names it.
    ///
    /// The first time, `bind_as_its_node` binds the port once as its node will. A port another
    /// socket took moves the address to a freshly drawn and held port, and the result reports each
    /// move. Any other failure of that bind is returned and leaves the address unnamed where it
    /// is. From then on the address stays where it is, and nothing binds its port again.
    pub(crate) fn name(
        &mut self,
        mut bind_as_its_node: impl FnMut(NodeBoundListener, u16) -> io::Result<()>,
    ) -> Result<NamedAddress, Report<NodeBoundAddressError>> {
        let mut moves = Vec::new();
        if self.naming == Naming::Named {
            return Ok(NamedAddress {
                address: self.address(),
                moves,
            });
        }
        loop {
            let refusal = match bind_as_its_node(self.listener, self.current.port) {
                Ok(()) => break,
                Err(refusal) => refusal,
            };
            if refusal.kind() != io::ErrorKind::AddrInUse {
                return Err(
                    Report::new(refusal).change_context(NodeBoundAddressError::Bind {
                        listener: self.listener,
                        port: self.current.port,
                    }),
                );
            }
            let fresh = HeldPort::draw(self.listener, &mut self.drawn)?;
            let taken = std::mem::replace(&mut self.current, fresh);
            let from = taken.port;
            // The hold goes first, so the listing names only what took the port.
            drop(taken);
            moves.push(AddressMove {
                from,
                to: self.current.port,
                refusal,
                holders: PortHolders::list(from),
            });
        }
        self.naming = Naming::Named;
        Ok(NamedAddress {
            address: self.address(),
            moves,
        })
    }

    fn address(&self) -> String {
        self.listener.address(self.current.port)
    }

    /// Lets the hold go and gives every port the address drew back to the pool, once nothing the
    /// scenario started binds them any more.
    pub(crate) fn release(self) {
        let Self { current, drawn, .. } = self;
        drop(current);
        release_test_ports(&drawn);
    }
}

/// Binds `port` once as a node that binds `listener` will, and closes it again.
///
/// TCP binds with `SO_REUSEADDR` on the wildcard address, as a Tokio listener does. Beside the hold
/// it is refused only where another socket listens on the port, on any address, or holds the port
/// without `SO_REUSEADDR`, which is where a node's own bind is refused too. A syslog listener's
/// port is bound over UDP as well, without `SO_REUSEADDR`, as its node's UDP socket binds.
pub(crate) fn bind_as_its_node(listener: NodeBoundListener, port: u16) -> io::Result<()> {
    let wildcard = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
    let tcp = TcpSocket::new_v4()?;
    tcp.set_reuseaddr(true)?;
    tcp.bind(wildcard)?;
    drop(tcp);
    if !listener.may_bind_udp() {
        return Ok(());
    }
    // A literal address needs no lookup, so the bind completes on its first poll and the check
    // stays as synchronous as the expansion that runs it.
    let Some(udp) = UdpSocket::bind(wildcard).now_or_never() else {
        return Err(io::Error::other(
            "binding a literal UDP address did not complete on its first poll",
        ));
    };
    drop(udp?);
    Ok(())
}

/// What the kernel's socket tables list on one port, as a move reports it.
pub(crate) struct PortHolders {
    port: u16,
    sockets: Vec<ListedSocket>,
    /// The socket inodes this process holds, which tell its own sockets apart from another
    /// process's.
    held_here: io::Result<BTreeSet<u64>>,
    unreadable: Vec<UnreadableTable>,
}

/// A socket table the listing could not read, as on a host without `/proc` or without IPv6.
struct UnreadableTable {
    table: SocketTable,
    error: io::Error,
}

impl PortHolders {
    /// Lists the sockets every table holds on `port`.
    pub(crate) fn list(port: u16) -> Self {
        let mut sockets = Vec::new();
        let mut unreadable = Vec::new();
        for table in SocketTable::ALL {
            let rows = match fs::read_to_string(table.path()) {
                Ok(rows) => rows,
                Err(error) => {
                    unreadable.push(UnreadableTable { table, error });
                    continue;
                }
            };
            for row in rows.lines() {
                let Some(socket) = ListedSocket::parse(table, row) else {
                    continue;
                };
                if socket.local.port() == port {
                    sockets.push(socket);
                }
            }
        }
        Self {
            port,
            sockets,
            held_here: this_process_socket_inodes(),
            unreadable,
        }
    }

    /// Whether this process holds `socket`, as far as its descriptors can be read.
    fn owner(&self, socket: &ListedSocket) -> &'static str {
        let Ok(held_here) = &self.held_here else {
            return "held by an unknown process";
        };
        if held_here.contains(&socket.inode) {
            "held by this process"
        } else {
            "held by another process"
        }
    }
}

impl fmt::Display for PortHolders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.sockets.is_empty() {
            write!(
                formatter,
                "the kernel listed no socket on port {} any more",
                self.port
            )?;
        } else {
            write!(formatter, "the kernel listed on port {}:", self.port)?;
            for socket in &self.sockets {
                write!(formatter, " [{socket}, {}]", self.owner(socket))?;
            }
        }
        if let Err(error) = &self.held_here {
            write!(
                formatter,
                "; this process's descriptors could not be read: {error}"
            )?;
        }
        for table in &self.unreadable {
            write!(
                formatter,
                "; {} could not be read: {}",
                table.table.path(),
                table.error
            )?;
        }
        Ok(())
    }
}

/// The inodes of the sockets this process holds, read from its descriptor links.
fn this_process_socket_inodes() -> io::Result<BTreeSet<u64>> {
    let mut inodes = BTreeSet::new();
    for entry in fs::read_dir("/proc/self/fd")? {
        let entry = entry?;
        // A descriptor closed while the listing reads it no longer belongs to this process.
        let Ok(target) = fs::read_link(entry.path()) else {
            continue;
        };
        let Some(target) = target.to_str() else {
            continue;
        };
        let Some(inode) = target.strip_prefix("socket:[") else {
            continue;
        };
        let Some(inode) = inode.strip_suffix(']') else {
            continue;
        };
        let Ok(inode) = inode.parse::<u64>() else {
            continue;
        };
        inodes.insert(inode);
    }
    Ok(inodes)
}

/// One of the kernel's socket tables under `/proc/net`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SocketTable {
    Tcp,
    Tcp6,
    Udp,
    Udp6,
}

impl SocketTable {
    const ALL: [Self; 4] = [Self::Tcp, Self::Tcp6, Self::Udp, Self::Udp6];

    fn path(self) -> &'static str {
        match self {
            Self::Tcp => "/proc/net/tcp",
            Self::Tcp6 => "/proc/net/tcp6",
            Self::Udp => "/proc/net/udp",
            Self::Udp6 => "/proc/net/udp6",
        }
    }

    fn lists_tcp(self) -> bool {
        match self {
            Self::Tcp | Self::Tcp6 => true,
            Self::Udp | Self::Udp6 => false,
        }
    }
}

impl fmt::Display for SocketTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp => formatter.write_str("tcp"),
            Self::Tcp6 => formatter.write_str("tcp6"),
            Self::Udp => formatter.write_str("udp"),
            Self::Udp6 => formatter.write_str("udp6"),
        }
    }
}

/// The columns of a socket table row the listing reads.
const LOCAL_ADDRESS_COLUMN: usize = 1;
const STATE_COLUMN: usize = 3;
const UID_COLUMN: usize = 7;
const INODE_COLUMN: usize = 9;

/// One socket a table lists.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ListedSocket {
    pub(crate) table: SocketTable,
    pub(crate) local: SocketAddr,
    /// The kernel's state code, which a TCP table and a UDP table name differently.
    pub(crate) state: u8,
    pub(crate) uid: u32,
    pub(crate) inode: u64,
}

impl ListedSocket {
    /// Reads one row of `table`. A row that does not parse, the table's header among them, is
    /// nothing.
    pub(crate) fn parse(table: SocketTable, row: &str) -> Option<Self> {
        let columns: Vec<&str> = row.split_whitespace().collect();
        let local = table_address(columns.get(LOCAL_ADDRESS_COLUMN)?)?;
        let state = u8::from_str_radix(columns.get(STATE_COLUMN)?, 16).ok()?;
        let uid = columns.get(UID_COLUMN)?.parse().ok()?;
        let inode = columns.get(INODE_COLUMN)?.parse().ok()?;
        Some(Self {
            table,
            local,
            state,
            uid,
            inode,
        })
    }

    /// The state as the kernel names it: a TCP socket's connection state, or whether a UDP socket
    /// is connected.
    fn state_name(&self) -> Option<&'static str> {
        if !self.table.lists_tcp() {
            return match self.state {
                0x01 => Some("connected"),
                0x07 => Some("unconnected"),
                _ => None,
            };
        }
        match self.state {
            0x01 => Some("ESTABLISHED"),
            0x02 => Some("SYN_SENT"),
            0x03 => Some("SYN_RECV"),
            0x04 => Some("FIN_WAIT1"),
            0x05 => Some("FIN_WAIT2"),
            0x06 => Some("TIME_WAIT"),
            0x07 => Some("CLOSE"),
            0x08 => Some("CLOSE_WAIT"),
            0x09 => Some("LAST_ACK"),
            0x0A => Some("LISTEN"),
            0x0B => Some("CLOSING"),
            _ => None,
        }
    }
}

impl fmt::Display for ListedSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {} ", self.table, self.local)?;
        match self.state_name() {
            Some(state) => formatter.write_str(state)?,
            None => write!(formatter, "state {:#04x}", self.state)?,
        }
        write!(formatter, " uid {} inode {}", self.uid, self.inode)
    }
}

/// Reads a table's `ADDRESS:PORT` column, both in hexadecimal. The kernel prints an address as the
/// native-endian 32-bit words of its network-order bytes, so each word's native bytes are the
/// address's bytes.
fn table_address(column: &str) -> Option<SocketAddr> {
    let (address, port) = column.split_once(':')?;
    let port = u16::from_str_radix(port, 16).ok()?;
    let ip = match address.len() {
        8 => IpAddr::V4(Ipv4Addr::from(table_word(address)?.to_ne_bytes())),
        32 => {
            let mut octets = [0_u8; 16];
            let (words, _) = address.as_bytes().as_chunks::<8>();
            let (octet_words, _) = octets.as_chunks_mut::<4>();
            for (octet_word, word) in octet_words.iter_mut().zip(words) {
                let word = std::str::from_utf8(word).ok()?;
                *octet_word = table_word(word)?.to_ne_bytes();
            }
            IpAddr::V6(Ipv6Addr::from(octets))
        }
        _ => return None,
    };
    Some(SocketAddr::new(ip, port))
}

fn table_word(hex: &str) -> Option<u32> {
    u32::from_str_radix(hex, 16).ok()
}
