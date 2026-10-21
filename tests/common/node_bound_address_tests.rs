//! Regressions for the addresses a scenario's own nodes bind: the hold that keeps a drawn port off
//! every ephemeral pick and out of a node's way, the check at first use that moves an address off a
//! port another socket took, and the listing a move reports.
//!
//! Layer: test harness.
//! - **Owns.** The sockets that stand in for a node's listeners and for whatever takes a drawn port,
//!   and the socket table rows the listing reads.
//! - **Depends on.** The node-bound address, the port pool, and the boundary's TCP and UDP sockets.
//! - **Must not know.** Scenario state, nodes, or statements.

use std::{
    collections::BTreeSet,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::net::{TcpListener, TcpSocket, UdpSocket};

use crate::{
    node_bound_address::{
        DRAW_LIMIT, ListedSocket, NamedAddress, NodeBoundAddress, NodeBoundAddressError,
        NodeBoundListener, SocketTable, bind_as_its_node,
    },
    port_pool::{PortPoolError, release_test_ports, reserve},
};

/// How many addresses the ephemeral-pick regression holds at once.
const HELD_ADDRESSES: usize = 32;
/// How many ports the ephemeral-pick regression asks the operating system for. Without the hold,
/// several dozen of them would land on a held port.
const EPHEMERAL_PICKS: usize = 20_000;

fn draw(listener: NodeBoundListener) -> NodeBoundAddress {
    match NodeBoundAddress::draw(listener) {
        Ok(address) => address,
        Err(error) => panic!("the pool draws and holds a port: {error:#}"),
    }
}

fn named(
    address: &mut NodeBoundAddress,
    bind: impl FnMut(NodeBoundListener, u16) -> io::Result<()>,
) -> NamedAddress {
    match address.name(bind) {
        Ok(named) => named,
        Err(error) => panic!("naming the address failed: {error:#}"),
    }
}

/// A listener bound as Tokio binds one, with `SO_REUSEADDR`: a node's, or one Docker's proxy or
/// another server binds explicitly.
async fn listen(host: Ipv4Addr, port: u16) -> TcpListener {
    match TcpListener::bind(SocketAddr::from((host, port))).await {
        Ok(listener) => listener,
        Err(error) => panic!("a listener with SO_REUSEADDR binds {host}:{port}: {error}"),
    }
}

/// Whether the pool still holds `port`: a draw that only ever finds it is refused while it does.
fn is_reserved(port: u16) -> bool {
    match reserve(1, || Ok(port)) {
        Ok(reserved) => {
            release_test_ports(&reserved);
            false
        }
        Err(PortPoolError::Exhausted { .. }) => true,
        Err(error) => panic!("a draw that only finds port {port} ends in another way: {error}"),
    }
}

#[test]
fn a_held_port_is_no_ephemeral_pick() {
    let addresses: Vec<NodeBoundAddress> = (0..HELD_ADDRESSES)
        .map(|_| draw(NodeBoundListener::ZeroMqPull))
        .collect();
    let held: BTreeSet<u16> = addresses.iter().map(NodeBoundAddress::port).collect();

    // Half the picks set SO_REUSEADDR, as a Tokio listener does, and Linux serves those from the
    // lower half of its range, where the pool draws too.
    for pick in 0..EPHEMERAL_PICKS {
        let socket = TcpSocket::new_v4().assured("the host creates TCP sockets");
        if pick.is_multiple_of(2) {
            socket
                .set_reuseaddr(true)
                .assured("a fresh socket takes SO_REUSEADDR");
        }
        socket
            .bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .assured("loopback has free ephemeral ports");
        let port = socket
            .local_addr()
            .assured("a bound socket knows its address")
            .port();
        assert!(
            !held.contains(&port),
            "the operating system picked held port {port}"
        );
    }

    for address in addresses {
        address.release();
    }
}

#[nervix_primitives::test]
async fn a_node_listener_binds_beside_the_hold_on_any_loopback_address_and_after_a_restart() {
    let address = draw(NodeBoundListener::Syslog);
    let port = address.port();

    // A syslog listener binds each node's own loopback address, and binds it again when its node
    // or its ingestor restarts.
    for host in [Ipv4Addr::LOCALHOST, Ipv4Addr::new(127, 0, 0, 2)] {
        for _ in 0..2 {
            drop(listen(host, port).await);
        }
    }

    address.release();
}

#[nervix_primitives::test]
async fn naming_a_free_address_binds_its_port_once_and_keeps_it() {
    let mut address = draw(NodeBoundListener::Syslog);
    let port = address.port();
    let mut binds = 0_u32;
    let mut counted_bind = |listener, port| {
        binds = binds
            .checked_add(1)
            .assured("the regression names its address twice");
        bind_as_its_node(listener, port)
    };

    let first = named(&mut address, &mut counted_bind);
    let second = named(&mut address, &mut counted_bind);

    assert_eq!(first.address, format!("127.0.0.1:{port}"));
    assert!(
        first.moves.is_empty(),
        "a free port keeps its address: {} moves",
        first.moves.len()
    );
    assert_eq!(second.address, first.address);
    assert!(second.moves.is_empty());
    assert_eq!(binds, 1, "a named address is bound only at its first use");
    address.release();
}

#[nervix_primitives::test]
async fn naming_moves_an_address_off_a_port_another_listener_took() {
    let mut address = draw(NodeBoundListener::ZeroMqPull);
    let taken = address.port();
    // A listener that binds the port explicitly with SO_REUSEADDR, as Docker's proxy does for a
    // port it publishes, binds beside the hold.
    let holder = listen(Ipv4Addr::UNSPECIFIED, taken).await;

    let named = named(&mut address, bind_as_its_node);

    let [moved] = named.moves.as_slice() else {
        panic!("one move off the taken port, not {}", named.moves.len());
    };
    assert_eq!(moved.from, taken);
    assert_ne!(moved.to, taken);
    assert_eq!(moved.refusal.kind(), io::ErrorKind::AddrInUse);
    assert_eq!(named.address, format!("tcp://127.0.0.1:{}", moved.to));
    // The node binds the port the address moved to.
    drop(listen(Ipv4Addr::LOCALHOST, moved.to).await);
    drop(holder);
    address.release();
}

#[nervix_primitives::test]
async fn naming_moves_a_syslog_address_off_a_port_a_udp_socket_took() {
    let mut address = draw(NodeBoundListener::Syslog);
    let taken = address.port();
    let holder = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, taken)))
        .await
        .assured("no UDP socket holds a port the pool just drew");

    let named = named(&mut address, bind_as_its_node);

    let [moved] = named.moves.as_slice() else {
        panic!("one move off the taken port, not {}", named.moves.len());
    };
    assert_eq!(moved.from, taken);
    assert_eq!(named.address, format!("127.0.0.1:{}", moved.to));
    // The node's UDP socket binds the port the address moved to.
    let node = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, moved.to))).await;
    assert!(node.is_ok(), "{node:?}");
    drop(holder);
    address.release();
}

#[nervix_primitives::test]
async fn a_zeromq_address_keeps_a_port_only_a_udp_socket_holds() {
    let mut address = draw(NodeBoundListener::ZeroMqPull);
    let port = address.port();
    let udp = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .await
        .assured("no UDP socket holds a port the pool just drew");

    let named = named(&mut address, bind_as_its_node);

    assert!(
        named.moves.is_empty(),
        "a ZeroMQ PULL socket binds TCP only: {} moves",
        named.moves.len()
    );
    assert_eq!(named.address, format!("tcp://127.0.0.1:{port}"));
    drop(udp);
    address.release();
}

#[test]
fn a_refusal_other_than_a_taken_port_fails_and_leaves_the_address_unnamed() {
    let mut address = draw(NodeBoundListener::ZeroMqPull);
    let port = address.port();

    let outcome = address.name(|_, _| Err(io::Error::from(io::ErrorKind::PermissionDenied)));

    let Err(error) = outcome else {
        panic!("a bind refused for another reason fails the naming");
    };
    let NodeBoundAddressError::Bind { port: refused, .. } = error.current_context() else {
        panic!("the failure names the refused bind: {error:#}");
    };
    assert_eq!(*refused, port);
    // The address stayed on its port, and its next use checks that port again.
    let mut binds = 0_u32;
    let renamed = named(&mut address, |_, _| {
        binds = binds.checked_add(1).assured("the regression binds once");
        Ok(())
    });
    assert_eq!(renamed.address, format!("tcp://127.0.0.1:{port}"));
    assert_eq!(binds, 1);
    address.release();
}

#[test]
fn an_address_whose_every_port_is_taken_ends_at_the_draw_limit() {
    let mut address = draw(NodeBoundListener::ZeroMqPull);

    let outcome = address.name(|_, _| Err(io::Error::from(io::ErrorKind::AddrInUse)));

    let Err(error) = outcome else {
        panic!("an address whose every port is taken cannot be named");
    };
    let NodeBoundAddressError::Taken { drawn, .. } = error.current_context() else {
        panic!("the failure names the ports that were taken: {error:#}");
    };
    assert_eq!(drawn.len(), DRAW_LIMIT);
    address.release();
}

#[nervix_primitives::test]
async fn release_gives_back_every_port_the_address_drew() {
    let mut address = draw(NodeBoundListener::ZeroMqPull);
    let holder = listen(Ipv4Addr::UNSPECIFIED, address.port()).await;
    let named = named(&mut address, bind_as_its_node);
    let [moved] = named.moves.as_slice() else {
        panic!("one move off the taken port, not {}", named.moves.len());
    };
    let drawn = [moved.from, moved.to];

    for port in drawn {
        assert!(
            is_reserved(port),
            "the pool holds port {port} while the address lives"
        );
    }
    address.release();
    for port in drawn {
        assert!(
            !is_reserved(port),
            "the pool takes port {port} back once the address is released"
        );
    }
    drop(holder);
}

#[cfg(target_os = "linux")]
#[nervix_primitives::test]
async fn the_listing_names_a_listener_on_the_port_and_its_process() {
    use crate::node_bound_address::PortHolders;

    let holder = listen(Ipv4Addr::UNSPECIFIED, 0).await;
    let port = holder
        .local_addr()
        .assured("a bound listener knows its address")
        .port();

    let holders = PortHolders::list(port).to_string();

    assert!(
        holders.contains(&format!("tcp 0.0.0.0:{port} LISTEN")),
        "{holders}"
    );
    assert!(holders.contains("held by this process"), "{holders}");
}

/// A table column as the kernel prints an address: each 32-bit word of its network-order bytes as
/// a native-endian number in hexadecimal.
fn table_column(octets: &[u8], port: u16) -> String {
    let mut column = String::new();
    let (words, _) = octets.as_chunks::<4>();
    for word in words {
        column.push_str(&format!("{:08X}", u32::from_ne_bytes(*word)));
    }
    format!("{column}:{port:04X}")
}

#[test]
fn a_socket_table_row_reads_its_address_state_owner_and_inode() {
    let header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   \
                  uid  timeout inode";
    assert_eq!(ListedSocket::parse(SocketTable::Tcp, header), None);

    let listening = format!(
        "   3: {} 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1001        0 4242 1 \
         0000000000000000 100 0 0 10 0",
        table_column(&Ipv4Addr::LOCALHOST.octets(), 41_139)
    );
    let listed = ListedSocket::parse(SocketTable::Tcp, &listening);
    assert_eq!(
        listed,
        Some(ListedSocket {
            table: SocketTable::Tcp,
            local: SocketAddr::from((Ipv4Addr::LOCALHOST, 41_139)),
            state: 0x0A,
            uid: 1001,
            inode: 4242,
        })
    );
    let rendered = listed.map(|listed| listed.to_string());
    assert_eq!(
        rendered.as_deref(),
        Some("tcp 127.0.0.1:41139 LISTEN uid 1001 inode 4242")
    );

    let bound = format!(
        "  128: {} 00000000000000000000000000000000:0000 07 00000000:00000000 00:00000000 \
         00000000     0        0 77 2 0000000000000000 0",
        table_column(&Ipv6Addr::LOCALHOST.octets(), 514)
    );
    let listed = ListedSocket::parse(SocketTable::Udp6, &bound);
    assert_eq!(
        listed,
        Some(ListedSocket {
            table: SocketTable::Udp6,
            local: SocketAddr::from((Ipv6Addr::LOCALHOST, 514)),
            state: 0x07,
            uid: 0,
            inode: 77,
        })
    );
    let rendered = listed.map(|listed| listed.to_string());
    assert_eq!(
        rendered.as_deref(),
        Some("udp6 [::1]:514 unconnected uid 0 inode 77")
    );
}
