//! Regressions for the loopback address a scenario dials and never connects to.
//!
//! Layer: test harness.
//! - **Owns.** The connection attempts these regressions make and the listener beside the held
//!   address that shows the hold is confined to it.
//! - **Depends on.** The unaccepted address, Tokio's TCP sockets and the primitives boundary's
//!   timer.
//! - **Must not know.** Scenario state, DNS or any client's protocol.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use meticulous::ResultExt as _;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

use crate::unaccepted_address::UnacceptedAddress;

/// How long an attempt at the held address is watched. Any window would do, since a held attempt
/// never connects at all; a longer one only strengthens the check.
const HELD_FOR: Duration = Duration::from_secs(2);

/// How long a connection to a listener that does accept may take. Generous, because the attempt
/// ends as soon as the handshake completes.
const CONNECTED_WITHIN: Duration = Duration::from_secs(30);

#[nervix_primitives::test]
async fn an_attempt_at_an_unaccepted_address_never_connects_while_its_port_serves_other_addresses()
{
    let serving = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("loopback has a free ephemeral port");
    let port = serving
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let held = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 6, 1)), port);
    let _unaccepted = UnacceptedAddress::listen(held)
        .await
        .assured("every 127.0.0.0/8 address is local, so another one can take the same port");

    let attempt = timeout(HELD_FOR, TcpStream::connect(held)).await;
    assert!(
        attempt.is_err(),
        "a connection to {held} completed although its listener's queue is full: {attempt:?}"
    );

    let connected = timeout(
        CONNECTED_WITHIN,
        TcpStream::connect((Ipv4Addr::LOCALHOST, port)),
    )
    .await;
    assert!(
        matches!(connected, Ok(Ok(_))),
        "the port's other address stopped accepting connections: {connected:?}"
    );
}
