//! Socket ownership regressions for the paced interconnect fixture.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Real loopback peers for the fixture's two distinct connection legs.
//! - **Depends on.** The paced connection owner and primitive sockets and timers.
//! - **Must not know.** TLS, HTTP/2, relay admission or scenario state.

use std::{io, net::Ipv4Addr, time::Duration};

use nervix_primitives::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::paced_connection::PacedConnection;

#[nervix_primitives::test]
async fn a_paced_connection_keeps_both_distinct_socket_legs() -> error_stack::Result<(), io::Error>
{
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let target = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let mut sender = TcpStream::connect(listener.local_addr()?).await?;
        let (incoming, _) = listener.accept().await?;
        let mut connection = PacedConnection::connect(incoming, target.local_addr()?).await?;
        let (mut receiver, _) = target.accept().await?;

        sender.write_all(b"request").await?;
        let mut request = [0; 7];
        connection.incoming.read_exact(&mut request).await?;
        assert_eq!(&request, b"request");
        connection.outgoing.write_all(&request).await?;
        receiver.read_exact(&mut request).await?;
        assert_eq!(&request, b"request");

        receiver.write_all(b"reply").await?;
        let mut reply = [0; 5];
        connection.outgoing.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"reply");
        connection.incoming.write_all(&reply).await?;
        sender.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"reply");
        Ok::<(), io::Error>(())
    })
    .await
    .map_err(io::Error::other)??;
    Ok(())
}

#[nervix_primitives::test]
async fn a_paced_connection_preserves_nodelay_on_both_legs() -> error_stack::Result<(), io::Error> {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let target = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let _sender = TcpStream::connect(listener.local_addr()?).await?;
        let (incoming, _) = listener.accept().await?;
        let connection = PacedConnection::connect(incoming, target.local_addr()?).await?;
        let _receiver = target.accept().await?;
        let incoming_nodelay = connection.incoming.nodelay()?;
        let outgoing_nodelay = connection.outgoing.nodelay()?;
        assert_eq!(
            (incoming_nodelay, outgoing_nodelay),
            (true, true),
            "the byte cadence must be the only delay the interconnect fixture adds"
        );
        Ok::<(), io::Error>(())
    })
    .await
    .map_err(io::Error::other)??;
    Ok(())
}
