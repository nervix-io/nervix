//! The Turmoil build's sockets, name lookup, clock and admitted CPU jobs belong to the simulated host
//! whose task uses them.
//!
//! The run's simulated duration is shorter than the wait the client makes plus any time the
//! operating system's clock could lend it, so the run completes only because every timer follows
//! the simulated clock; no assertion compares real time.

use std::{
    io::{self, ErrorKind},
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use meticulous::ResultExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::{
    net::{TcpListener, TcpStream, lookup_host},
    task::spawn_cpu,
    thread,
    time::{Instant, sleep},
};

const PORT: u16 = 7443;
/// The client waits this long on its own clock.
const WAIT: Duration = Duration::from_secs(30);

#[test]
fn sockets_names_timers_and_cpu_jobs_belong_to_the_simulated_host() {
    let mut simulation = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(40))
        .build();
    simulation.host("server", || async {
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
        let (mut accepted, peer) = listener.accept().await?;
        assert_eq!(peer.ip(), turmoil::lookup("client"));
        let mut received = [0_u8; 4];
        accepted.read_exact(&mut received).await?;
        accepted.write_all(&received).await?;
        Ok(())
    });
    simulation.client("client", async {
        let resolved: Vec<SocketAddr> = lookup_host(("server", PORT)).await?.collect();
        assert_eq!(resolved, [SocketAddr::new(turmoil::lookup("server"), PORT)]);
        let unknown = lookup_host(("unregistered", PORT)).await;
        assert!(unknown.is_err_and(|error: io::Error| error.kind() == ErrorKind::NotFound));

        let mut stream = TcpStream::connect(("server", PORT)).await?;
        assert_eq!(stream.local_addr()?.ip(), turmoil::lookup("client"));
        stream.write_all(b"ping").await?;
        let mut echoed = [0_u8; 4];
        stream.read_exact(&mut echoed).await?;
        assert_eq!(&echoed, b"ping");

        let started = Instant::now();
        sleep(WAIT).await;
        assert!(started.elapsed() >= WAIT);

        let scheduler = thread::current().id();
        let job = spawn_cpu(|| thread::current().id())
            .await
            .assured("the job returns its thread without panicking");
        assert_eq!(job, scheduler);
        Ok(())
    });
    simulation
        .run()
        .assured("both simulated hosts complete within the simulated duration");
}
