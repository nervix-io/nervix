//! Outside the layer order: observations and deadlines of the dependency HTTP readiness owner.
//!
//! - **Owns.** Controlled HTTP answers and a peer that never sends response headers.
//! - **Depends on.** The production readiness probe and loopback primitive sockets.
//! - **Must not know.** Docker lifecycle, Pulsar implementation details or product runtime state.

use std::{io, time::Duration};

use nervix_primitives::{net::TcpListener, sync::oneshot, task};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

use super::HttpReadiness;

const NAMESPACE_PATH: &str = "/admin/v2/namespaces/public/default";

struct AdminFixture {
    listener: TcpListener,
}

impl AdminFixture {
    async fn bind() -> Self {
        Self {
            listener: TcpListener::bind("127.0.0.1:0")
                .await
                .expect("the fixture listener should bind"),
        }
    }

    fn port(&self) -> u16 {
        self.listener
            .local_addr()
            .expect("the bound fixture should have an address")
            .port()
    }

    async fn respond(&self, status: &str) -> io::Result<()> {
        let (stream, _) = self.listener.accept().await?;
        let mut stream = BufReader::new(stream);
        let mut line = String::new();
        stream.read_line(&mut line).await?;
        assert_eq!(line, format!("GET {NAMESPACE_PATH} HTTP/1.1\r\n"));
        loop {
            nervix_primitives::task::consume_budget().await;
            line.clear();
            if stream.read_line(&mut line).await? == 0 || line == "\r\n" {
                break;
            }
        }
        stream
            .write_all(
                format!(
                    "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\nLocation: \
                     /somewhere-else\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
    }
}

#[nervix_primitives::test]
async fn namespace_readiness_requires_an_exact_ok_without_following_redirects() {
    let fixture = AdminFixture::bind().await;
    let readiness = HttpReadiness::new(fixture.port(), NAMESPACE_PATH);
    let server = task::spawn(async move {
        for status in [
            "201 Created",
            "302 Found",
            "404 Not Found",
            "503 Service Unavailable",
            "200 OK",
        ] {
            nervix_primitives::task::consume_budget().await;
            fixture.respond(status).await?;
        }
        Ok::<_, io::Error>(())
    });
    readiness
        .wait(Duration::from_secs(30))
        .await
        .expect("the namespace should become ready once it returns 200");
    nervix_primitives::time::timeout(Duration::from_secs(30), server)
        .await
        .expect("readiness must reach the final 200 rather than an earlier answer")
        .expect("the admin fixture should not panic")
        .expect("the admin fixture should answer every probe");
}

#[nervix_primitives::test]
async fn an_unavailable_namespace_retains_its_last_status_at_the_deadline() {
    let fixture = AdminFixture::bind().await;
    let readiness = HttpReadiness::new(fixture.port(), NAMESPACE_PATH);
    let server: task::JoinHandle<()> = task::spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            fixture
                .respond("404 Not Found")
                .await
                .expect("the admin fixture should answer a missing namespace");
        }
    });
    let error = readiness
        .wait(Duration::from_secs(1))
        .await
        .expect_err("a missing namespace must reach its deadline");
    server.abort();
    assert!(
        server
            .await
            .expect_err("the fixture owner has aborted the server")
            .is_cancelled()
    );
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    let message = error.to_string();
    assert!(message.contains(NAMESPACE_PATH), "{message}");
    assert!(message.contains("404 Not Found"), "{message}");
}

#[nervix_primitives::test]
async fn a_peer_that_never_sends_headers_cannot_extend_the_probe_budget() {
    let fixture = AdminFixture::bind().await;
    let readiness = HttpReadiness::new(fixture.port(), NAMESPACE_PATH);
    let (release, released) = oneshot::channel::<()>();
    let server = task::spawn(async move {
        let (_stream, _) = fixture.listener.accept().await?;
        assert!(released.await.is_err());
        Ok::<_, io::Error>(())
    });
    let error = readiness
        .wait(Duration::from_secs(1))
        .await
        .expect_err("the entire probe must end while response headers are pending");
    drop(release);
    server
        .await
        .expect("the silent peer fixture should not panic")
        .expect("the silent peer fixture should accept the request");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(error.to_string().contains("no request completed"));
}

#[nervix_primitives::test]
async fn an_initialized_namespace_is_ready_on_its_first_response() {
    let fixture = AdminFixture::bind().await;
    let readiness = HttpReadiness::new(fixture.port(), NAMESPACE_PATH);
    let server = task::spawn(async move { fixture.respond("200 OK").await });
    readiness
        .wait(Duration::from_secs(30))
        .await
        .expect("an existing namespace should need no creation event");
    server
        .await
        .expect("the initialized admin fixture should not panic")
        .expect("the initialized admin fixture should answer the probe");
}

#[nervix_primitives::test]
async fn a_transport_failure_remains_visible_at_the_probe_deadline() {
    let fixture = AdminFixture::bind().await;
    let readiness = HttpReadiness::new(fixture.port(), NAMESPACE_PATH);
    let server = task::spawn(async move { fixture.respond("invalid").await });
    let error = readiness
        .wait(Duration::from_secs(1))
        .await
        .expect_err("a malformed HTTP answer cannot make the dependency ready");
    server
        .await
        .expect("the malformed-answer fixture should not panic")
        .expect("the malformed-answer fixture should finish its write");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    let message = error.to_string();
    assert!(message.contains(NAMESPACE_PATH), "{message}");
    assert!(message.contains("last request failed:"), "{message}");
}
