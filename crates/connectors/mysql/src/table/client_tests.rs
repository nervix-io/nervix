//! Harness layer: MySQL client deadlines and TLS configuration through their production owners.
//!
//! - **Owns.** Loopback stalled-peer checks and typed configuration assertions.
//! - **Depends on.** The MySQL client owner, connector errors and selected test primitives.
//! - **Must not know.** Runtime tasks, graph Models or registry internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the MySQL client owner")
)]

use nervix_primitives::{
    net::TcpListener,
    task::{AbortOnDropHandle, spawn},
    time::timeout,
};

use super::*;

#[test]
fn tls_files_extend_the_urls_trust_and_identity_policy() {
    for query in [
        "require_ssl=true",
        "require_ssl=true&verify_ca=false&verify_identity=false&built_in_roots=false",
    ] {
        let address = format!("mysql://localhost/nervix?{query}");
        let original = Opts::from_url(&address).expect("the driver declares these URL options");
        let config = [
            ClientConfigEntry {
                key: "addr".to_string(),
                value: address,
            },
            ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: "/mount/ca.pem".to_string(),
            },
            ClientConfigEntry {
                key: "tls_cert_file".to_string(),
                value: "/mount/client.pem".to_string(),
            },
            ClientConfigEntry {
                key: "tls_key_file".to_string(),
                value: "/mount/client-key.pem".to_string(),
            },
        ];
        let configured =
            Opts::from(MySqlPool::options(&config).expect("paired paths configure the driver"));
        let original_tls = original.ssl_opts().expect("the URL requires TLS");
        let tls = configured
            .ssl_opts()
            .expect("the configured pool requires TLS");
        assert_eq!(
            tls.accept_invalid_certs(),
            original_tls.accept_invalid_certs()
        );
        assert_eq!(
            tls.skip_domain_validation(),
            original_tls.skip_domain_validation()
        );
        assert_eq!(
            tls.disable_built_in_roots(),
            original_tls.disable_built_in_roots()
        );
        assert_eq!(
            tls.root_certs(),
            &[std::path::PathBuf::from("/mount/ca.pem").into()]
        );
        let identity = ClientIdentity::new(
            std::path::PathBuf::from("/mount/client.pem").into(),
            std::path::PathBuf::from("/mount/client-key.pem").into(),
        );
        assert_eq!(tls.client_identity(), Some(&identity));
    }
}

#[test]
fn an_incomplete_identity_retains_the_shared_typed_refusal() {
    let config = [
        ClientConfigEntry {
            key: "addr".to_string(),
            value: "mysql://localhost/nervix".to_string(),
        },
        ClientConfigEntry {
            key: "tls_cert_file".to_string(),
            value: "/mount/client.pem".to_string(),
        },
    ];
    let report = MySqlPool::options(&config).expect_err("an identity needs both files");
    assert!(report.contains::<ClientConfigError>());
    assert!(matches!(
        report.current_context(),
        EmitterOpenError::InvalidConfiguration { .. }
    ));
    assert!(format!("{report:#}").contains("both 'tls_cert_file' and 'tls_key_file'"));
}

#[nervix_primitives::test]
async fn a_silent_mysql_handshake_ends_at_its_configured_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback is available");
    let address = listener
        .local_addr()
        .expect("the listener has a bound address");
    let _peer = AbortOnDropHandle::new(spawn(async move {
        let (_stream, _) = listener
            .accept()
            .await
            .expect("the pool connects to the fixture");
        std::future::pending::<()>().await;
    }));
    let config = [
        ClientConfigEntry {
            key: "addr".to_string(),
            value: format!("mysql://{address}/nervix"),
        },
        ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "100".to_string(),
        },
    ];
    let bounds = ClientPoolBounds::new(0, std::num::NonZeroU32::MIN)
        .expect("one pooled connection is valid");
    let outcome = timeout(Duration::from_secs(30), MySqlPool::open(&config, bounds))
        .await
        .expect("the configured operation deadline must end before the test observation bound");
    let report = match outcome {
        Ok(_) => panic!("a silent server never completes its MySQL handshake"),
        Err(report) => report,
    };
    assert!(
        report.contains::<nervix_connector::DeadlineElapsed>(),
        "{report:#}"
    );
    assert!(format!("{report:#}").contains("timeout_ms"));
    assert!(report.frames().any(|frame| matches!(
        frame.kind(),
        error_stack::FrameKind::Attachment(error_stack::AttachmentKind::Printable(message))
            if message.to_string().contains("within timeout_ms 100ms")
    )));
}
