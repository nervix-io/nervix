//! Layer: test harness.
//! - **Owns.** TLS option preservation, private identity preparation and shared client lifetime evidence.
//! - **Depends on.** The production MongoDB configuration and client owners.
//! - **Must not know.** Cluster state, resources or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests MongoDB client TLS ownership")
)]

use std::fs;

use meticulous::ResultExt as _;
use mongodb::options::{ClientOptions, Tls, TlsOptions};
use nervix_execution::{ExecutionConfig, Executor};
use nervix_models::{ClientConfigEntry, ClientPoolBounds};

use super::MongoDbClient;
use crate::tls_identity::{MongoDbClientOptions, MongoDbTlsError};

fn entry(key: &str, value: impl Into<String>) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_owned(),
        value: value.into(),
    }
}

fn executor() -> Executor {
    Executor::new(ExecutionConfig::default()).assured("default execution configuration is valid")
}

#[nervix_primitives::test]
async fn explicit_ca_preserves_all_other_native_tls_options() {
    let mut tls = TlsOptions::default();
    tls.ca_file_path = Some("url-ca.pem".into());
    tls.cert_key_file_path = Some("url-identity.pem".into());
    tls.allow_invalid_certificates = Some(true);
    let mut options = ClientOptions::default();
    options.tls = Some(Tls::Enabled(tls.clone()));
    options.app_name = Some("Nervix".to_owned());
    let prepared = MongoDbClientOptions::new(options, &[entry("tls_ca_file", "explicit-ca.pem")])
        .assured("a CA override is valid")
        .prepare(&executor())
        .await
        .assured("a CA override needs no identity file");
    tls.ca_file_path = Some("explicit-ca.pem".into());
    assert_eq!(prepared.options.tls, Some(Tls::Enabled(tls)));
    assert_eq!(prepared.options.app_name.as_deref(), Some("Nervix"));
    assert!(prepared.identity.is_none());
}

#[nervix_primitives::test]
async fn native_tls_state_is_preserved_without_explicit_files() {
    for tls in [
        None,
        Some(Tls::Disabled),
        Some(Tls::Enabled(TlsOptions::default())),
    ] {
        let mut options = ClientOptions::default();
        options.tls = tls.clone();
        let prepared = MongoDbClientOptions::new(options, &[])
            .assured("native TLS options are valid")
            .prepare(&executor())
            .await
            .assured("native TLS options need no preparation");
        assert_eq!(prepared.options.tls, tls);
        assert!(prepared.identity.is_none());
    }
}

#[test]
fn explicit_files_require_enabled_tls_and_a_complete_identity() {
    for config in [
        vec![entry("tls_ca_file", "ca.pem")],
        vec![
            entry("tls_cert_file", "cert.pem"),
            entry("tls_key_file", "key.pem"),
        ],
    ] {
        let mut options = ClientOptions::default();
        options.tls = Some(Tls::Disabled);
        let report = match MongoDbClientOptions::new(options, &config) {
            Ok(_) => panic!("explicit files cannot override tls=false"),
            Err(report) => report,
        };
        assert!(matches!(
            report.current_context(),
            MongoDbTlsError::Disabled
        ));
    }
    for config in [
        vec![entry("tls_cert_file", "cert.pem")],
        vec![entry("tls_key_file", "key.pem")],
    ] {
        let report = match MongoDbClientOptions::new(ClientOptions::default(), &config) {
            Ok(_) => panic!("both identity files are required"),
            Err(report) => report,
        };
        assert!(
            report
                .downcast_ref::<nervix_connector::ClientConfigError>()
                .is_some()
        );
    }
}

#[nervix_primitives::test]
async fn combined_identity_lives_until_the_last_shared_client_is_dropped() {
    let executor = executor();
    let directory = tempfile::tempdir().assured("test directory is available");
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .assured("test identity can be generated");
    let certificate = identity.cert.pem();
    let private_key = identity.key_pair.serialize_pem();
    let cert_path = directory.path().join("cert.pem");
    let key_path = directory.path().join("key.pem");
    fs::write(&cert_path, certificate.trim_end()).assured("certificate can be written");
    fs::write(&key_path, &private_key).assured("private key can be written");
    let config = [
        entry("tls_cert_file", cert_path.to_string_lossy()),
        entry("tls_key_file", key_path.to_string_lossy()),
    ];
    let mut native = ClientOptions::default();
    let mut tls = TlsOptions::default();
    tls.allow_invalid_certificates = Some(true);
    native.tls = Some(Tls::Enabled(tls.clone()));
    let prepared = MongoDbClientOptions::new(native, &config)
        .assured("split identity is valid")
        .prepare(&executor)
        .await
        .assured("split identity can be prepared");
    let path = prepared
        .identity
        .as_ref()
        .expect("split identity owns a file")
        .path()
        .to_path_buf();
    tls.cert_key_file_path = Some(path.clone());
    assert_eq!(prepared.options.tls, Some(Tls::Enabled(tls)));
    assert_eq!(
        fs::read_to_string(&path).assured("identity is readable"),
        format!("{}\n{}\n", certificate.trim_end(), private_key)
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            fs::metadata(&path)
                .assured("identity metadata is readable")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let before = executor.snapshot().filesystem_storage.admitted;
    let client = MongoDbClient::from_prepared(prepared, &executor)
        .await
        .assured("driver accepts the prepared identity");
    assert_eq!(
        executor.snapshot().filesystem_storage.admitted,
        before + 1,
        "the native constructor reads and parses TLS files on the filesystem executor"
    );
    let retained = client.clone();
    drop(client);
    assert!(
        path.exists(),
        "a retained client still owns its prepared identity"
    );
    drop(retained);
    assert!(
        !path.exists(),
        "the final client drop removes the private identity"
    );
}

#[nervix_primitives::test]
async fn identity_preparation_preserves_the_filesystem_cause() {
    let directory = tempfile::tempdir().assured("test directory is available");
    let missing = directory.path().join("missing.pem");
    let config = [
        entry("addr", "mongodb://unresolvable.invalid/events?tls=true"),
        entry("tls_cert_file", missing.to_string_lossy()),
        entry("tls_key_file", missing.to_string_lossy()),
    ];
    let bounds = ClientPoolBounds::new(0, std::num::NonZeroU32::MIN)
        .assured("the minimum does not exceed the maximum");
    let result = MongoDbClient::open(&config, bounds, &executor()).await;
    let report = match result {
        Ok(_) => panic!("a missing certificate cannot be prepared"),
        Err(report) => report,
    };
    assert!(matches!(
        report.current_context(),
        nervix_connector::EmitterOpenError::Initialize { .. }
    ));
    let cause = report
        .downcast_ref::<std::io::Error>()
        .expect("the typed I/O cause is retained");
    assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
}

#[nervix_primitives::test]
async fn an_explicit_ca_enables_tls_when_the_url_does_not_select_a_mode() {
    let prepared = MongoDbClientOptions::new(
        ClientOptions::default(),
        &[entry("tls_ca_file", "explicit-ca.pem")],
    )
    .assured("an explicit CA enables TLS")
    .prepare(&executor())
    .await
    .assured("a CA-only configuration needs no identity preparation");
    let tls = TlsOptions::builder()
        .ca_file_path(std::path::PathBuf::from("explicit-ca.pem"))
        .build();
    assert_eq!(prepared.options.tls, Some(Tls::Enabled(tls)));
    assert!(prepared.identity.is_none());
}

#[nervix_primitives::test]
async fn native_tls_file_refusal_retains_its_driver_cause_after_worker_admission() {
    let executor = executor();
    let directory = tempfile::tempdir().assured("test directory is available");
    let missing = directory.path().join("missing-ca.pem");
    let config = [
        entry("addr", "mongodb://unresolvable.invalid/events?tls=true"),
        entry("tls_ca_file", missing.to_string_lossy()),
    ];
    let bounds = ClientPoolBounds::new(0, std::num::NonZeroU32::MIN)
        .assured("the minimum does not exceed the maximum");
    let result = MongoDbClient::open(&config, bounds, &executor).await;
    let report = match result {
        Ok(_) => panic!("a missing CA cannot initialize native TLS"),
        Err(report) => report,
    };
    assert!(report.downcast_ref::<mongodb::error::Error>().is_some());
    assert!(matches!(
        report.current_context(),
        nervix_connector::EmitterOpenError::InvalidConfiguration { .. }
    ));
    assert_eq!(
        executor.snapshot().filesystem_storage.admitted,
        1,
        "CA-only native construction also runs on the filesystem executor"
    );
}
