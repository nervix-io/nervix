//! Connection policy through the C ABI, including caller buffer ownership and refused input.
//!
//! Layer: test harness.
//! - **Owns.** Host-facing examples and bounded generated seeds for the options handle.
//! - **Depends on.** The production C ABI and the Rust client's connection options.
//! - **Must not know.** How the client schedules a reconnect or chooses a server.

use std::ptr;

use url::Url;

use super::{failure_kind, succeeded};
use crate::{
    FailureKind, nx_connect_options_add_seed, nx_connect_options_basic_auth,
    nx_connect_options_ca_certificate, nx_connect_options_free, nx_connect_options_new,
    nx_connect_options_timeout, nx_connect_options_tls_requirement, nx_session_connect,
};

#[test]
fn options_copy_host_buffers_and_validate_their_choices() {
    let options = nx_connect_options_new();
    let mut username = b"guest".to_vec();
    let mut password = b"secret".to_vec();
    let mut pem = b"certificate bytes".to_vec();
    let mut seed = b"https://example.com:8443".to_vec();
    // SAFETY: all buffers are live for their lengths and the options handle is used exclusively.
    unsafe {
        succeeded(nx_connect_options_basic_auth(
            options,
            username.as_ptr(),
            username.len(),
            password.as_ptr(),
            password.len(),
        ));
        succeeded(nx_connect_options_ca_certificate(
            options,
            pem.as_ptr(),
            pem.len(),
        ));
        succeeded(nx_connect_options_add_seed(
            options,
            seed.as_ptr(),
            seed.len(),
        ));
        succeeded(nx_connect_options_tls_requirement(options, 1));
        assert_eq!(
            (*options).0.tls_requirement,
            Some(nervix_client_core::TlsRequirement::Preferred),
        );
        succeeded(nx_connect_options_tls_requirement(options, 2));
        for kind in 1..=4 {
            succeeded(nx_connect_options_timeout(options, kind, 2_000));
        }
        username.fill(0);
        password.fill(0);
        pem.fill(0);
        seed.fill(0);
        let owned = &(*options).0;
        assert_eq!(owned.username.as_deref(), Some("guest"));
        assert_eq!(owned.password.as_deref(), Some("secret"));
        assert_eq!(
            owned.ca_certificate_pem.as_deref(),
            Some(b"certificate bytes".as_slice())
        );
        assert_eq!(owned.seed_servers[0].as_str(), "https://example.com:8443/");
        assert_eq!(owned.connect_timeout.as_millis(), 2_000);
        assert_eq!(owned.request_timeout.as_millis(), 2_000);
        assert_eq!(owned.retry_timeout.as_millis(), 2_000);
        assert_eq!(owned.backup_wait_timeout.as_millis(), 2_000);
        let extra = b"https://example.com";
        for _ in 1..nervix_client_core::ConnectOptions::MAX_SEED_SERVERS {
            succeeded(nx_connect_options_add_seed(
                options,
                extra.as_ptr(),
                extra.len(),
            ));
        }
        assert_eq!(
            failure_kind(nx_connect_options_add_seed(
                options,
                extra.as_ptr(),
                extra.len(),
            )),
            FailureKind::InvalidArgument,
        );
        assert_eq!(
            failure_kind(nx_connect_options_tls_requirement(options, 3)),
            FailureKind::InvalidArgument,
        );
        assert_eq!(
            failure_kind(nx_connect_options_timeout(options, 5, 1)),
            FailureKind::InvalidArgument,
        );
        assert_eq!(
            failure_kind(nx_connect_options_ca_certificate(options, ptr::null(), 0)),
            FailureKind::InvalidArgument,
        );
        assert_eq!(
            failure_kind(nx_connect_options_add_seed(options, b"bad".as_ptr(), 3)),
            FailureKind::InvalidArgument,
        );
        assert_eq!(
            failure_kind(nx_connect_options_basic_auth(
                options,
                ptr::null(),
                0,
                ptr::null(),
                0
            )),
            FailureKind::InvalidArgument,
        );
        let mut session = ptr::null_mut();
        let server = b"http://127.0.0.1:1";
        assert_eq!(
            failure_kind(nx_session_connect(
                server.as_ptr(),
                server.len(),
                ptr::null(),
                0,
                options,
                ptr::null(),
                &mut session,
            )),
            FailureKind::Connect,
        );
        assert!(session.is_null());
        nx_connect_options_free(options);
    }
}

#[test]
fn invalid_deadlines_are_rejected_before_connecting() {
    let options = nx_connect_options_new();
    let server = b"http://127.0.0.1:1";
    let mut session = ptr::null_mut();
    // SAFETY: the inputs are live, and no call succeeds or creates a session.
    unsafe {
        for millis in [0, 86_400_001] {
            succeeded(nx_connect_options_timeout(options, 1, millis));
            assert_eq!(
                failure_kind(nx_session_connect(
                    server.as_ptr(),
                    server.len(),
                    ptr::null(),
                    0,
                    options,
                    ptr::null(),
                    &mut session,
                )),
                FailureKind::InvalidArgument,
            );
        }
        assert_eq!(
            failure_kind(nx_session_connect(
                server.as_ptr(),
                server.len(),
                ptr::null(),
                0,
                ptr::null(),
                ptr::null(),
                &mut session,
            )),
            FailureKind::InvalidArgument,
        );
        nx_connect_options_free(options);
    }
}

#[test]
fn an_https_session_refuses_a_plaintext_seed_before_network_access() {
    let options = nx_connect_options_new();
    let initial = b"https://127.0.0.1:1";
    let plaintext_seed = b"http://127.0.0.1:2";
    let mut session = ptr::null_mut();
    // SAFETY: both input buffers and the options handle stay live through the call.
    unsafe {
        succeeded(nx_connect_options_add_seed(
            options,
            plaintext_seed.as_ptr(),
            plaintext_seed.len(),
        ));
        assert_eq!(
            failure_kind(nx_session_connect(
                initial.as_ptr(),
                initial.len(),
                ptr::null(),
                0,
                options,
                ptr::null(),
                &mut session,
            )),
            FailureKind::Connect,
        );
        assert!(session.is_null());
        nx_connect_options_free(options);
    }
}

#[test]
fn bolero_seed_urls_are_copied_or_refused_without_changing_options() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .for_each(|bytes: &[u8]| {
            let options = nx_connect_options_new();
            let mut source = bytes.to_vec();
            let expected = std::str::from_utf8(&source)
                .ok()
                .and_then(|text| Url::parse(text).ok());
            // SAFETY: the buffer addresses its length and the options handle is live and used
            // exclusively, then freed once after the call.
            unsafe {
                let result = nx_connect_options_add_seed(options, source.as_ptr(), source.len());
                match expected {
                    Some(url) => {
                        assert!(result.is_null());
                        source.fill(0);
                        assert_eq!((*options).0.seed_servers.as_slice(), &[url]);
                    }
                    None => {
                        assert_eq!(failure_kind(result), FailureKind::InvalidArgument);
                        assert!((*options).0.seed_servers.is_empty());
                    }
                }
                nx_connect_options_free(options);
            }
        });
}
