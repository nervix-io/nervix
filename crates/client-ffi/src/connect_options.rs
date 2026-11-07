//! `nx_connect_options`: owned connection policy a C host builds before opening a session.
//!
//! Layer: edges.
//!
//! - **Owns.** Copying credentials and CA bytes, parsing configured seed URLs, and projecting
//!   timeout and TLS choices into the Rust client's connection options.
//! - **Depends on.** The Rust client's connection options and the C ABI boundary.
//! - **Must not know.** How a session connects, validates endpoints, or retries them.

use std::time::Duration;

use nervix_client_core::{ConnectOptions, TlsRequirement};
use nervix_client_host::Failure;
use url::Url;

use crate::abi;

/// The host-owned connection policy. A session takes a snapshot when it connects.
#[derive(Debug, Default)]
pub struct ConnectionOptions(pub(crate) ConnectOptions);

#[unsafe(no_mangle)]
pub extern "C" fn nx_connect_options_new() -> *mut ConnectionOptions {
    abi::into_handle(ConnectionOptions::default())
}

/// # Safety
///
/// `options` is null or a live options handle that no other thread is using.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_connect_options_free(options: *mut ConnectionOptions) {
    // SAFETY: the caller owns this handle and uses it from one thread at a time.
    unsafe { abi::release(options) };
}

/// # Safety
///
/// `options` is a live, exclusively used handle; both text pointers address their lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_connect_options_basic_auth(
    options: *mut ConnectionOptions,
    username: *const u8,
    username_len: usize,
    password: *const u8,
    password_len: usize,
) -> *mut Failure {
    // SAFETY: the caller guarantees a live, exclusively used handle and readable input buffers.
    abi::outcome((|| unsafe {
        let username = abi::text(username, username_len, "username")?;
        let password = abi::text(password, password_len, "password")?;
        let options = abi::handle_mut(options, "options")?;
        options.0.username = Some(username.to_owned());
        options.0.password = Some(password.to_owned());
        Ok(())
    })())
}

/// # Safety
///
/// `options` is a live, exclusively used handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_connect_options_tls_requirement(
    options: *mut ConnectionOptions,
    requirement: i32,
) -> *mut Failure {
    // SAFETY: the caller guarantees a live, exclusively used options handle.
    abi::outcome((|| unsafe {
        let value = match requirement {
            1 => TlsRequirement::Preferred,
            2 => TlsRequirement::Required,
            _ => {
                return Err(Failure::invalid_argument(
                    "requirement",
                    "is not an nx_tls_requirement",
                ));
            }
        };
        abi::handle_mut(options, "options")?.0.tls_requirement = Some(value);
        Ok(())
    })())
}

/// # Safety
///
/// `options` is a live, exclusively used handle; `pem` addresses `pem_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_connect_options_ca_certificate(
    options: *mut ConnectionOptions,
    pem: *const u8,
    pem_len: usize,
) -> *mut Failure {
    // SAFETY: the caller guarantees a live, exclusively used handle and readable PEM bytes.
    abi::outcome((|| unsafe {
        if pem.is_null() || pem_len == 0 {
            return Err(Failure::invalid_argument("pem", "is required"));
        }
        let bytes = abi::input_slice(pem, pem_len, "pem")?;
        abi::handle_mut(options, "options")?.0.ca_certificate_pem = Some(bytes.to_vec());
        Ok(())
    })())
}

/// # Safety
///
/// `options` is a live, exclusively used handle; `server` addresses `server_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_connect_options_add_seed(
    options: *mut ConnectionOptions,
    server: *const u8,
    server_len: usize,
) -> *mut Failure {
    // SAFETY: the caller guarantees a live, exclusively used handle and readable URL bytes.
    abi::outcome((|| unsafe {
        let text = abi::text(server, server_len, "server")?;
        let url = Url::parse(text)
            .map_err(|error| Failure::invalid_argument("server", &error.to_string()))?;
        let options = &mut abi::handle_mut(options, "options")?.0;
        if options.seed_servers.len() >= ConnectOptions::MAX_SEED_SERVERS {
            return Err(Failure::invalid_argument(
                "server",
                "exceeds the maximum seed count",
            ));
        }
        options.seed_servers.push(url);
        Ok(())
    })())
}

/// # Safety
///
/// `options` is a live, exclusively used handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_connect_options_timeout(
    options: *mut ConnectionOptions,
    kind: i32,
    millis: u64,
) -> *mut Failure {
    // SAFETY: the caller guarantees a live, exclusively used options handle.
    abi::outcome((|| unsafe {
        let options = &mut abi::handle_mut(options, "options")?.0;
        let timeout = Duration::from_millis(millis);
        match kind {
            1 => options.connect_timeout = timeout,
            2 => options.request_timeout = timeout,
            3 => options.retry_timeout = timeout,
            4 => options.backup_wait_timeout = timeout,
            _ => {
                return Err(Failure::invalid_argument(
                    "kind",
                    "is not an nx_timeout_kind",
                ));
            }
        }
        Ok(())
    })())
}
