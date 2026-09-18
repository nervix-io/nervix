//! The client configuration a connector is started with.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The resolved configuration entries of a client and the resource mounts they read
//!   files from, template rendering of configuration values, lookup of required, optional and
//!   boolean keys, the TLS file paths and PEM material a client names, and the parsed retry policy
//!   with its doubling delay.
//! - **Depends on.** The vocabulary's configuration entries, the template engine and the
//!   filesystem.
//! - **Must not know.** How a resource mount is resolved, how retry backoff is scheduled, or any
//!   connector's driver.

use std::{collections::BTreeMap, fs, path::PathBuf, time::Duration};

use error_stack::Report;
use tempfile::TempDir;
use thiserror::Error;
use triomphe::Arc;
use upon::Engine as TemplateEngine;

pub type ClientConfigResult<T> = Result<T, Report<ClientConfigError>>;

#[derive(Debug, Error)]
pub enum ClientConfigError {
    #[error("failed to compile client config template for key '{key}'")]
    CompileTemplate { key: String },
    #[error("failed to render client config template for key '{key}'")]
    RenderTemplate { key: String },
    #[error("TLS client authentication requires both 'tls_cert_file' and 'tls_key_file'")]
    IncompleteTlsIdentity,
    #[error("failed to read {label} '{path}'")]
    ReadTlsFile { label: String, path: PathBuf },
    #[error("missing {connector} client config key '{key}'")]
    MissingRequired {
        connector: &'static str,
        key: String,
    },
    #[error("invalid boolean client config key '{key}' value '{value}'")]
    InvalidBoolean { key: String, value: String },
}

#[derive(Debug, Clone)]
pub struct ClientTlsPaths {
    pub ca_file: Option<PathBuf>,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
}

impl ClientTlsPaths {
    pub fn is_empty(&self) -> bool {
        self.ca_file.is_none() && self.cert_file.is_none() && self.key_file.is_none()
    }
}

pub fn render_client_config_template<T: serde::Serialize>(
    template_engine: &TemplateEngine<'_>,
    key: &str,
    value: &str,
    context: &T,
) -> ClientConfigResult<String> {
    let template = template_engine.compile(value).map_err(|source| {
        Report::new(ClientConfigError::CompileTemplate {
            key: key.to_string(),
        })
        .attach_printable(source.to_string())
    })?;
    template
        .render(template_engine, context)
        .to_string()
        .map_err(|source| {
            Report::new(ClientConfigError::RenderTemplate {
                key: key.to_string(),
            })
            .attach_printable(source.to_string())
        })
}

pub fn client_tls_paths(config: &[nervix_models::ClientConfigEntry]) -> ClientTlsPaths {
    ClientTlsPaths {
        ca_file: optional_client_config_value(config, "tls_ca_file").map(PathBuf::from),
        cert_file: optional_client_config_value(config, "tls_cert_file").map(PathBuf::from),
        key_file: optional_client_config_value(config, "tls_key_file").map(PathBuf::from),
    }
}

pub(crate) fn client_identity_pem(tls: &ClientTlsPaths) -> ClientConfigResult<Option<Vec<u8>>> {
    match (&tls.cert_file, &tls.key_file) {
        (Some(cert_file), Some(key_file)) => {
            let mut pem = read_tls_file(cert_file, "TLS certificate")?;
            pem.extend(read_tls_file(key_file, "TLS private key")?);
            Ok(Some(pem))
        }
        (None, None) => Ok(None),
        _ => Err(Report::new(ClientConfigError::IncompleteTlsIdentity)),
    }
}

pub fn read_tls_file(path: &PathBuf, label: &str) -> ClientConfigResult<Vec<u8>> {
    fs::read(path).map_err(|source| {
        Report::new(ClientConfigError::ReadTlsFile {
            label: label.to_string(),
            path: path.clone(),
        })
        .attach_printable(source.to_string())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedRetryPolicy {
    pub backoff: Duration,
    pub max_backoff: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct ResolvedClientConfig {
    pub entries: Vec<nervix_models::ClientConfigEntry>,
    pub mounts: Option<Arc<ClientResourceMounts>>,
}

/// The resource mounts a resolved client configuration reads its files from.
///
/// The host resolves every mount into a directory under one temporary root and renders the
/// resolved paths into the configuration entries. This value only keeps that root alive: while it
/// is held, the rendered paths stay readable, and dropping it removes the root.
#[derive(Debug)]
pub struct ClientResourceMounts {
    _root: TempDir,
    _aliases: BTreeMap<String, PathBuf>,
}

impl ClientResourceMounts {
    /// Holds the root the host resolved its mounts under, with the path each alias resolved to.
    pub fn new(root: TempDir, aliases: BTreeMap<String, PathBuf>) -> Self {
        Self {
            _root: root,
            _aliases: aliases,
        }
    }
}

pub fn client_config_value(
    config: &[nervix_models::ClientConfigEntry],
    key: &str,
    connector: &'static str,
) -> ClientConfigResult<String> {
    let entry = config
        .iter()
        .find(|entry| entry.key.eq_ignore_ascii_case(key));
    match entry {
        Some(entry) => Ok(entry.value.clone()),
        None => Err(Report::new(ClientConfigError::MissingRequired {
            connector,
            key: key.to_string(),
        })),
    }
}

pub fn optional_client_config_value<'a>(
    config: &'a [nervix_models::ClientConfigEntry],
    key: &str,
) -> Option<&'a str> {
    config
        .iter()
        .find(|entry| entry.key.eq_ignore_ascii_case(key))
        .map(|entry| entry.value.as_str())
}

pub fn optional_bool_client_config_value(
    config: &[nervix_models::ClientConfigEntry],
    key: &str,
) -> ClientConfigResult<Option<bool>> {
    let Some(value) = optional_client_config_value(config, key) else {
        return Ok(None);
    };

    if value.eq_ignore_ascii_case("true") {
        Ok(Some(true))
    } else if value.eq_ignore_ascii_case("false") {
        Ok(Some(false))
    } else {
        Err(Report::new(ClientConfigError::InvalidBoolean {
            key: key.to_string(),
            value: value.to_string(),
        }))
    }
}

pub fn next_retry_delay(current: Duration, policy: ParsedRetryPolicy) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(policy.max_backoff)
        .min(policy.max_backoff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_retry_delay_doubles_and_caps() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        };

        assert_eq!(
            next_retry_delay(policy.backoff, policy),
            Duration::from_millis(200)
        );
        assert_eq!(
            next_retry_delay(Duration::from_millis(700), policy),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn client_config_reports_template_and_tls_identity_shape_errors() {
        let template = render_client_config_template(
            &TemplateEngine::new(),
            "password",
            "{{",
            &serde_json::json!({}),
        )
        .expect_err("an incomplete template must fail compilation");
        assert!(matches!(
            template.current_context(),
            ClientConfigError::CompileTemplate { key } if key == "password"
        ));

        let tls = ClientTlsPaths {
            ca_file: None,
            cert_file: Some(PathBuf::from("client.pem")),
            key_file: None,
        };
        let identity = client_identity_pem(&tls)
            .expect_err("a TLS identity requires both certificate and key files");
        assert!(matches!(
            identity.current_context(),
            ClientConfigError::IncompleteTlsIdentity
        ));
    }
}
