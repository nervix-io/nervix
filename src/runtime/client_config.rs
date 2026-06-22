use std::{fs, path::PathBuf};

use upon::Engine as TemplateEngine;

#[derive(Debug, Clone)]
pub(super) struct ClientTlsPaths {
    pub(super) ca_file: Option<PathBuf>,
    pub(super) cert_file: Option<PathBuf>,
    pub(super) key_file: Option<PathBuf>,
}

impl ClientTlsPaths {
    pub(super) fn is_empty(&self) -> bool {
        self.ca_file.is_none() && self.cert_file.is_none() && self.key_file.is_none()
    }
}

pub(super) fn render_client_config_template<T: serde::Serialize>(
    template_engine: &TemplateEngine<'_>,
    key: &str,
    value: &str,
    context: &T,
) -> Result<String, String> {
    let template = template_engine.compile(value).map_err(|source| {
        format!(
            "failed to compile client config template for '{}={value}': {source:#}",
            key
        )
    })?;
    template
        .render(template_engine, context)
        .to_string()
        .map_err(|source| {
            format!(
                "failed to render client config template for '{}={value}': {source:#}",
                key
            )
        })
}

pub(super) fn client_tls_paths(config: &[nervix_models::ClientConfigEntry]) -> ClientTlsPaths {
    ClientTlsPaths {
        ca_file: super::optional_client_config_value(config, "tls_ca_file").map(PathBuf::from),
        cert_file: super::optional_client_config_value(config, "tls_cert_file").map(PathBuf::from),
        key_file: super::optional_client_config_value(config, "tls_key_file").map(PathBuf::from),
    }
}

pub(super) fn client_identity_pem(tls: &ClientTlsPaths) -> Result<Option<Vec<u8>>, String> {
    match (&tls.cert_file, &tls.key_file) {
        (Some(cert_file), Some(key_file)) => {
            let mut pem = read_tls_file(cert_file, "TLS certificate")?;
            pem.extend(read_tls_file(key_file, "TLS private key")?);
            Ok(Some(pem))
        }
        (None, None) => Ok(None),
        _ => Err(
            "TLS client authentication requires both 'tls_cert_file' and 'tls_key_file'"
                .to_string(),
        ),
    }
}

pub(super) fn read_tls_file(path: &PathBuf, label: &str) -> Result<Vec<u8>, String> {
    fs::read(path)
        .map_err(|source| format!("failed to read {label} '{}': {source}", path.display()))
}
