//! Declared connector client configuration and its validation and inspection boundaries.
//!
//! Layer: vocabulary.
//! - **Owns.** Key names, presence, types, confidentiality, file roles and delegated namespaces.
//! - **Depends on.** Configuration entries, the shared deadline contract and URL values.
//! - **Must not know.** Driver implementations, resource installation or network operations.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns declared client configuration contracts")
)]

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    time::Duration,
};

use error_stack::{Report, ResultExt as _};
use thiserror::Error;
use url::Url;

use crate::{ClientConfigEntry, ClientDeadline};

/// The value public client inspection uses for a confidential configuration entry.
pub const CLIENT_CONFIG_REDACTION: &str = "********";

#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "Linux TCP_KEEPIDLE accepts at most 32767 seconds")
)]
const MAX_TCP_KEEPALIVE: Duration = Duration::from_secs(32_767);

/// The reader that owns a declared key's interpretation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientConfigOwner {
    Nervix,
    Driver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientConfigPresence {
    Required,
    Optional,
    Defaulted(&'static str),
    RequiredForScheme {
        address: &'static str,
        scheme: &'static str,
    },
    OptionalForScheme {
        address: &'static str,
        scheme: &'static str,
    },
}

/// A file's role, including the two halves of a TLS client identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientConfigFile {
    CertificateAuthority,
    Certificate,
    PrivateKey,
    MountedFile,
}

/// The interpretation a configuration value must support before it is committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientConfigValue {
    String,
    PercentEncodedHeaders,
    Boolean,
    UnsignedInteger,
    PositiveInteger,
    Milliseconds,
    TcpKeepaliveMilliseconds,
    Url(&'static [&'static str]),
    DriverAddress,
    Choice(&'static [&'static str]),
    File(ClientConfigFile),
    /// Exactly the levels its template names, such as `tenant/namespace`: each nonempty, without
    /// whitespace or control characters, and separated by `/`. The template is also its label.
    Levels(&'static str),
}

impl ClientConfigValue {
    pub const fn label(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::PercentEncodedHeaders => "comma-separated name=percent-encoded-value pairs",
            Self::Boolean => "boolean",
            Self::UnsignedInteger => "unsigned integer",
            Self::PositiveInteger => "positive integer",
            Self::Milliseconds => "positive, schedulable whole milliseconds",
            Self::TcpKeepaliveMilliseconds => {
                "whole seconds from 1000 through 32767000 milliseconds"
            }
            Self::Url(_) => "URL",
            Self::DriverAddress => "driver address, including its delegated query options",
            Self::Choice(_) => "declared choice",
            Self::File(_) => "file path, with resource mount templates resolved before use",
            Self::Levels(template) => template,
        }
    }

    /// Whether `value` names exactly the levels `template` names, each nonempty and without
    /// whitespace or control characters.
    fn holds_levels(template: &str, value: &str) -> bool {
        let levels: Vec<&str> = value.split('/').collect();
        if levels.len() != template.split('/').count() {
            return false;
        }
        for level in levels {
            if level.is_empty() {
                return false;
            }
            if level
                .chars()
                .any(|ch| ch.is_whitespace() || ch.is_control())
            {
                return false;
            }
        }
        true
    }

    fn validate(self, key: &str, value: &str) -> Result<(), Report<ClientConfigSchemaError>> {
        let invalid = || ClientConfigSchemaError::InvalidValue {
            key: key.to_string(),
            expected: self.label(),
        };
        match self {
            Self::String => {}
            Self::PercentEncodedHeaders => {
                crate::PercentEncodedHeaders::parse(value).change_context(invalid())?;
            }
            Self::Boolean => {
                if !value.eq_ignore_ascii_case("true") && !value.eq_ignore_ascii_case("false") {
                    return Err(Report::new(invalid()));
                }
            }
            Self::UnsignedInteger | Self::PositiveInteger => {
                let count = value.parse::<u64>().change_context(invalid())?;
                if value.is_empty()
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
                    || (self == Self::PositiveInteger && count == 0)
                {
                    return Err(Report::new(invalid()));
                }
            }
            Self::Milliseconds => {
                ClientDeadline::parse(value).change_context(invalid())?;
            }
            Self::TcpKeepaliveMilliseconds => {
                let interval = ClientDeadline::parse(value)
                    .change_context(invalid())?
                    .duration();
                if interval.subsec_nanos() != 0 || interval > MAX_TCP_KEEPALIVE {
                    return Err(Report::new(invalid()));
                }
            }
            Self::Url(schemes) => {
                let parsed = Url::parse(value).change_context(invalid())?;
                if !schemes.contains(&parsed.scheme()) || parsed.host_str().is_none() {
                    return Err(Report::new(invalid()));
                }
            }
            Self::DriverAddress | Self::File(_) => {
                if value.is_empty() {
                    return Err(Report::new(invalid()));
                }
            }
            Self::Choice(choices) => {
                if !choices.contains(&value) {
                    return Err(Report::new(invalid()));
                }
            }
            Self::Levels(template) => {
                if !Self::holds_levels(template, value) {
                    return Err(Report::new(invalid()));
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "public inspection probes an address only to decide whether its value must \
                      be redacted; a parse refusal means redact it"
        )
    )]
    fn inspection_address(self, value: &str) -> Result<Url, url::ParseError> {
        Url::parse(value)
    }

    fn address_is_confidential(self, value: &str) -> bool {
        if !matches!(self, Self::Url(_) | Self::DriverAddress) {
            return false;
        }
        match self.inspection_address(value) {
            Ok(address) => {
                address.host_str().is_none()
                    || !address.username().is_empty()
                    || address.password().is_some()
                    || address.query().is_some()
            }
            Err(_) => true,
        }
    }
}

/// One key's current contract. Driver properties still have their offline driver check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientConfigKey {
    pub name: &'static str,
    pub presence: ClientConfigPresence,
    pub value: ClientConfigValue,
    pub owner: ClientConfigOwner,
    pub confidential: bool,
}

impl ClientConfigKey {
    pub const TLS_CA: Self = Self::optional(
        "tls_ca_file",
        ClientConfigValue::File(ClientConfigFile::CertificateAuthority),
    );
    pub const TLS_CERT: Self = Self::optional(
        "tls_cert_file",
        ClientConfigValue::File(ClientConfigFile::Certificate),
    );
    pub const TLS_KEY: Self = Self::optional(
        "tls_key_file",
        ClientConfigValue::File(ClientConfigFile::PrivateKey),
    );
    pub const TIMEOUT: Self = Self::optional("timeout_ms", ClientConfigValue::Milliseconds);
    pub const CONNECT_TIMEOUT: Self =
        Self::optional("connect_timeout_ms", ClientConfigValue::Milliseconds);
    pub const IDLE_TIMEOUT: Self =
        Self::optional("idle_timeout_ms", ClientConfigValue::Milliseconds);

    pub const fn required(name: &'static str, value: ClientConfigValue) -> Self {
        Self {
            name,
            presence: ClientConfigPresence::Required,
            value,
            owner: ClientConfigOwner::Nervix,
            confidential: false,
        }
    }

    pub const fn optional(name: &'static str, value: ClientConfigValue) -> Self {
        Self {
            presence: ClientConfigPresence::Optional,
            ..Self::required(name, value)
        }
    }

    /// Apply this declared value only when the authored configuration omits the key.
    pub const fn defaulted(
        name: &'static str,
        value: ClientConfigValue,
        default: &'static str,
    ) -> Self {
        Self {
            presence: ClientConfigPresence::Defaulted(default),
            ..Self::required(name, value)
        }
    }

    pub fn validate(self, value: &str) -> Result<(), Report<ClientConfigSchemaError>> {
        self.value.validate(self.name, value)
    }

    pub const fn secret(self) -> Self {
        Self {
            confidential: true,
            ..self
        }
    }

    /// Require this key when the named address uses a scheme without built-in trust roots.
    pub const fn required_for_scheme(self, address: &'static str, scheme: &'static str) -> Self {
        Self {
            presence: ClientConfigPresence::RequiredForScheme { address, scheme },
            ..self
        }
    }

    /// Permit this key only when the named address uses the declared scheme.
    pub const fn only_for_scheme(self, address: &'static str, scheme: &'static str) -> Self {
        Self {
            presence: ClientConfigPresence::OptionalForScheme { address, scheme },
            ..self
        }
    }

    pub const fn driver(self) -> Self {
        Self {
            owner: ClientConfigOwner::Driver,
            ..self
        }
    }
}

/// A pass-through namespace, explicitly including the case where the driver reads every key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientConfigNamespace {
    All,
    Prefix(&'static str),
}

impl ClientConfigNamespace {
    fn accepts(self, key: &str) -> bool {
        match self {
            Self::All => true,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }
}

/// The one declaration consumed by validation, completion, previews and generated reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientConfigSchema {
    keys: &'static [ClientConfigKey],
    namespaces: &'static [ClientConfigNamespace],
    reserved: &'static [&'static str],
}

#[derive(Clone, Copy)]
enum ConfigValueStage {
    Declared,
    Rendered,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClientConfigSchemaError {
    #[error("unknown client config key '{key}'")]
    UnknownKey { key: String },
    #[error("duplicate client config key '{key}'")]
    DuplicateKey { key: String },
    #[error("missing required client config key '{key}'")]
    MissingRequired { key: &'static str },
    #[error("client config key '{key}' must be lowercase")]
    KeyCase { key: String },
    #[error("invalid client config key '{key}': expected {expected}")]
    InvalidValue { key: String, expected: &'static str },
    #[error("client config key '{key}' requires '{required}'")]
    RequiresKey {
        key: &'static str,
        required: &'static str,
    },
    #[error("client config key '{key}' requires '{address}' to use {scheme}")]
    RequiresScheme {
        key: &'static str,
        address: &'static str,
        scheme: &'static str,
    },
    #[error("client config key '{key}' is reserved for Nervix")]
    Reserved { key: String },
}

impl ClientConfigSchema {
    /// Keys are sorted and unique at construction, so lookup is binary rather than a scan.
    pub const fn new(
        keys: &'static [ClientConfigKey],
        namespaces: &'static [ClientConfigNamespace],
        reserved: &'static [&'static str],
    ) -> Self {
        let mut index = 1;
        while index < keys.len() {
            assert!(Self::name_precedes(keys[index - 1].name, keys[index].name));
            index += 1;
        }
        Self {
            keys,
            namespaces,
            reserved,
        }
    }

    const fn name_precedes(left: &str, right: &str) -> bool {
        let left = left.as_bytes();
        let right = right.as_bytes();
        let mut index = 0;
        while index < left.len() && index < right.len() {
            if left[index] != right[index] {
                return left[index] < right[index];
            }
            index += 1;
        }
        left.len() < right.len()
    }

    pub const fn keys(self) -> &'static [ClientConfigKey] {
        self.keys
    }

    pub const fn namespaces(self) -> &'static [ClientConfigNamespace] {
        self.namespaces
    }

    pub const fn reserved(self) -> &'static [&'static str] {
        self.reserved
    }

    /// The summary is derived from the same per-key and namespace declarations as validation.
    pub fn ownership_label(self) -> &'static str {
        if !self.namespaces.is_empty() {
            "declared keys and driver-delegated property namespaces"
        } else if self
            .keys
            .iter()
            .any(|key| key.value == ClientConfigValue::DriverAddress)
        {
            "Nervix; address query delegated to driver"
        } else {
            "Nervix"
        }
    }

    pub fn key(self, name: &str) -> Option<&'static ClientConfigKey> {
        match self.keys.binary_search_by_key(&name, |key| key.name) {
            Ok(index) => Some(&self.keys[index]),
            Err(_) => None,
        }
    }

    pub fn validate(
        self,
        entries: &[ClientConfigEntry],
    ) -> Result<(), Report<ClientConfigSchemaError>> {
        self.validate_at(entries, ConfigValueStage::Declared)
    }

    pub fn validate_rendered(
        self,
        entries: &[ClientConfigEntry],
    ) -> Result<(), Report<ClientConfigSchemaError>> {
        self.validate_at(entries, ConfigValueStage::Rendered)
    }

    fn validate_at(
        self,
        entries: &[ClientConfigEntry],
        stage: ConfigValueStage,
    ) -> Result<(), Report<ClientConfigSchemaError>> {
        let mut values = BTreeMap::new();
        for entry in entries {
            match values.entry(entry.key.as_str()) {
                Entry::Vacant(place) => {
                    place.insert(entry.value.as_str());
                }
                Entry::Occupied(_) => {
                    return Err(Report::new(ClientConfigSchemaError::DuplicateKey {
                        key: entry.key.clone(),
                    }));
                }
            }
            if entry.key.bytes().any(|byte| byte.is_ascii_uppercase()) {
                return Err(Report::new(ClientConfigSchemaError::KeyCase {
                    key: entry.key.clone(),
                }));
            }
            if self.reserved.contains(&entry.key.as_str()) {
                return Err(Report::new(ClientConfigSchemaError::Reserved {
                    key: entry.key.clone(),
                }));
            }
            let Some(key) = self.key(&entry.key) else {
                if self
                    .namespaces
                    .iter()
                    .any(|namespace| namespace.accepts(&entry.key))
                {
                    continue;
                }
                return Err(Report::new(ClientConfigSchemaError::UnknownKey {
                    key: entry.key.clone(),
                }));
            };
            // A declaration retains its mount/instance template. The existing renderer compiles
            // it before commit and validate_rendered holds the resulting value to the same type.
            if matches!(stage, ConfigValueStage::Declared) && entry.value.contains("{{") {
                continue;
            }
            if key.owner == ClientConfigOwner::Nervix {
                key.validate(&entry.value)?;
            }
        }
        for key in self.keys {
            if key.presence == ClientConfigPresence::Required && !values.contains_key(key.name) {
                return Err(Report::new(ClientConfigSchemaError::MissingRequired {
                    key: key.name,
                }));
            }
        }
        self.validate_identity_pair(&values)?;
        self.validate_conditional_presence(&values, stage)
    }

    fn validate_conditional_presence(
        self,
        values: &BTreeMap<&str, &str>,
        stage: ConfigValueStage,
    ) -> Result<(), Report<ClientConfigSchemaError>> {
        for key in self.keys {
            let (address, scheme) = match key.presence {
                ClientConfigPresence::RequiredForScheme { address, scheme }
                | ClientConfigPresence::OptionalForScheme { address, scheme } => (address, scheme),
                _ => continue,
            };
            let present = values.contains_key(key.name);
            if matches!(key.presence, ClientConfigPresence::RequiredForScheme { .. }) && present
                || matches!(key.presence, ClientConfigPresence::OptionalForScheme { .. })
                    && !present
            {
                continue;
            }
            let Some(value) = values.get(address) else {
                continue;
            };
            if matches!(stage, ConfigValueStage::Declared) && value.contains("{{") {
                continue;
            }
            let parsed = Url::parse(value).change_context_lazy(|| {
                ClientConfigSchemaError::InvalidValue {
                    key: address.to_string(),
                    expected: "URL",
                }
            })?;
            match key.presence {
                ClientConfigPresence::RequiredForScheme { .. } if parsed.scheme() == scheme => {
                    return Err(Report::new(ClientConfigSchemaError::RequiresKey {
                        key: address,
                        required: key.name,
                    }));
                }
                ClientConfigPresence::OptionalForScheme { .. } if parsed.scheme() != scheme => {
                    return Err(Report::new(ClientConfigSchemaError::RequiresScheme {
                        key: key.name,
                        address,
                        scheme,
                    }));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_identity_pair(
        self,
        values: &BTreeMap<&str, &str>,
    ) -> Result<(), Report<ClientConfigSchemaError>> {
        let certificate = self
            .keys
            .iter()
            .find(|key| key.value == ClientConfigValue::File(ClientConfigFile::Certificate));
        let private_key = self
            .keys
            .iter()
            .find(|key| key.value == ClientConfigValue::File(ClientConfigFile::PrivateKey));
        if let (Some(certificate), Some(private_key)) = (certificate, private_key) {
            for (key, required) in [
                (certificate.name, private_key.name),
                (private_key.name, certificate.name),
            ] {
                if values.contains_key(key) && !values.contains_key(required) {
                    return Err(Report::new(ClientConfigSchemaError::RequiresKey {
                        key,
                        required,
                    }));
                }
            }
        }
        Ok(())
    }

    /// The effective configuration retains authored values and fills only omitted defaults.
    /// This projection never changes the stored entries or their complete serialization.
    pub fn effective_entries(self, entries: &[ClientConfigEntry]) -> Vec<ClientConfigEntry> {
        let authored: BTreeSet<_> = entries.iter().map(|entry| entry.key.as_str()).collect();
        let mut effective = entries.to_vec();
        for key in self.keys {
            if let ClientConfigPresence::Defaulted(value) = key.presence
                && !authored.contains(key.name)
            {
                effective.push(ClientConfigEntry {
                    key: key.name.to_string(),
                    value: value.to_string(),
                });
            }
        }
        effective
    }

    /// Delegated keys without an explicit public declaration remain confidential. Complete
    /// serialization never calls this projection, so the stored configuration is preserved.
    pub fn inspection_entries(self, entries: &[ClientConfigEntry]) -> Vec<ClientConfigEntry> {
        entries
            .iter()
            .map(|entry| {
                let confidential = match self.key(&entry.key) {
                    Some(key) => {
                        key.confidential || key.value.address_is_confidential(&entry.value)
                    }
                    None => true,
                };
                ClientConfigEntry {
                    key: entry.key.clone(),
                    value: if confidential {
                        CLIENT_CONFIG_REDACTION.to_string()
                    } else {
                        entry.value.clone()
                    },
                }
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "config_schema_tests.rs"]
mod tests;
