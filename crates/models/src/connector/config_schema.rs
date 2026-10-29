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
use meticulous::OptionExt as _;
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
    /// The key is written exactly while the named address uses the declared scheme: required with
    /// it and refused without it.
    ExactlyForScheme {
        address: &'static str,
        scheme: &'static str,
    },
    /// The key may be written only while the named driver URL sets one of its Nervix-owned query
    /// options to the declared choice.
    OptionalForQuery {
        address: &'static str,
        option: &'static str,
        value: &'static str,
    },
}

/// A query option of a driver URL whose value Nervix owns rather than the driver.
///
/// An address sets it exactly once, under its name or one of the driver's aliases for it, to one
/// of its choices. The driver reads every other query option as written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnedQueryOption {
    pub name: &'static str,
    /// Other spellings the driver reads as this same option.
    pub aliases: &'static [&'static str],
    pub choices: &'static [&'static str],
}

impl OwnedQueryOption {
    /// The choice `address` sets this option to, which is how both validation and the connector
    /// read it.
    pub fn value_in(
        self,
        key: &str,
        address: &Url,
    ) -> Result<&'static str, Report<ClientConfigSchemaError>> {
        let refused = || {
            Report::new(ClientConfigSchemaError::QueryOption {
                key: key.to_string(),
                option: self.name,
                choices: self.choices,
            })
        };
        let mut written = None;
        // The query is bounded by the address the statement writes, and the declared spellings
        // are a fixed list of at most a few entries.
        for (name, value) in address.query_pairs() {
            let spells_this_option =
                name == self.name || self.aliases.iter().any(|alias| *alias == name);
            if !spells_this_option {
                continue;
            }
            if written.is_some() {
                return Err(refused());
            }
            written = Some(value);
        }
        let Some(written) = written else {
            return Err(refused());
        };
        // The declared choices are a fixed list of a few spellings.
        let choice = self
            .choices
            .iter()
            .copied()
            .find(|choice| *choice == written);
        match choice {
            Some(choice) => Ok(choice),
            None => Err(refused()),
        }
    }
}

/// A driver URL whose scheme and listed query options Nervix owns; the driver reads every other
/// part of it, including its other query options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverUrl {
    pub schemes: &'static [&'static str],
    pub owned: &'static [OwnedQueryOption],
}

impl DriverUrl {
    /// Holds a parsed `key` value to a declared scheme and each owned option to one of its
    /// choices.
    fn validate(self, key: &str, address: &Url) -> Result<(), Report<ClientConfigSchemaError>> {
        if !self.schemes.contains(&address.scheme()) {
            return Err(Report::new(ClientConfigSchemaError::Scheme {
                key: key.to_string(),
                schemes: self.schemes,
            }));
        }
        for option in self.owned {
            option.value_in(key, address)?;
        }
        Ok(())
    }

    /// The owned query option named `option`, if this URL declares one.
    fn owned_option(self, option: &str) -> Option<OwnedQueryOption> {
        // A driver URL owns a fixed declared list of at most a few options.
        self.owned
            .iter()
            .copied()
            .find(|candidate| candidate.name == option)
    }
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
    /// A URL with one of these schemes that names a host and a nonzero port and nothing else: no
    /// credentials, path, query or fragment.
    HostPortUrl(&'static [&'static str]),
    DriverAddress,
    DriverUrl(DriverUrl),
    Choice(&'static [&'static str]),
    File(ClientConfigFile),
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
            Self::HostPortUrl(_) => "URL naming only a host and a port",
            Self::DriverAddress => "driver address, including its delegated query options",
            Self::DriverUrl(_) => {
                "driver URL with Nervix-owned query options, including its delegated query options"
            }
            Self::Choice(_) => "declared choice",
            Self::File(_) => "file path, with resource mount templates resolved before use",
        }
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
            Self::Url(_) | Self::HostPortUrl(_) => {
                let parsed = Url::parse(value).change_context(invalid())?;
                if !self.admits_url(&parsed) {
                    return Err(Report::new(invalid()));
                }
            }
            Self::DriverUrl(url) => {
                let parsed = Url::parse(value).change_context(invalid())?;
                url.validate(key, &parsed)?;
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
        }
        Ok(())
    }

    /// Whether a parsed `address` has one of this kind's schemes and the parts the kind requires: a
    /// host for a URL, and only a host and a nonzero port for a host-and-port URL. No other kind
    /// admits a URL.
    fn admits_url(self, address: &Url) -> bool {
        match self {
            Self::Url(schemes) => {
                schemes.contains(&address.scheme()) && address.host_str().is_some()
            }
            Self::HostPortUrl(schemes) => {
                schemes.contains(&address.scheme()) && Self::names_only_host_and_port(address)
            }
            Self::String
            | Self::PercentEncodedHeaders
            | Self::Boolean
            | Self::UnsignedInteger
            | Self::PositiveInteger
            | Self::Milliseconds
            | Self::TcpKeepaliveMilliseconds
            | Self::DriverUrl(_)
            | Self::DriverAddress
            | Self::Choice(_)
            | Self::File(_) => false,
        }
    }

    /// Whether `address` names a host and a nonzero port and nothing else: no credentials, no path
    /// beyond the root, no query and no fragment.
    fn names_only_host_and_port(address: &Url) -> bool {
        let names_host = matches!(address.host_str(), Some(host) if !host.is_empty());
        let names_port = matches!(address.port(), Some(port) if port != 0);
        let names_root = matches!(address.path(), "" | "/");
        let names_credentials = !address.username().is_empty() || address.password().is_some();
        let names_query_or_fragment = address.query().is_some() || address.fragment().is_some();
        names_host && names_port && names_root && !names_credentials && !names_query_or_fragment
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
        if !matches!(
            self,
            Self::Url(_) | Self::HostPortUrl(_) | Self::DriverAddress | Self::DriverUrl(_)
        ) {
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

    /// Require this key when the named address uses the declared scheme, and refuse it otherwise,
    /// for a key that only a connection over that scheme reads and that it cannot do without.
    pub const fn exactly_for_scheme(self, address: &'static str, scheme: &'static str) -> Self {
        Self {
            presence: ClientConfigPresence::ExactlyForScheme { address, scheme },
            ..self
        }
    }

    /// Permit this key only while the named driver URL sets its Nervix-owned `option` to `value`.
    pub const fn only_for_query(
        self,
        address: &'static str,
        option: &'static str,
        value: &'static str,
    ) -> Self {
        Self {
            presence: ClientConfigPresence::OptionalForQuery {
                address,
                option,
                value,
            },
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
    #[error("client config key '{key}' must be a {} URL", .schemes.join(" or "))]
    Scheme {
        key: String,
        schemes: &'static [&'static str],
    },
    #[error(
        "client config key '{key}' must set the {option} query option once, to {}",
        .choices.join(" or ")
    )]
    QueryOption {
        key: String,
        option: &'static str,
        choices: &'static [&'static str],
    },
    #[error("client config key '{key}' requires '{address}' to set {option}={value}")]
    RequiresQuery {
        key: &'static str,
        address: &'static str,
        option: &'static str,
        value: &'static str,
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
        let mut index = 0;
        while index < keys.len() {
            if let ClientConfigPresence::OptionalForQuery {
                address,
                option,
                value,
            } = keys[index].presence
            {
                assert!(Self::owns_choice(keys, address, option, value));
            }
            index += 1;
        }
        Self {
            keys,
            namespaces,
            reserved,
        }
    }

    /// Whether `address` is a driver URL whose Nervix-owned `option` offers `value`, which a key
    /// conditioned on that choice requires of its declaration.
    const fn owns_choice(
        keys: &'static [ClientConfigKey],
        address: &str,
        option: &str,
        value: &str,
    ) -> bool {
        let mut key = 0;
        while key < keys.len() {
            if let ClientConfigValue::DriverUrl(url) = keys[key].value
                && Self::same_name(keys[key].name, address)
                && Self::offers_choice(url.owned, option, value)
            {
                return true;
            }
            key += 1;
        }
        false
    }

    /// Whether one of `owned` is `option` with `value` among its choices.
    const fn offers_choice(owned: &[OwnedQueryOption], option: &str, value: &str) -> bool {
        let mut index = 0;
        while index < owned.len() {
            if Self::same_name(owned[index].name, option) {
                let choices = owned[index].choices;
                let mut choice = 0;
                while choice < choices.len() {
                    if Self::same_name(choices[choice], value) {
                        return true;
                    }
                    choice += 1;
                }
            }
            index += 1;
        }
        false
    }

    const fn same_name(left: &str, right: &str) -> bool {
        !Self::name_precedes(left, right) && !Self::name_precedes(right, left)
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
        } else if self.keys.iter().any(|key| {
            matches!(
                key.value,
                ClientConfigValue::DriverAddress | ClientConfigValue::DriverUrl(_)
            )
        }) {
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

    /// Whether `key` lies in one of the driver-delegated property namespaces of this schema.
    pub fn delegates(self, key: &str) -> bool {
        self.namespaces
            .iter()
            .any(|namespace| namespace.accepts(key))
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
                if self.delegates(&entry.key) {
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
        self.validate_conditional_presence(&values, stage)?;
        self.validate_query_presence(&values, stage)
    }

    /// Holds each key conditioned on a Nervix-owned query option to the choice its address sets.
    fn validate_query_presence(
        self,
        values: &BTreeMap<&str, &str>,
        stage: ConfigValueStage,
    ) -> Result<(), Report<ClientConfigSchemaError>> {
        for key in self.keys {
            let ClientConfigPresence::OptionalForQuery {
                address,
                option,
                value,
            } = key.presence
            else {
                continue;
            };
            if !values.contains_key(key.name) {
                continue;
            }
            let refused = || {
                Report::new(ClientConfigSchemaError::RequiresQuery {
                    key: key.name,
                    address,
                    option,
                    value,
                })
            };
            let Some(written) = values.get(address) else {
                return Err(refused());
            };
            if matches!(stage, ConfigValueStage::Declared) && written.contains("{{") {
                continue;
            }
            let owned = self
                .owned_option(address, option)
                .assured("the schema constructor checked that the address owns this option");
            let parsed = Url::parse(written).change_context_lazy(|| {
                ClientConfigSchemaError::InvalidValue {
                    key: address.to_string(),
                    expected: "URL",
                }
            })?;
            if owned.value_in(address, &parsed)? != value {
                return Err(refused());
            }
        }
        Ok(())
    }

    /// The Nervix-owned query option `option` of the driver URL key `address`, if it declares one.
    fn owned_option(self, address: &str, option: &str) -> Option<OwnedQueryOption> {
        let ClientConfigValue::DriverUrl(url) = self.key(address)?.value else {
            return None;
        };
        url.owned_option(option)
    }

    fn validate_conditional_presence(
        self,
        values: &BTreeMap<&str, &str>,
        stage: ConfigValueStage,
    ) -> Result<(), Report<ClientConfigSchemaError>> {
        for key in self.keys {
            let (address, scheme) = match key.presence {
                ClientConfigPresence::RequiredForScheme { address, scheme }
                | ClientConfigPresence::OptionalForScheme { address, scheme }
                | ClientConfigPresence::ExactlyForScheme { address, scheme } => (address, scheme),
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
                ClientConfigPresence::ExactlyForScheme { .. }
                    if present && parsed.scheme() != scheme =>
                {
                    return Err(Report::new(ClientConfigSchemaError::RequiresScheme {
                        key: key.name,
                        address,
                        scheme,
                    }));
                }
                ClientConfigPresence::ExactlyForScheme { .. }
                    if !present && parsed.scheme() == scheme =>
                {
                    return Err(Report::new(ClientConfigSchemaError::RequiresKey {
                        key: address,
                        required: key.name,
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
