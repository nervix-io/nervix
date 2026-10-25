//! The application headers a WebSockets connection writes in its HTTP upgrade.
//!
//! Layer: vocabulary.
//! - **Owns.** Literal upgrade header Models and their conversion into validated HTTP fields.
//! - **Depends on.** Shared HTTP application-header rules and serialization.
//! - **Must not know.** WebSocket connections, HTTP clients or runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{HttpApplicationHeaders, HttpHeaderName, HttpHeaderValue};

/// One literal field, in the statement's written order. The registry validates it before commit.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WebsocketsUpgradeHeader {
    pub name: String,
    pub value: String,
}

/// Application fields validated for a WebSockets upgrade, including protocol-owned names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebsocketsUpgradeHeaders(HttpApplicationHeaders);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "pure validation of literal WebSockets upgrade headers"
    )
)]
pub enum WebsocketsUpgradeHeadersError {
    #[error("upgrade header '{name}' is invalid or reserved")]
    InvalidName { name: String },
    #[error("upgrade header '{name}' has an invalid value")]
    InvalidValue { name: String },
    #[error("upgrade header '{name}' is set more than once")]
    RepeatedName { name: String },
    #[error("upgrade headers exceed the HTTP application-header bounds")]
    Limit,
}

impl WebsocketsUpgradeHeaders {
    /// Applies HTTP field rules plus the protocol-owned WebSocket field restrictions.
    pub fn parse(
        headers: &[WebsocketsUpgradeHeader],
    ) -> error_stack::Result<Self, WebsocketsUpgradeHeadersError> {
        let mut names = BTreeSet::new();
        let mut validated = HttpApplicationHeaders::default();
        for header in headers {
            if header
                .name
                .to_ascii_lowercase()
                .starts_with("sec-websocket-")
            {
                return Err(Report::new(WebsocketsUpgradeHeadersError::InvalidName {
                    name: header.name.clone(),
                }));
            }
            let name = HttpHeaderName::parse(&header.name).change_context_lazy(|| {
                WebsocketsUpgradeHeadersError::InvalidName {
                    name: header.name.clone(),
                }
            })?;
            if !names.insert(name.clone()) {
                return Err(Report::new(WebsocketsUpgradeHeadersError::RepeatedName {
                    name: header.name.clone(),
                }));
            }
            let value = HttpHeaderValue::parse(&header.value).change_context_lazy(|| {
                WebsocketsUpgradeHeadersError::InvalidValue {
                    name: header.name.clone(),
                }
            })?;
            validated
                .insert(name, value)
                .change_context(WebsocketsUpgradeHeadersError::Limit)?;
        }
        validated
            .validate_total()
            .change_context(WebsocketsUpgradeHeadersError::Limit)?;
        Ok(Self(validated))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&HttpHeaderName, &HttpHeaderValue)> {
        self.0.iter()
    }
}
