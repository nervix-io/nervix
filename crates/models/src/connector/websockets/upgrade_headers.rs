//! The application headers a WebSockets connection writes in its HTTP upgrade.
//!
//! Layer: vocabulary.
//! - **Owns.** Literal upgrade header Models, their conversion into validated HTTP fields, and the
//!   subprotocol offer, the one protocol field a statement may write.
//! - **Depends on.** Shared HTTP application-header rules and serialization.
//! - **Must not know.** WebSocket connections, HTTP clients or runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    HttpApplicationHeaders, HttpHeaderName, HttpHeaderValue, http_request::http_token_byte,
};

/// The upgrade field that offers subprotocols, in the lowercase spelling field names take.
const SUBPROTOCOL_FIELD: &str = "sec-websocket-protocol";

/// One literal field, in the statement's written order. The registry validates it before commit.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WebsocketsUpgradeHeader {
    pub name: String,
    pub value: String,
}

/// Application fields validated for a WebSockets upgrade: every protocol-owned name is refused
/// except the subprotocol offer, which holds its canonical tokens.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebsocketsUpgradeHeaders(HttpApplicationHeaders);

/// A refusal names the field and, for an offer, the position of its element; never a value, which
/// can carry a credential.
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
    #[error("upgrade header 'sec-websocket-protocol' has an empty element {element}")]
    EmptySubprotocol { element: usize },
    #[error("upgrade header 'sec-websocket-protocol' element {element} is not a subprotocol token")]
    InvalidSubprotocol { element: usize },
    #[error(
        "upgrade header 'sec-websocket-protocol' element {element} repeats an earlier subprotocol"
    )]
    RepeatedSubprotocol { element: usize },
}

impl WebsocketsUpgradeHeaders {
    /// Applies HTTP field rules, the protocol-owned WebSocket field restrictions and the token
    /// rules of a subprotocol offer.
    pub fn parse(
        headers: &[WebsocketsUpgradeHeader],
    ) -> error_stack::Result<Self, WebsocketsUpgradeHeadersError> {
        let mut names = BTreeSet::new();
        let mut validated = HttpApplicationHeaders::default();
        for header in headers {
            let lowercase = header.name.to_ascii_lowercase();
            let offers_subprotocols = lowercase == SUBPROTOCOL_FIELD;
            if !offers_subprotocols && lowercase.starts_with("sec-websocket-") {
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
            let value = if offers_subprotocols {
                Self::subprotocol_offer(&header.value)?
            } else {
                HttpHeaderValue::parse(&header.value).change_context_lazy(|| {
                    WebsocketsUpgradeHeadersError::InvalidValue {
                        name: header.name.clone(),
                    }
                })?
            };
            validated
                .insert(name, value)
                .change_context(WebsocketsUpgradeHeadersError::Limit)?;
        }
        validated
            .validate_total()
            .change_context(WebsocketsUpgradeHeadersError::Limit)?;
        Ok(Self(validated))
    }

    /// An offer lists one or more unique subprotocol tokens (RFC 6455 section 4.1), separated by
    /// commas with the optional spaces or tabs HTTP allows around each comma. Elements are counted
    /// from 1. The validated value is the tokens joined by commas alone: the transport splits the
    /// request's offer at each comma, without trimming, before comparing the peer's selection with
    /// each element, so written whitespace would become part of a token.
    fn subprotocol_offer(
        value: &str,
    ) -> error_stack::Result<HttpHeaderValue, WebsocketsUpgradeHeadersError> {
        let mut offered = BTreeSet::new();
        let mut tokens = Vec::new();
        for (index, element) in value.split(',').enumerate() {
            let element_number = index
                .checked_add(1)
                .assured("an offer has fewer elements than bytes, so far fewer than usize::MAX");
            let protocol = element.trim_matches([' ', '\t']);
            if protocol.is_empty() {
                return Err(Report::new(
                    WebsocketsUpgradeHeadersError::EmptySubprotocol {
                        element: element_number,
                    },
                ));
            }
            if !protocol.bytes().all(http_token_byte) {
                return Err(Report::new(
                    WebsocketsUpgradeHeadersError::InvalidSubprotocol {
                        element: element_number,
                    },
                ));
            }
            if !offered.insert(protocol) {
                return Err(Report::new(
                    WebsocketsUpgradeHeadersError::RepeatedSubprotocol {
                        element: element_number,
                    },
                ));
            }
            tokens.push(protocol);
        }
        let canonical = tokens.join(",");
        HttpHeaderValue::parse(&canonical).change_context_lazy(|| {
            WebsocketsUpgradeHeadersError::InvalidValue {
                name: SUBPROTOCOL_FIELD.to_string(),
            }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&HttpHeaderName, &HttpHeaderValue)> {
        self.0.iter()
    }
}
