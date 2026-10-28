//! Percent-encoded connector header configuration.
//!
//! Layer: vocabulary.
//! - **Owns.** Ordered header names and once-decoded values from declared configuration.
//! - **Depends on.** Percent encoding and typed configuration diagnostics.
//! - **Must not know.** HTTP clients, transports or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns percent-encoded header configuration")
)]

use error_stack::Report;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderPair {
    name: String,
    value: String,
}

impl HeaderPair {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PercentEncodedHeaders {
    pairs: Vec<HeaderPair>,
}

#[derive(Debug, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "declared header format refusals contain no input values"
    )
)]
pub enum HeaderPairsError {
    #[error("header entries require name=value")]
    Entry,
    #[error("header entries require a nonempty name")]
    Name,
}

impl PercentEncodedHeaders {
    pub fn parse(raw: &str) -> Result<Self, Report<HeaderPairsError>> {
        let mut pairs = Vec::new();
        if raw.trim().is_empty() {
            return Ok(Self { pairs });
        }
        for entry in raw.split(',') {
            let Some((name, value)) = entry.split_once('=') else {
                return Err(Report::new(HeaderPairsError::Entry));
            };
            let name = name.trim().to_ascii_lowercase();
            if name.is_empty() {
                return Err(Report::new(HeaderPairsError::Name));
            }
            let value = percent_encoding::percent_decode_str(value.trim())
                .decode_utf8_lossy()
                .into_owned();
            pairs.push(HeaderPair { name, value });
        }
        Ok(Self { pairs })
    }

    pub fn into_pairs(self) -> Vec<HeaderPair> {
        self.pairs
    }
}

#[cfg(test)]
mod tests;
