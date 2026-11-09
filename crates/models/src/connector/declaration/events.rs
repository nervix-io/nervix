//! Layer: vocabulary.
//! - **Owns.** Closed connector event identities and each entity's declared event set.
//! - **Depends on.** Serialization and enum presentation.
//! - **Must not know.** Metric registries, host handles, drivers or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns declared event vocabulary")
)]

/// A bounded transport observation an entity may declare, independent of delivery outcomes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    strum::AsRefStr,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ConnectorEvent {
    /// Redis accepted a PUBLISH and reported no channel receivers.
    PublishedWithoutSubscribers,
}

/// The entity's complete current event set. Names come only from [`ConnectorEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(transparent)]
pub struct DeclaredEvents(&'static [ConnectorEvent]);

impl DeclaredEvents {
    /// An entity with no transport-specific event observations.
    pub const NONE: Self = Self(&[]);

    /// Declares each event at most once, in documentation order.
    pub const fn new(events: &'static [ConnectorEvent]) -> Self {
        Self(events)
    }

    pub const fn as_slice(self) -> &'static [ConnectorEvent] {
        self.0
    }

    /// The scan is bounded by the closed vocabulary's event identities.
    pub fn contains(self, event: ConnectorEvent) -> bool {
        self.0.contains(&event)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests remain outside product execution")
)]
mod tests;
