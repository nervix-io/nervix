//! Local resolver policy and peer-relative admission declarations.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Resolver cache, TTL, address selection, negative-answer trust and hook budgets.
//! - **Depends on.** Vocabulary setting classes and positive peer capacity.
//! - **Must not know.** Peers, connectors, graph policy or command-line parsing.
#![cfg_attr(nervix_lint, nervix::layer(engine, reason = "owns resolver policy"))]
use std::time::Duration;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use nervix_primitives::time::Instant;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("{setting} must be positive and representable")]
pub struct DnsSettingsError {
    pub setting: &'static str,
}

pub use nervix_models::DnsAddressFamilies as AddressFamilies;

/// The resolver's `cache-entries` policy input.
pub struct CacheEntries;
impl SettingDefinition for CacheEntries {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = DnsSettingsError;
    const NAME: &'static str = "dns-cache-entries";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u64 {
        4096
    }
    fn validate(value: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0 || usize::try_from(*value).is_err() {
            return Err(Report::new(DnsSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}

/// The resolver's `positive-max-ttl` policy input.
pub struct PositiveMaxTtl;
impl SettingDefinition for PositiveMaxTtl {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = DnsSettingsError;
    const NAME: &'static str = "dns-positive-max-ttl";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(3600)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() || Instant::now().checked_add(*value).is_none() {
            return Err(Report::new(DnsSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}

/// The resolver's `negative-max-ttl` policy input.
pub struct NegativeMaxTtl;
impl SettingDefinition for NegativeMaxTtl {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = DnsSettingsError;
    const NAME: &'static str = "dns-negative-max-ttl";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(30)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() || Instant::now().checked_add(*value).is_none() {
            return Err(Report::new(DnsSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}

/// The resolver's `hook-lookup-budget` policy input.
pub struct HookLookupBudget;
impl SettingDefinition for HookLookupBudget {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = DnsSettingsError;
    const NAME: &'static str = "dns-hook-lookup-budget";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(30)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() || Instant::now().checked_add(*value).is_none() {
            return Err(Report::new(DnsSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}

/// The resolver's `address-family-policy` policy input.
pub struct AddressFamilyPolicy;
impl SettingDefinition for AddressFamilyPolicy {
    type Class = NodeCapabilities;
    type Value = AddressFamilies;
    type Context = ();
    type Error = DnsSettingsError;
    const NAME: &'static str = "dns-address-family-policy";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> AddressFamilies {
        AddressFamilies::DualStack
    }
    fn validate(_: &Self::Value, _: &()) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}

/// The resolver's `trust-negative-answers` policy input.
pub struct TrustNegativeAnswers;
impl SettingDefinition for TrustNegativeAnswers {
    type Class = NodeCapabilities;
    type Value = bool;
    type Context = ();
    type Error = DnsSettingsError;
    const NAME: &'static str = "dns-trust-negative-answers";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> bool {
        true
    }
    fn validate(_: &Self::Value, _: &()) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}

/// Concurrent DNS lookups: one for every declared peer.
pub struct ConcurrentLookups;
impl SettingDefinition for ConcurrentLookups {
    type Class = NodeCapabilities;
    type Value = usize;
    type Context = usize;
    type Error = DnsSettingsError;
    const NAME: &'static str = "dns-concurrent-lookups";
    const DERIVATION: Option<&'static str> = Some("interconnect-max-peers");
    fn default_value(peers: &usize) -> usize {
        *peers
    }
    fn validate(value: &usize, _: &usize) -> Result<(), Report<Self::Error>> {
        if *value == 0 || *value > nervix_primitives::sync::Semaphore::MAX_PERMITS {
            return Err(Report::new(DnsSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DnsSettingValues {
    pub cache_entries: Option<u64>,
    pub positive_max_ttl: Option<Duration>,
    pub negative_max_ttl: Option<Duration>,
    pub hook_lookup_budget: Option<Duration>,
    pub address_family_policy: Option<AddressFamilies>,
    pub trust_negative_answers: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DnsSettings {
    pub cache_entries: Setting<CacheEntries>,
    pub positive_max_ttl: Setting<PositiveMaxTtl>,
    pub negative_max_ttl: Setting<NegativeMaxTtl>,
    pub hook_lookup_budget: Setting<HookLookupBudget>,
    pub address_family_policy: Setting<AddressFamilyPolicy>,
    pub trust_negative_answers: Setting<TrustNegativeAnswers>,
    pub concurrent_lookups: Setting<ConcurrentLookups>,
}
impl DnsSettings {
    pub fn capabilities(&self) -> nervix_models::DnsCapabilities {
        nervix_models::DnsCapabilities {
            cache_entries: *self.cache_entries.value(),
            positive_max_ttl: *self.positive_max_ttl.value(),
            negative_max_ttl: *self.negative_max_ttl.value(),
            hook_lookup_budget: *self.hook_lookup_budget.value(),
            address_families: *self.address_family_policy.value(),
            trust_negative_answers: *self.trust_negative_answers.value(),
            concurrent_lookups: std::num::NonZeroU64::new(
                u64::try_from(*self.concurrent_lookups.value())
                    .assured("supported targets have at most 64-bit counts"),
            )
            .assured("DNS admission validation requires a positive count"),
        }
    }
    pub fn resolve(
        values: DnsSettingValues,
        peers: usize,
    ) -> Result<Self, Report<DnsSettingsError>> {
        Ok(Self {
            cache_entries: Setting::resolve(values.cache_entries, &())?,
            positive_max_ttl: Setting::resolve(values.positive_max_ttl, &())?,
            negative_max_ttl: Setting::resolve(values.negative_max_ttl, &())?,
            hook_lookup_budget: Setting::resolve(values.hook_lookup_budget, &())?,
            address_family_policy: Setting::resolve(values.address_family_policy, &())?,
            trust_negative_answers: Setting::resolve(values.trust_negative_answers, &())?,
            concurrent_lookups: Setting::resolve(None, &peers)?,
        })
    }
}
impl Default for DnsSettings {
    fn default() -> Self {
        Self::resolve(
            DnsSettingValues::default(),
            nervix_models::MaxPeers::default_value(&()),
        )
        .assured("the declared resolver defaults are positive and bounded")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dns_admission_tracks_peers_and_rejects_an_unusable_hook_or_cache() {
        let settings = DnsSettings::resolve(DnsSettingValues::default(), 10)
            .assured("ten peers fit lookup admission");
        assert_eq!(*settings.concurrent_lookups.value(), 10);
        assert!(
            DnsSettings::resolve(
                DnsSettingValues {
                    cache_entries: Some(0),
                    ..Default::default()
                },
                10
            )
            .is_err()
        );
        assert!(
            DnsSettings::resolve(
                DnsSettingValues {
                    hook_lookup_budget: Some(Duration::ZERO),
                    ..Default::default()
                },
                10
            )
            .is_err()
        );
        assert!(DnsSettings::resolve(DnsSettingValues::default(), 0).is_err());
    }
}
