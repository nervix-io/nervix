//! Resolver settings at the command-line edge.
//!
//! Layer: edges.
//! - **Owns.** Parsing and rendering resolver setting inputs.
//! - **Depends on.** Resolver declarations and edge durations.
//! - **Must not know.** DNS queries, peers or resolver lifetime.
#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns DNS argument conversion")
)]
use error_stack::Report;
use nervix_dns::{DnsSettingValues, DnsSettings, DnsSettingsError};
use nervix_models::SettingDefinition as _;

use super::settings::CommandLine;
use crate::application::cli_values::HumanDuration;

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "DNS")]
pub struct DnsArgs {
    #[arg(long = nervix_dns::CacheEntries::NAME, env = "NERVIX_DNS_CACHE_ENTRIES", help = super::settings::capability_help::<nervix_dns::CacheEntries>("Cache entries", nervix_dns::CacheEntries::default_value(&())))]
    pub cache_entries: Option<u64>,
    #[arg(long = nervix_dns::PositiveMaxTtl::NAME, env = "NERVIX_DNS_POSITIVE_MAX_TTL", help = super::settings::capability_help::<nervix_dns::PositiveMaxTtl>("Positive max ttl", HumanDuration(nervix_dns::PositiveMaxTtl::default_value(&()))))]
    pub positive_max_ttl: Option<HumanDuration>,
    #[arg(long = nervix_dns::NegativeMaxTtl::NAME, env = "NERVIX_DNS_NEGATIVE_MAX_TTL", help = super::settings::capability_help::<nervix_dns::NegativeMaxTtl>("Negative max ttl", HumanDuration(nervix_dns::NegativeMaxTtl::default_value(&()))))]
    pub negative_max_ttl: Option<HumanDuration>,
    #[arg(long = nervix_dns::HookLookupBudget::NAME, env = "NERVIX_DNS_HOOK_LOOKUP_BUDGET", help = super::settings::capability_help::<nervix_dns::HookLookupBudget>("Hook lookup budget", HumanDuration(nervix_dns::HookLookupBudget::default_value(&()))))]
    pub hook_lookup_budget: Option<HumanDuration>,
    #[arg(long = nervix_dns::AddressFamilyPolicy::NAME, env = "NERVIX_DNS_ADDRESS_FAMILY_POLICY", help = super::settings::capability_help::<nervix_dns::AddressFamilyPolicy>("Address family policy", nervix_dns::AddressFamilyPolicy::default_value(&())))]
    pub address_family_policy: Option<nervix_dns::AddressFamilies>,
    #[arg(long = nervix_dns::TrustNegativeAnswers::NAME, env = "NERVIX_DNS_TRUST_NEGATIVE_ANSWERS", help = super::settings::capability_help::<nervix_dns::TrustNegativeAnswers>("Trust negative answers", nervix_dns::TrustNegativeAnswers::default_value(&())))]
    pub trust_negative_answers: Option<bool>,
}
impl DnsArgs {
    pub(super) fn settings(&self, peers: usize) -> Result<DnsSettings, Report<DnsSettingsError>> {
        DnsSettings::resolve(
            DnsSettingValues {
                cache_entries: self.cache_entries,
                positive_max_ttl: self.positive_max_ttl.map(|value| value.0),
                negative_max_ttl: self.negative_max_ttl.map(|value| value.0),
                hook_lookup_budget: self.hook_lookup_budget.map(|value| value.0),
                address_family_policy: self.address_family_policy,
                trust_negative_answers: self.trust_negative_answers,
            },
            peers,
        )
    }
    pub(super) fn from_settings(settings: &DnsSettings) -> Self {
        Self {
            cache_entries: Some(*settings.cache_entries.value()),
            positive_max_ttl: Some(HumanDuration(*settings.positive_max_ttl.value())),
            negative_max_ttl: Some(HumanDuration(*settings.negative_max_ttl.value())),
            hook_lookup_budget: Some(HumanDuration(*settings.hook_lookup_budget.value())),
            address_family_policy: Some(*settings.address_family_policy.value()),
            trust_negative_answers: Some(*settings.trust_negative_answers.value()),
        }
    }
    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(nervix_dns::CacheEntries::NAME, self.cache_entries);
        command_line.optional(nervix_dns::PositiveMaxTtl::NAME, self.positive_max_ttl);
        command_line.optional(nervix_dns::NegativeMaxTtl::NAME, self.negative_max_ttl);
        command_line.optional(nervix_dns::HookLookupBudget::NAME, self.hook_lookup_budget);
        command_line.optional(
            nervix_dns::AddressFamilyPolicy::NAME,
            self.address_family_policy,
        );
        command_line.optional(
            nervix_dns::TrustNegativeAnswers::NAME,
            self.trust_negative_answers,
        );
    }
}
