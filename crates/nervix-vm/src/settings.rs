//! Node-local expression execution capabilities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** VM admission and cache settings, their defaults and validated construction.
//! - **Depends on.** Vocabulary setting classes and scalar counts.
//! - **Must not know.** Graphs, command-line arguments or the node that supplies its settings.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns VM execution capabilities")
)]

use std::num::NonZeroUsize;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};

#[derive(Debug, thiserror::Error)]
pub enum VmSettingsError {
    #[error("{setting} must be positive")]
    Zero { setting: &'static str },
    #[error("regex search cache size {bytes} cannot be addressed on this host")]
    CacheSize { bytes: u64 },
}

pub struct VmInlineRowLimit;

impl VmInlineRowLimit {
    pub const DEFAULT: usize = 1_024;
}

impl SettingDefinition for VmInlineRowLimit {
    type Class = NodeCapabilities;
    type Value = usize;
    type Context = ();
    type Error = VmSettingsError;
    const NAME: &'static str = "vm-inline-row-limit";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> usize {
        Self::DEFAULT
    }
    fn validate(_: &usize, _: &()) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}

macro_rules! cache_setting {
    ($definition:ident, $name:literal, $default:expr) => {
        pub struct $definition;
        impl $definition {
            pub const DEFAULT: usize = $default;
        }
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = NonZeroUsize;
            type Context = ();
            type Error = VmSettingsError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> NonZeroUsize {
                NonZeroUsize::new(Self::DEFAULT).assured("cache defaults are positive")
            }
            fn validate(_: &NonZeroUsize, _: &()) -> Result<(), Report<Self::Error>> {
                Ok(())
            }
        }
    };
}

cache_setting!(
    VmRegexSearchCacheBytes,
    "vm-regex-search-cache-bytes",
    2 * (1 << 20)
);
cache_setting!(
    VmDynamicPatternCacheCapacity,
    "vm-dynamic-pattern-cache-capacity",
    64
);
cache_setting!(VmPatternSetsPerBatch, "vm-pattern-sets-per-batch", 64);

/// Raw local overrides, normalized once before compilation.
#[derive(Debug, Clone, Copy, Default)]
pub struct VmSettingOverrides {
    pub inline_row_limit: Option<usize>,
    pub regex_search_cache_bytes: Option<u64>,
    pub dynamic_pattern_cache_capacity: Option<usize>,
    pub pattern_sets_per_batch: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VmSettings {
    inline_row_limit: Setting<VmInlineRowLimit>,
    regex_search_cache_bytes: Setting<VmRegexSearchCacheBytes>,
    dynamic_pattern_cache_capacity: Setting<VmDynamicPatternCacheCapacity>,
    pattern_sets_per_batch: Setting<VmPatternSetsPerBatch>,
}

impl VmSettings {
    pub fn resolve(values: VmSettingOverrides) -> Result<Self, Report<VmSettingsError>> {
        let search_bytes = match values.regex_search_cache_bytes {
            Some(bytes) => Some(usize::try_from(bytes).map_err(|source| {
                Report::new(source).change_context(VmSettingsError::CacheSize { bytes })
            })?),
            None => None,
        };
        Ok(Self {
            inline_row_limit: Setting::resolve(values.inline_row_limit, &())?,
            regex_search_cache_bytes: Self::cache(search_bytes)?,
            dynamic_pattern_cache_capacity: Self::cache(values.dynamic_pattern_cache_capacity)?,
            pattern_sets_per_batch: Self::cache(values.pattern_sets_per_batch)?,
        })
    }

    fn cache<D>(value: Option<usize>) -> Result<Setting<D>, Report<VmSettingsError>>
    where
        D: SettingDefinition<Value = NonZeroUsize, Context = (), Error = VmSettingsError>,
    {
        let value = match value {
            Some(value) => match NonZeroUsize::new(value) {
                Some(value) => Some(value),
                None => return Err(Report::new(VmSettingsError::Zero { setting: D::NAME })),
            },
            None => None,
        };
        Setting::resolve(value, &())
    }

    pub fn inline_row_limit(self) -> usize {
        *self.inline_row_limit.value()
    }
    pub fn regex_search_cache_bytes(self) -> usize {
        self.regex_search_cache_bytes.value().get()
    }
    pub fn dynamic_pattern_cache_capacity(self) -> usize {
        self.dynamic_pattern_cache_capacity.value().get()
    }
    pub fn pattern_sets_per_batch(self) -> NonZeroUsize {
        *self.pattern_sets_per_batch.value()
    }
}

impl Default for VmSettings {
    fn default() -> Self {
        Self::resolve(VmSettingOverrides::default()).assured("the VM defaults are valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_explicit_capabilities_have_one_owner() {
        let defaults = VmSettings::default();
        assert_eq!(defaults.inline_row_limit(), VmInlineRowLimit::DEFAULT);
        assert_eq!(
            defaults.regex_search_cache_bytes(),
            VmRegexSearchCacheBytes::DEFAULT
        );
        let settings = VmSettings::resolve(VmSettingOverrides {
            inline_row_limit: Some(0),
            regex_search_cache_bytes: Some(4096),
            dynamic_pattern_cache_capacity: Some(2),
            pattern_sets_per_batch: Some(3),
        })
        .assured("positive cache capabilities validate");
        assert_eq!(settings.inline_row_limit(), 0);
        assert_eq!(settings.regex_search_cache_bytes(), 4096);
        assert_eq!(settings.dynamic_pattern_cache_capacity(), 2);
        assert_eq!(settings.pattern_sets_per_batch().get(), 3);
    }

    #[test]
    fn every_cache_refuses_zero_before_compilation() {
        for values in [
            VmSettingOverrides {
                regex_search_cache_bytes: Some(0),
                ..Default::default()
            },
            VmSettingOverrides {
                dynamic_pattern_cache_capacity: Some(0),
                ..Default::default()
            },
            VmSettingOverrides {
                pattern_sets_per_batch: Some(0),
                ..Default::default()
            },
        ] {
            assert!(matches!(
                VmSettings::resolve(values).unwrap_err().current_context(),
                VmSettingsError::Zero { .. }
            ));
        }
    }
}
