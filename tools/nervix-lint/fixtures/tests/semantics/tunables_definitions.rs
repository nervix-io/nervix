//! A setting definition's own default, which its `default_value` returns, is that setting's one
//! default. The definition's bounds and another type's constants are not.
use core::time::Duration;
use nervix_lint_fixtures::settings::SettingDefinition;

pub struct Interval;

impl Interval {
    /// The definition's one default, which `default_value` returns.
    pub const DEFAULT: Duration = Duration::from_millis(100);
    /// A bound the definition does not return as its default.
    pub const MAXIMUM: Duration = Duration::from_secs(60);
}

impl SettingDefinition for Interval {
    type Value = Duration;

    fn default_value() -> Duration {
        Self::DEFAULT
    }
}

pub struct Elsewhere;

impl Elsewhere {
    /// Read by a definition's default, but owned by a type that is no definition.
    pub const CEILING: u64 = 7;
}

pub struct Count;

impl SettingDefinition for Count {
    type Value = u64;

    fn default_value() -> u64 {
        Elsewhere::CEILING
    }
}
