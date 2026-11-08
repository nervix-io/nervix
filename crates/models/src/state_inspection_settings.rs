//! Local capability bounds for operator state reads.
//!
//! Layer: vocabulary.
//! - **Owns.** Validated row, section and elapsed-time bounds of one state inspection.
//! - **Depends on.** Setting definitions and the shared vocabulary and addressable collection bounds.
//! - **Must not know.** Plexus parsing, sessions, schedules or snapshot contents.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the boundary declared above")
)]

use std::{num::NonZeroUsize, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};

use crate::{NodeCapabilities, Setting, SettingDefinition};

#[derive(Debug, thiserror::Error)]
pub enum StateInspectionSettingsError {
    #[error("{setting} ({value}) exceeds the 32-bit collection bound ({limit})")]
    Collection {
        setting: &'static str,
        value: usize,
        limit: usize,
    },
    #[error("inspection-read-timeout must be positive and fit an unsigned 32-bit seconds bound")]
    Timeout,
}

pub struct StateInspectionMaxRows;
pub struct StateInspectionMaxSections;
pub struct StateInspectionReadTimeout;

macro_rules! inspection_count {
    ($definition:ident, $name:literal, $default:literal) => {
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = NonZeroUsize;
            type Context = ();
            type Error = StateInspectionSettingsError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> NonZeroUsize {
                NonZeroUsize::new($default).assured("the declared capability default is nonzero")
            }
            fn validate(value: &NonZeroUsize, _: &()) -> Result<(), Report<Self::Error>> {
                let limit =
                    usize::try_from(u32::MAX).assured("u32 lengths fit supported platforms");
                if value.get() > limit {
                    return Err(Report::new(StateInspectionSettingsError::Collection {
                        setting: Self::NAME,
                        value: value.get(),
                        limit,
                    }));
                }
                Ok(())
            }
        }
    };
}

inspection_count!(StateInspectionMaxRows, "inspection-max-rows", 1024);
inspection_count!(StateInspectionMaxSections, "inspection-max-sections", 128);

impl StateInspectionReadTimeout {
    pub const DEFAULT: Duration = Duration::from_secs(10);
}

impl SettingDefinition for StateInspectionReadTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = StateInspectionSettingsError;
    const NAME: &'static str = "inspection-read-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() || value.as_secs() > u64::from(u32::MAX) {
            return Err(Report::new(StateInspectionSettingsError::Timeout));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateInspectionSettings {
    pub max_rows: Setting<StateInspectionMaxRows>,
    pub max_sections: Setting<StateInspectionMaxSections>,
    pub read_timeout: Setting<StateInspectionReadTimeout>,
}

impl StateInspectionSettings {
    pub fn resolve(
        max_rows: Option<NonZeroUsize>,
        max_sections: Option<NonZeroUsize>,
        read_timeout: Option<Duration>,
    ) -> Result<Self, Report<StateInspectionSettingsError>> {
        Ok(Self {
            max_rows: Setting::resolve(max_rows, &())?,
            max_sections: Setting::resolve(max_sections, &())?,
            read_timeout: Setting::resolve(read_timeout, &())?,
        })
    }
}

impl Default for StateInspectionSettings {
    fn default() -> Self {
        Self::resolve(None, None, None).assured("inspection defaults satisfy their declared bounds")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspection_capabilities_refuse_unrepresentable_counts_and_deadlines() {
        let one = NonZeroUsize::new(1).expect("one is nonzero");
        assert!(
            StateInspectionSettings::resolve(Some(one), Some(one), Some(Duration::from_nanos(1)))
                .is_ok()
        );
        assert!(StateInspectionSettings::resolve(None, None, Some(Duration::ZERO)).is_err());
        assert!(
            StateInspectionSettings::resolve(
                None,
                None,
                Some(Duration::from_secs(u64::from(u32::MAX) + 1))
            )
            .is_err()
        );
        if let Ok(count) = usize::try_from(u64::from(u32::MAX) + 1) {
            let count = NonZeroUsize::new(count).expect("above the nonzero u32 bound");
            assert!(StateInspectionSettings::resolve(Some(count), None, None).is_err());
            assert!(StateInspectionSettings::resolve(None, Some(count), None).is_err());
        }
    }
}
