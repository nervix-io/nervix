//! Local bounds for a Syslog listener's stream intake.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Stream queue and read-chunk capabilities, their defaults and channel-bound checks.
//! - **Depends on.** Vocabulary setting classes and the native primitive channel bound.
//! - **Must not know.** Graphs, message framing syntax, external client keys or CLI parsing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns Syslog stream intake capabilities")
)]

use std::num::NonZeroUsize;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use nervix_primitives::sync::Semaphore;

#[derive(Debug, thiserror::Error)]
pub enum SyslogListenerSettingsError {
    #[error("syslog-read-chunk-bytes must be positive and fit the native address space: {bytes}")]
    ReadChunk { bytes: u64 },
    #[error("syslog-intake-queue-capacity {value} exceeds the native channel limit")]
    QueueCapacity { value: usize },
}
pub struct SyslogIntakeQueueCapacity;
impl SettingDefinition for SyslogIntakeQueueCapacity {
    type Class = NodeCapabilities;
    type Value = NonZeroUsize;
    type Context = ();
    type Error = SyslogListenerSettingsError;
    const NAME: &'static str = "syslog-intake-queue-capacity";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> NonZeroUsize {
        NonZeroUsize::new(64).assured("the stream queue default is positive")
    }
    fn validate(value: &NonZeroUsize, _: &()) -> Result<(), Report<Self::Error>> {
        if value.get() > Semaphore::MAX_PERMITS {
            return Err(Report::new(SyslogListenerSettingsError::QueueCapacity {
                value: value.get(),
            }));
        }
        Ok(())
    }
}
pub struct SyslogReadChunkBytes;
impl SettingDefinition for SyslogReadChunkBytes {
    type Class = NodeCapabilities;
    type Value = NonZeroUsize;
    type Context = ();
    type Error = SyslogListenerSettingsError;
    const NAME: &'static str = "syslog-read-chunk-bytes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> NonZeroUsize {
        NonZeroUsize::new(8192).assured("the read chunk default is positive")
    }
    fn validate(_: &NonZeroUsize, _: &()) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyslogListenerSettings {
    queue_capacity: Setting<SyslogIntakeQueueCapacity>,
    read_chunk_bytes: Setting<SyslogReadChunkBytes>,
}
impl SyslogListenerSettings {
    pub fn resolve(
        queue: Option<NonZeroUsize>,
        chunk: Option<u64>,
    ) -> Result<Self, Report<SyslogListenerSettingsError>> {
        let chunk = match chunk {
            Some(bytes) => {
                let value = usize::try_from(bytes).map_err(|source| {
                    Report::new(source)
                        .change_context(SyslogListenerSettingsError::ReadChunk { bytes })
                })?;
                let Some(value) = NonZeroUsize::new(value) else {
                    return Err(Report::new(SyslogListenerSettingsError::ReadChunk {
                        bytes,
                    }));
                };
                Some(value)
            }
            None => None,
        };
        Ok(Self {
            queue_capacity: Setting::resolve(queue, &())?,
            read_chunk_bytes: Setting::resolve(chunk, &())?,
        })
    }
    pub fn queue_capacity(self) -> usize {
        self.queue_capacity.value().get()
    }
    pub fn read_chunk_bytes(self) -> usize {
        self.read_chunk_bytes.value().get()
    }
}
impl Default for SyslogListenerSettings {
    fn default() -> Self {
        Self::resolve(None, None).assured("listener capability defaults validate")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_native_bounds_are_refused() {
        assert!(SyslogListenerSettings::resolve(None, Some(0)).is_err());
        assert!(SyslogListenerSettings::resolve(NonZeroUsize::new(usize::MAX), None).is_err());
        let settings = SyslogListenerSettings::resolve(NonZeroUsize::new(2), Some(3))
            .assured("small bounds validate");
        assert_eq!(
            (settings.queue_capacity(), settings.read_chunk_bytes()),
            (2, 3)
        );
    }
}
