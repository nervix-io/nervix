//! Local Wasmtime construction capabilities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Explicit stack and address reservation settings and the bounded compilation policy.
//! - **Depends on.** Vocabulary setting classes and scalar sizes.
//! - **Must not know.** Graphs, guest state, command-line parsing or node lifecycle.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns WASM engine capabilities")
)]

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};

#[derive(Debug, thiserror::Error)]
pub enum WasmEngineSettingsError {
    #[error("wasm-stack-bytes must be positive and addressable on this host: {bytes}")]
    Stack { bytes: u64 },
}

pub struct WasmStackBytes;
impl SettingDefinition for WasmStackBytes {
    type Class = NodeCapabilities;
    type Value = usize;
    type Context = ();
    type Error = WasmEngineSettingsError;
    const NAME: &'static str = "wasm-stack-bytes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> usize {
        512 * 1024
    }
    fn validate(value: &usize, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0 {
            return Err(Report::new(WasmEngineSettingsError::Stack { bytes: 0 }));
        }
        Ok(())
    }
}

pub struct WasmMemoryReservationBytes;
impl SettingDefinition for WasmMemoryReservationBytes {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = WasmEngineSettingsError;
    const NAME: &'static str = "wasm-memory-reservation-bytes";
    const DERIVATION: Option<&'static str> =
        Some("4 GiB on a 64-bit host; 10 MiB on a 32-bit host");
    fn default_value(_: &()) -> u64 {
        if usize::BITS == 64 {
            1 << 32
        } else {
            10 * (1 << 20)
        }
    }
    fn validate(_: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmEngineSettings {
    stack_bytes: Setting<WasmStackBytes>,
    memory_reservation_bytes: Setting<WasmMemoryReservationBytes>,
}

impl WasmEngineSettings {
    pub fn resolve(
        stack_bytes: Option<u64>,
        memory_reservation_bytes: Option<u64>,
    ) -> Result<Self, Report<WasmEngineSettingsError>> {
        let stack_bytes = match stack_bytes {
            Some(bytes) => Some(usize::try_from(bytes).map_err(|source| {
                Report::new(source).change_context(WasmEngineSettingsError::Stack { bytes })
            })?),
            None => None,
        };
        Ok(Self {
            stack_bytes: Setting::resolve(stack_bytes, &())?,
            memory_reservation_bytes: Setting::resolve(memory_reservation_bytes, &())?,
        })
    }
    pub fn stack_bytes(self) -> usize {
        *self.stack_bytes.value()
    }
    pub fn memory_reservation_bytes(self) -> u64 {
        *self.memory_reservation_bytes.value()
    }
    /// Compilation already occupies the node's bounded bulk workers; nested fan-out is disabled.
    pub const fn parallel_compilation(self) -> bool {
        false
    }
}

impl Default for WasmEngineSettings {
    fn default() -> Self {
        Self::resolve(None, None).assured("WASM engine defaults validate")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_construction_keeps_compilation_on_the_bulk_workers() {
        let settings = WasmEngineSettings::resolve(Some(1024 * 1024), Some(16 * 1024 * 1024))
            .assured("explicit sizes validate");
        assert_eq!(settings.stack_bytes(), 1024 * 1024);
        assert_eq!(settings.memory_reservation_bytes(), 16 * 1024 * 1024);
        assert!(!settings.parallel_compilation());
        assert!(WasmEngineSettings::resolve(Some(0), None).is_err());
    }
}
