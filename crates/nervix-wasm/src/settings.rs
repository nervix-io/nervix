//! Local Wasmtime construction capabilities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Explicit stack, address reservation, guard and growth-reserve settings, the call
//!   stack derived from the stack setting, and the bounded compilation policy.
//! - **Depends on.** Vocabulary setting classes and scalar sizes.
//! - **Must not know.** Graphs, guest state, command-line parsing or node lifecycle.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns WASM engine capabilities")
)]

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};

/// The stack a guest call keeps for the host functions the guest calls, beside the stack its own
/// frames may use. Wasmtime runs every guest call on a stack of its own and refuses a guest stack
/// that leaves the host nothing, so the call stack is the guest's stack plus this allowance.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "Wasmtime's 2 MiB call stack less its 512 KiB default guest stack, the headroom \
                  every host function a guest calls has run with"
    )
)]
pub const WASM_HOST_STACK_BYTES: usize = 1_572_864;

#[derive(Debug, thiserror::Error)]
pub enum WasmEngineSettingsError {
    #[error("wasm-stack-bytes must be positive and addressable on this host: {bytes}")]
    Stack { bytes: u64 },
    #[error(
        "wasm-stack-bytes ({bytes}) and the {host_bytes} bytes a guest call keeps for host \
         functions must fit one addressable call stack on this host"
    )]
    CallStack { bytes: usize, host_bytes: usize },
    #[error(
        "a guest memory's address range, wasm-memory-guard-bytes ({guard}) on each side of the \
         larger of wasm-memory-reservation-bytes ({reservation}) and \
         wasm-memory-growth-reservation-bytes ({growth}), must be addressable on this host"
    )]
    AddressSpace {
        guard: u64,
        reservation: u64,
        growth: u64,
    },
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
        if value.checked_add(WASM_HOST_STACK_BYTES).is_none() {
            return Err(Report::new(WasmEngineSettingsError::CallStack {
                bytes: *value,
                host_bytes: WASM_HOST_STACK_BYTES,
            }));
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

/// The unmapped guard on each side of every guest memory. Compiled guest code skips the bounds
/// check of an access whose static offset falls inside it, because such an access traps in the
/// guard instead of reaching other memory.
pub struct WasmMemoryGuardBytes;
impl SettingDefinition for WasmMemoryGuardBytes {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = WasmEngineSettingsError;
    const NAME: &'static str = "wasm-memory-guard-bytes";
    const DERIVATION: Option<&'static str> =
        Some("32 MiB on a 64-bit host; 64 KiB on a 32-bit host");
    fn default_value(_: &()) -> u64 {
        if usize::BITS == 64 {
            32 << 20
        } else {
            64 << 10
        }
    }
    fn validate(_: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}

/// The extra address space a guest memory reserves to grow into when it is larger than its
/// reservation, or moves to grow past it.
pub struct WasmMemoryGrowthReservationBytes;
impl SettingDefinition for WasmMemoryGrowthReservationBytes {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = WasmEngineSettingsError;
    const NAME: &'static str = "wasm-memory-growth-reservation-bytes";
    const DERIVATION: Option<&'static str> = Some("2 GiB on a 64-bit host; 1 MiB on a 32-bit host");
    fn default_value(_: &()) -> u64 {
        if usize::BITS == 64 { 2 << 30 } else { 1 << 20 }
    }
    fn validate(_: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}

/// The engine values an operator gave, each resolved through its definition's default when absent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WasmEngineSettingOverrides {
    pub stack_bytes: Option<u64>,
    pub memory_reservation_bytes: Option<u64>,
    pub memory_guard_bytes: Option<u64>,
    pub memory_growth_reservation_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmEngineSettings {
    stack_bytes: Setting<WasmStackBytes>,
    memory_reservation_bytes: Setting<WasmMemoryReservationBytes>,
    memory_guard_bytes: Setting<WasmMemoryGuardBytes>,
    memory_growth_reservation_bytes: Setting<WasmMemoryGrowthReservationBytes>,
}

impl WasmEngineSettings {
    pub fn resolve(
        overrides: WasmEngineSettingOverrides,
    ) -> Result<Self, Report<WasmEngineSettingsError>> {
        let stack_bytes = match overrides.stack_bytes {
            Some(bytes) => Some(usize::try_from(bytes).map_err(|source| {
                Report::new(source).change_context(WasmEngineSettingsError::Stack { bytes })
            })?),
            None => None,
        };
        let settings = Self {
            stack_bytes: Setting::resolve(stack_bytes, &())?,
            memory_reservation_bytes: Setting::resolve(overrides.memory_reservation_bytes, &())?,
            memory_guard_bytes: Setting::resolve(overrides.memory_guard_bytes, &())?,
            memory_growth_reservation_bytes: Setting::resolve(
                overrides.memory_growth_reservation_bytes,
                &(),
            )?,
        };
        settings.validate_memory_address_range()?;
        Ok(settings)
    }

    /// The stack a guest's own frames may use in one call.
    pub fn stack_bytes(self) -> usize {
        *self.stack_bytes.value()
    }

    /// The stack each guest call runs on: the guest's stack and the host allowance beside it.
    pub fn call_stack_bytes(self) -> usize {
        self.stack_bytes()
            .checked_add(WASM_HOST_STACK_BYTES)
            .verified("WasmStackBytes::validate admits only stacks the host allowance fits beside")
    }

    pub fn memory_reservation_bytes(self) -> u64 {
        *self.memory_reservation_bytes.value()
    }

    pub fn memory_guard_bytes(self) -> u64 {
        *self.memory_guard_bytes.value()
    }

    pub fn memory_growth_reservation_bytes(self) -> u64 {
        *self.memory_growth_reservation_bytes.value()
    }

    /// A guard sits before every guest memory as well as after it, as it always has. That is
    /// Wasmtime's own default, set explicitly so that a library upgrade cannot move it.
    pub const fn guard_before_memory(self) -> bool {
        true
    }

    /// Compilation already occupies the node's bounded bulk workers; nested fan-out is disabled.
    pub const fn parallel_compilation(self) -> bool {
        false
    }

    /// One guest memory's largest address range, a guard on each side of the larger of its
    /// reservation and its growth reserve, must fit this host's address space.
    fn validate_memory_address_range(self) -> Result<(), Report<WasmEngineSettingsError>> {
        let guard = self.memory_guard_bytes();
        let reservation = self.memory_reservation_bytes();
        let growth = self.memory_growth_reservation_bytes();
        let reserved = std::cmp::max(reservation, growth);
        let range = guard
            .checked_mul(2)
            .and_then(|guards| guards.checked_add(reserved));
        let addressable = match range {
            Some(range) => usize::try_from(range).is_ok(),
            None => false,
        };
        if addressable {
            return Ok(());
        }
        Err(Report::new(WasmEngineSettingsError::AddressSpace {
            guard,
            reservation,
            growth,
        }))
    }
}

impl Default for WasmEngineSettings {
    fn default() -> Self {
        Self::resolve(WasmEngineSettingOverrides::default())
            .assured("WASM engine defaults validate")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_construction_keeps_compilation_on_the_bulk_workers() {
        let settings = WasmEngineSettings::resolve(WasmEngineSettingOverrides {
            stack_bytes: Some(1024 * 1024),
            memory_reservation_bytes: Some(16 * 1024 * 1024),
            memory_guard_bytes: Some(64 * 1024),
            memory_growth_reservation_bytes: Some(1024 * 1024),
        })
        .assured("explicit sizes validate");
        assert_eq!(settings.stack_bytes(), 1024 * 1024);
        assert_eq!(settings.memory_reservation_bytes(), 16 * 1024 * 1024);
        assert_eq!(settings.memory_guard_bytes(), 64 * 1024);
        assert_eq!(settings.memory_growth_reservation_bytes(), 1024 * 1024);
        assert!(settings.guard_before_memory());
        assert!(!settings.parallel_compilation());
        assert!(
            WasmEngineSettings::resolve(WasmEngineSettingOverrides {
                stack_bytes: Some(0),
                ..WasmEngineSettingOverrides::default()
            })
            .is_err()
        );
    }

    /// The defaults are the values Wasmtime chose before Nervix set them, so a node that sets
    /// nothing runs its guests exactly as it did.
    #[test]
    fn the_defaults_keep_wasmtimes_previous_engine() {
        let settings = WasmEngineSettings::default();
        assert_eq!(settings.stack_bytes(), 512 * 1024);
        assert_eq!(settings.call_stack_bytes(), 2 * 1024 * 1024);
        if usize::BITS == 64 {
            assert_eq!(settings.memory_reservation_bytes(), 4 << 30);
            assert_eq!(settings.memory_guard_bytes(), 32 << 20);
            assert_eq!(settings.memory_growth_reservation_bytes(), 2 << 30);
        }
    }

    #[test]
    fn the_call_stack_holds_the_guest_stack_and_the_host_allowance() {
        let settings = WasmEngineSettings::resolve(WasmEngineSettingOverrides {
            stack_bytes: Some(4 * 1024 * 1024),
            ..WasmEngineSettingOverrides::default()
        })
        .assured("a 4 MiB stack is valid");
        assert_eq!(
            settings.call_stack_bytes(),
            4 * 1024 * 1024 + WASM_HOST_STACK_BYTES
        );
    }

    #[test]
    fn a_stack_the_host_allowance_cannot_fit_beside_is_refused() {
        let stack = usize::MAX - WASM_HOST_STACK_BYTES + 1;
        let refusal = WasmEngineSettings::resolve(WasmEngineSettingOverrides {
            stack_bytes: Some(u64::try_from(stack).assured("usize fits u64 on supported hosts")),
            ..WasmEngineSettingOverrides::default()
        })
        .expect_err("the call stack would not be addressable");
        assert!(
            matches!(
                refusal.current_context(),
                WasmEngineSettingsError::CallStack {
                    bytes,
                    host_bytes: WASM_HOST_STACK_BYTES,
                } if *bytes == stack
            ),
            "unexpected refusal: {refusal:?}"
        );
        assert_eq!(
            refusal.current_context().to_string(),
            format!(
                "wasm-stack-bytes ({stack}) and the {WASM_HOST_STACK_BYTES} bytes a guest call \
                 keeps for host functions must fit one addressable call stack on this host"
            )
        );
    }

    #[test]
    fn a_memory_address_range_past_the_host_is_refused_with_every_value_it_names() {
        let refusal = WasmEngineSettings::resolve(WasmEngineSettingOverrides {
            memory_reservation_bytes: Some(16 << 20),
            memory_guard_bytes: Some(u64::MAX / 2 + 1),
            memory_growth_reservation_bytes: Some(1 << 20),
            ..WasmEngineSettingOverrides::default()
        })
        .expect_err("two such guards overflow any address space");
        assert!(
            matches!(
                refusal.current_context(),
                WasmEngineSettingsError::AddressSpace {
                    guard,
                    reservation: 16_777_216,
                    growth: 1_048_576,
                } if *guard == u64::MAX / 2 + 1
            ),
            "unexpected refusal: {refusal:?}"
        );
    }
}
