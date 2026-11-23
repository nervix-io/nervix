//! Node-local expression engine options.
//!
//! Layer: edges.
//! - **Owns.** Engine arguments and their conversion into validated capabilities.
//! - **Depends on.** Engine definitions and the edge's byte representation.
//! - **Must not know.** Compilation, execution or cache implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns engine capability arguments")
)]

use arch_into::ArchInto as _;
use nervix_models::SettingDefinition as _;
use nervix_roto::{UdfSettings, UdfSettingsError, UdfWatchdog};
use nervix_vm::{
    VmDynamicPatternCacheCapacity, VmInlineRowLimit, VmPatternSetsPerBatch,
    VmRegexSearchCacheBytes, VmSettingOverrides, VmSettings, VmSettingsError,
};
use nervix_wasm::{
    WasmEngineSettingOverrides, WasmEngineSettings, WasmEngineSettingsError,
    WasmMemoryGrowthReservationBytes, WasmMemoryGuardBytes, WasmMemoryReservationBytes,
    WasmStackBytes,
};

use super::settings::CommandLine;
use crate::application::cli_values::{HumanBytes, HumanDuration};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Expression engine")]
pub struct EngineArgs {
    #[arg(long = WasmStackBytes::NAME, env = "NERVIX_WASM_STACK_BYTES", help = super::settings::capability_help::<WasmStackBytes>("Stack a guest's own frames may use in one call, which runs on this stack plus a fixed 1.5 MiB host allowance", HumanBytes(ubyte::ByteUnit::Byte(WasmStackBytes::default_value(&()).arch_into()))))]
    pub wasm_stack_bytes: Option<HumanBytes>,
    #[arg(long = WasmMemoryReservationBytes::NAME, env = "NERVIX_WASM_MEMORY_RESERVATION_BYTES", help = super::settings::capability_help::<WasmMemoryReservationBytes>("Virtual address reservation per guest memory", HumanBytes(ubyte::ByteUnit::Byte(WasmMemoryReservationBytes::default_value(&())))))]
    pub wasm_memory_reservation_bytes: Option<HumanBytes>,
    #[arg(long = WasmMemoryGuardBytes::NAME, env = "NERVIX_WASM_MEMORY_GUARD_BYTES", help = super::settings::capability_help::<WasmMemoryGuardBytes>("Unmapped guard on each side of every guest memory", HumanBytes(ubyte::ByteUnit::Byte(WasmMemoryGuardBytes::default_value(&())))))]
    pub wasm_memory_guard_bytes: Option<HumanBytes>,
    #[arg(long = WasmMemoryGrowthReservationBytes::NAME, env = "NERVIX_WASM_MEMORY_GROWTH_RESERVATION_BYTES", help = super::settings::capability_help::<WasmMemoryGrowthReservationBytes>("Address space a guest memory reserves to grow into past its reservation", HumanBytes(ubyte::ByteUnit::Byte(WasmMemoryGrowthReservationBytes::default_value(&())))))]
    pub wasm_memory_growth_reservation_bytes: Option<HumanBytes>,
    #[arg(long = VmInlineRowLimit::NAME, env = "NERVIX_VM_INLINE_ROW_LIMIT", help = format!("Largest batch executed inline without an extension call; zero admits every nonempty batch. Default: {}", VmInlineRowLimit::DEFAULT))]
    pub vm_inline_row_limit: Option<usize>,
    #[arg(long = VmRegexSearchCacheBytes::NAME, env = "NERVIX_VM_REGEX_SEARCH_CACHE_BYTES", help = format!("Lazy-DFA search cache bytes per compiled regex. Default: {}", VmRegexSearchCacheBytes::DEFAULT))]
    pub vm_regex_search_cache_bytes: Option<HumanBytes>,
    #[arg(long = VmDynamicPatternCacheCapacity::NAME, env = "NERVIX_VM_DYNAMIC_PATTERN_CACHE_CAPACITY", help = format!("Field-supplied regex patterns retained per call. Default: {}", VmDynamicPatternCacheCapacity::DEFAULT))]
    pub vm_dynamic_pattern_cache_capacity: Option<usize>,
    #[arg(long = VmPatternSetsPerBatch::NAME, env = "NERVIX_VM_PATTERN_SETS_PER_BATCH", help = format!("Dynamic contains_any sets retained per batch. Default: {}", VmPatternSetsPerBatch::DEFAULT))]
    pub vm_pattern_sets_per_batch: Option<usize>,
    #[arg(long = UdfWatchdog::NAME, env = "NERVIX_UDF_WATCHDOG", help = format!("How long one call of a compiled Roto UDF may run before its batch fails. Default: {}", HumanDuration(UdfWatchdog::default_value(&()))))]
    pub udf_watchdog: Option<HumanDuration>,
}

impl EngineArgs {
    pub(super) fn wasm_settings(
        &self,
    ) -> error_stack::Result<WasmEngineSettings, WasmEngineSettingsError> {
        WasmEngineSettings::resolve(WasmEngineSettingOverrides {
            stack_bytes: self.wasm_stack_bytes.map(|value| value.0.as_u64()),
            memory_reservation_bytes: self
                .wasm_memory_reservation_bytes
                .map(|value| value.0.as_u64()),
            memory_guard_bytes: self.wasm_memory_guard_bytes.map(|value| value.0.as_u64()),
            memory_growth_reservation_bytes: self
                .wasm_memory_growth_reservation_bytes
                .map(|value| value.0.as_u64()),
        })
    }
    pub(super) fn settings(&self) -> error_stack::Result<VmSettings, VmSettingsError> {
        VmSettings::resolve(VmSettingOverrides {
            inline_row_limit: self.vm_inline_row_limit,
            regex_search_cache_bytes: self
                .vm_regex_search_cache_bytes
                .map(|value| value.0.as_u64()),
            dynamic_pattern_cache_capacity: self.vm_dynamic_pattern_cache_capacity,
            pattern_sets_per_batch: self.vm_pattern_sets_per_batch,
        })
    }
    pub(super) fn udf_settings(&self) -> error_stack::Result<UdfSettings, UdfSettingsError> {
        UdfSettings::resolve(self.udf_watchdog.map(|value| value.0))
    }
    pub(super) fn from_settings(
        settings: &VmSettings,
        wasm: &WasmEngineSettings,
        udf: &UdfSettings,
    ) -> Self {
        Self {
            wasm_stack_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                wasm.stack_bytes().arch_into(),
            ))),
            wasm_memory_reservation_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                wasm.memory_reservation_bytes(),
            ))),
            wasm_memory_guard_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                wasm.memory_guard_bytes(),
            ))),
            wasm_memory_growth_reservation_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                wasm.memory_growth_reservation_bytes(),
            ))),
            vm_inline_row_limit: Some(settings.inline_row_limit()),
            vm_regex_search_cache_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                settings.regex_search_cache_bytes().arch_into(),
            ))),
            vm_dynamic_pattern_cache_capacity: Some(settings.dynamic_pattern_cache_capacity()),
            vm_pattern_sets_per_batch: Some(settings.pattern_sets_per_batch().get()),
            udf_watchdog: Some(HumanDuration(udf.watchdog())),
        }
    }
    pub(super) fn render(&self, command: &mut CommandLine) {
        command.optional(WasmStackBytes::NAME, self.wasm_stack_bytes);
        command.optional(
            WasmMemoryReservationBytes::NAME,
            self.wasm_memory_reservation_bytes,
        );
        command.optional(WasmMemoryGuardBytes::NAME, self.wasm_memory_guard_bytes);
        command.optional(
            WasmMemoryGrowthReservationBytes::NAME,
            self.wasm_memory_growth_reservation_bytes,
        );
        command.optional(VmInlineRowLimit::NAME, self.vm_inline_row_limit);
        command.optional(
            VmRegexSearchCacheBytes::NAME,
            self.vm_regex_search_cache_bytes,
        );
        command.optional(
            VmDynamicPatternCacheCapacity::NAME,
            self.vm_dynamic_pattern_cache_capacity,
        );
        command.optional(VmPatternSetsPerBatch::NAME, self.vm_pattern_sets_per_batch);
        command.optional(UdfWatchdog::NAME, self.udf_watchdog);
    }
}
