//! Bounded choices shared by the window checkpoint representation properties.
//!
//! Layer: harness.
//! - **Owns.** Valid checkpoint limits, row and removal counts, and typed concrete branch keys.
//! - **Depends on.** Runtime branch values, vocabulary timestamps and the execution configuration.
//! - **Must not know.** Graph placement, checkpoint publication or node lifecycle policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "generates test-only window checkpoint inputs")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::Entropy;
use nervix_execution::{ExecutionConfig, Executor, OperationLimits};
use nervix_models::FieldName;
use ordered_float::OrderedFloat;
use ubyte::ByteUnit;

use super::BranchKey;
use crate::runtime_schema::RuntimeValue;

/// One bounded checkpoint domain, including empty windows and split typed sections.
pub(super) struct WindowCheckpointCase {
    pub(super) branch: Option<BranchKey>,
    pub(super) rows: usize,
    pub(super) removals: usize,
    pub(super) column_bytes: usize,
    pub(super) section_bytes: u64,
    pub(super) record_bytes: u64,
}

impl WindowCheckpointCase {
    pub(super) const MAX_ROWS: u64 = 192;

    pub(super) fn generated(entropy: &mut Entropy<'_>) -> Self {
        // The compiled schemas' Arrow IPC framing needs more than 1 KiB before any rows.
        // Padded string rows leave room for framing while splitting independently of identities.
        let section_bytes = entropy.pick([4096, 8192, 16_384]);
        let record_bytes = entropy.pick([1024, 2048, 32_768]);
        let rows = entropy
            .count(usize::try_from(Self::MAX_ROWS).assured("192 generated rows fit every target"));
        let removals = entropy.count(96);
        let column_bytes = if entropy.flag() { 2049 } else { 0 };
        let branch = if entropy.flag() {
            Some(Self::generated_branch(entropy))
        } else {
            None
        };
        Self {
            branch,
            rows,
            removals,
            column_bytes,
            section_bytes,
            record_bytes,
        }
    }

    /// Keep each scalar identity within the narrowest record while varying float bits and
    /// whole-minute datetime offsets. Every concrete key carries several distinct field types.
    fn generated_branch(entropy: &mut Entropy<'_>) -> BranchKey {
        let float_bits = u32::try_from(entropy.up_to(u64::from(u32::MAX)))
            .verified("the generated bits fit an F32");
        let float = f32::from_bits(float_bits);
        let float = if float.is_finite() { float } else { -0.0 };
        let double = f64::from_bits(entropy.any_u64());
        let double = if double.is_finite() {
            double
        } else {
            f64::MIN_POSITIVE
        };
        let instant = chrono::DateTime::from_timestamp_nanos(entropy.any_i64());
        let offset = chrono::FixedOffset::east_opt(entropy.pick([-43_200, 0, 20_700, 50_400]))
            .assured("whole-minute offsets within a day are valid");
        let values = [
            ("float", RuntimeValue::F32(OrderedFloat(float))),
            ("double", RuntimeValue::F64(OrderedFloat(double))),
            ("at", RuntimeValue::Datetime(instant.with_timezone(&offset))),
            (
                "tenant",
                RuntimeValue::String(entropy.pick(["", "a", "é"]).into()),
            ),
            ("partition", RuntimeValue::I64(entropy.any_i64())),
            ("enabled", RuntimeValue::Bool(entropy.flag())),
        ];
        let fields = values.map(|(name, value)| {
            let name = FieldName::parse(name).assured("fixed test field names are valid");
            (name, value)
        });
        BranchKey::from_fields(fields).assured("six typed fields contain only finite floats")
    }

    pub(super) fn executor(&self) -> Executor {
        Executor::new(ExecutionConfig {
            limits: OperationLimits {
                snapshot_section_bytes: ByteUnit::Byte(self.section_bytes),
                snapshot_record_bytes: ByteUnit::Byte(self.record_bytes),
                ..OperationLimits::default()
            },
            ..ExecutionConfig::default()
        })
        .assured("bounded checkpoint limits fit the default execution budgets")
    }

    /// Archive sections may be kept whole before the native identity writer splits their rows.
    pub(super) fn archive_executor(&self) -> Executor {
        Executor::new(ExecutionConfig {
            limits: OperationLimits {
                snapshot_record_bytes: ByteUnit::Byte(self.record_bytes),
                ..OperationLimits::default()
            },
            ..ExecutionConfig::default()
        })
        .assured("bounded archive identities fit the default execution budgets")
    }
}
