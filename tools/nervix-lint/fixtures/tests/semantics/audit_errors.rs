//! Audit fixture: failures replaced, defaulted or tested without their value, and clamps spelled
//! as checked operations.
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "the audit calibration runs these examples once")
)]

use std::num::ParseIntError;

/// Each operation keeps only whether the parse succeeded.
pub fn dropped(text: &str) -> u32 {
    let parsed: Result<u32, ParseIntError> = text.parse();
    let first = parsed.clone().ok().unwrap_or(0);
    let second = parsed.clone().unwrap_or_default();
    #[cfg_attr(nervix_lint, expect(nervix::replaced_error, reason = "this one replacement calibrates the audit's separate review observation"))]
    let replaced = parsed.clone().map_err(|_| "unparsed");
    let replaced = match replaced {
        Ok(value) => value,
        Err(message) => u32::from(message.is_empty()),
    };
    let matched = match parsed.clone() {
        Ok(value) => value,
        Err(_) => 0,
    };
    if let Ok(value) = parsed.clone() {
        return value;
    }
    let Ok(value) = parsed else {
        return first + second + replaced + matched;
    };
    value
}

/// A failure handed to its arm or wrapped as a cause keeps its value.
pub fn kept(text: &str) -> Result<u32, ParseIntError> {
    let parsed: Result<u32, ParseIntError> = text.parse();
    let wrapped = parsed.clone().map_err(|error| error);
    match wrapped {
        Ok(value) => Ok(value),
        Err(error) => Err(error),
    }
}

/// A failure with no fields.
pub struct Missing;

/// Unit, zero-sized and uninhabited failures carry no information to drop.
pub fn empty(
    unit: Result<u8, ()>,
    missing: Result<u8, Missing>,
    never: Result<u8, std::convert::Infallible>,
) -> u8 {
    let first = unit.unwrap_or_default();
    let third = never.unwrap_or_default();
    match missing.ok() {
        Some(second) => second ^ first ^ third,
        None => first,
    }
}

/// Checked operations whose overflow falls back to a value instead of being decided.
pub fn clamped(remaining: u64, used: u64, total: usize) -> u64 {
    let left = remaining.checked_sub(used).unwrap_or(0);
    let narrowed = u64::try_from(total).unwrap_or(u64::MAX);
    left.min(narrowed)
}

/// A checked operation whose overflow is decided explicitly is not a clamp.
pub fn decided(remaining: u64, used: u64) -> Option<u64> {
    let Some(left) = remaining.checked_sub(used) else {
        return None;
    };
    Some(left)
}
