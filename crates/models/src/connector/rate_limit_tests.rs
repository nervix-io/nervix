//! Tests of the counts and periods a `RATE LIMIT` clause writes.
//!
//! Layer: test harness.
//! - **Owns.** How a written count, byte size and period read, and the refusals of text that
//!   writes none.
//! - **Depends on.** The rate-limit vocabulary alone.
//! - **Must not know.** Parsing, planning, the allowance engine or how a host holds a source back.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;

use super::*;

#[test]
fn a_count_keeps_its_spelling_and_counts_its_units() {
    for (text, units) in [("100", 100), ("16MiB", 16 * 1024 * 1024), ("1KiB", 1024)] {
        let quantity = RateQuantity::parse(text).assured("the text writes a positive count");
        assert_eq!(quantity.as_str(), text);
        let counted = quantity
            .units()
            .assured("a parsed quantity counts its units");
        assert_eq!(counted.get(), units, "{text}");
    }
}

#[test]
fn a_zero_count_is_refused() {
    let error = RateQuantity::parse("0").expect_err("a count must be positive");
    assert_eq!(error.current_context(), &RateQuantityError::Zero);
}

#[test]
fn unreadable_text_is_refused_with_the_parser_cause_beneath_it() {
    let error = RateQuantity::parse("ten").expect_err("`ten` is no count");
    assert_eq!(
        error.current_context(),
        &RateQuantityError::Unreadable {
            text: "ten".to_owned()
        }
    );
    assert!(
        error.downcast_ref::<RateQuantityCause>().is_some(),
        "the byte-size parser's refusal stays in the report: {error:?}"
    );
}

#[test]
fn a_period_must_be_a_duration_longer_than_zero() {
    let clause = |period: &str| RateLimit {
        count: RateQuantity::parse("1").assured("one is a count"),
        measure: None,
        period: period.to_owned(),
        burst: RateQuantity::parse("1").assured("one is a count"),
    };
    let period = clause("250ms")
        .period()
        .assured("250ms is a positive duration");
    assert_eq!(period, Duration::from_millis(250));
    let zero = clause("0s")
        .period()
        .expect_err("a zero period refills nothing");
    assert_eq!(zero.current_context(), &RatePeriodError::Zero);
    let unreadable = clause("soon").period().expect_err("`soon` is no duration");
    assert_eq!(unreadable.current_context(), &RatePeriodError::Unreadable);
}
