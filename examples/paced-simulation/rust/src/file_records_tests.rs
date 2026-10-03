//! The ledger's and the effect store's records read back from their JSON lines as the complete
//! values that were written: every field, every timestamp to the nanosecond, and text that JSON
//! has to escape.

use meticulous::ResultExt as _;
use nervix_models::Timestamp;

use crate::{
    effects::EffectRecord,
    ledger::{LedgerReading, LedgerRecord, OutcomeKind},
    readings::Stamp,
};

/// Text a record field carries: drawn from the input, with characters JSON escapes.
fn text(prefix: &str, value: u8) -> String {
    format!("{prefix}-{value}\"\\\n\t🚀{}", char::from(value))
}

fn read_back<T>(record: &T) -> T
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let line = serde_json::to_string(record).assured("every record serializes");
    assert!(!line.contains('\n'), "a record is one line: {line}");
    serde_json::from_str(&line).assured("a written record reads back")
}

#[test]
fn bolero_ledger_and_effect_records_read_back_whole() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(96)
        .for_each(|bytes: &[u8]| {
            let byte = |index: usize| match bytes.get(index) {
                Some(value) => *value,
                None => 0,
            };
            let word = |offset: usize| {
                u64::from_le_bytes(std::array::from_fn(|index| byte(offset + index)))
            };
            // Every instant a signed nanosecond count can name, before and after the epoch.
            let instant = |offset: usize| Timestamp::from_unix_nanos(word(offset).cast_signed());
            let stamp = if byte(0) & 1 == 0 {
                Stamp::Center
            } else {
                Stamp::BeforeWindow
            };
            let outcome = match byte(1) % 4 {
                0 => OutcomeKind::Completed,
                1 => OutcomeKind::NotAdmitted,
                2 => OutcomeKind::ProcessingFailed,
                _ => OutcomeKind::OutcomeUnknown,
            };
            let records = [
                LedgerRecord::Generation {
                    generation: word(2),
                },
                LedgerRecord::Reading(LedgerReading {
                    reading_id: text("g", byte(3)),
                    generation: word(10),
                    tick: word(18),
                    sensor: text("sensor", byte(4)),
                    occurred_at: instant(26),
                    value: word(34).cast_signed(),
                    ingestor: text("ingestor", byte(5)),
                    timestamps: text("at", byte(6)),
                    stamp,
                }),
                LedgerRecord::Outcome {
                    reading_ids: vec![text("first", byte(7)), text("second", byte(8))],
                    outcome,
                    cause: text("cause", byte(9)),
                },
            ];
            for record in &records {
                assert_eq!(&read_back(record), record);
            }
            let effects = [
                EffectRecord::Reading {
                    reading_id: text("g", byte(42)),
                    sensor: text("sensor", byte(43)),
                    tick: word(44),
                    occurred_at: instant(52),
                    admitted_at: instant(60),
                    timestamp_source: text("now", byte(68)),
                    value: word(69).cast_signed(),
                    generation: word(77),
                },
                EffectRecord::Rejection {
                    reading_id: text("g", byte(85)),
                    occurred_at: instant(86),
                    error_code: text("validation", byte(94)),
                    error_message: text("outside", byte(95)),
                    generation: word(88),
                },
            ];
            for effect in &effects {
                assert_eq!(&read_back(effect), effect);
            }
        });
}
