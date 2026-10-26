//! The readings the simulation plans, the schemas the example graph's endpoints carry them in, and
//! the Arrow batches they travel as.
//!
//! - **Owns.** The identity and content of a reading, which stamp it carries, the expected fields
//!   of the example's ingestor input and its two emitter outputs, and building and reading their
//!   Arrow batches.
//! - **Depends on.** Arrow arrays and the vocabulary's schema fields and timestamps.
//! - **Must not know.** Sessions, clocks or files.

use arrow_array::{
    Array as _, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::{ArrowError, Schema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{FieldName, ParseAsType, SchemaField, Timestamp};
use nervix_primitives::sync::StdArc;
use serde::{Deserialize, Serialize};

/// The time zone the Arrow representation of a `DATETIME` carries.
const UTC: &str = "+00:00";

/// Which time a reading is stamped with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Stamp {
    /// The tick center the clock reached: inside the admission window.
    Center,
    /// Deliberately before the oldest tick center the admission window retains, less the skew.
    BeforeWindow,
}

/// One simulated sensor reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reading {
    /// Stable across replays: the generation, tick, sensor and position it was planned for.
    pub(crate) reading_id: String,
    /// The START generation whose clock the reading was planned against.
    pub(crate) generation: u64,
    /// The tick center's position since the generation's logical origin.
    pub(crate) tick: u64,
    pub(crate) sensor: String,
    pub(crate) occurred_at: Timestamp,
    pub(crate) value: i64,
    pub(crate) stamp: Stamp,
}

/// Where in the simulation one reading is planned.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadingSlot {
    pub(crate) generation: u64,
    pub(crate) tick: u64,
    pub(crate) sensor: u32,
    pub(crate) position: u32,
}

impl Reading {
    pub(crate) fn planned(slot: ReadingSlot, occurred_at: Timestamp, stamp: Stamp) -> Self {
        let ReadingSlot {
            generation,
            tick,
            sensor,
            position,
        } = slot;
        Self {
            reading_id: format!("g{generation}-t{tick}-s{sensor}-{position}"),
            generation,
            tick,
            sensor: format!("sensor-{sensor}"),
            occurred_at,
            value: simulated_value(slot),
            stamp,
        }
    }
}

/// The value a sensor reports: a deterministic mix of where the reading is planned, so a replay
/// and the other driver report the same value for the same reading.
fn simulated_value(slot: ReadingSlot) -> i64 {
    let tick = u128::from(slot.tick)
        .checked_mul(31)
        .assured("a u64 times 31 fits u128");
    let sensor = u128::from(slot.sensor)
        .checked_mul(17)
        .assured("a u32 times 17 fits u128");
    let position = u128::from(slot.position)
        .checked_mul(7)
        .assured("a u32 times 7 fits u128");
    let partial = tick
        .checked_add(sensor)
        .assured("two products of u64 or u32 values and small factors fit u128");
    let mixed = partial
        .checked_add(position)
        .assured("three products of u64 or u32 values and small factors fit u128");
    i64::try_from(mixed % 1000).assured("a remainder of 1000 fits i64")
}

fn field(name: &str, ty: ParseAsType) -> SchemaField {
    SchemaField {
        name: FieldName::parse(name).assured("the example's field names are valid names"),
        ty,
        optional: false,
        sensitive: false,
    }
}

/// The input schema of the example's ingestors, `reading`.
pub(crate) fn reading_fields() -> Vec<SchemaField> {
    vec![
        field("reading_id", ParseAsType::String),
        field("sensor", ParseAsType::String),
        field("tick", ParseAsType::U64),
        field("occurred_at", ParseAsType::Datetime),
        field("value", ParseAsType::I64),
    ]
}

/// The output schema of the example's reading emitter, `observed_reading`.
pub(crate) fn observed_fields() -> Vec<SchemaField> {
    vec![
        field("reading_id", ParseAsType::String),
        field("sensor", ParseAsType::String),
        field("tick", ParseAsType::U64),
        field("occurred_at", ParseAsType::Datetime),
        field("admitted_at", ParseAsType::Datetime),
        field("timestamp_source", ParseAsType::String),
        field("value", ParseAsType::I64),
    ]
}

/// The output schema of the example's rejection emitter, `rejected_reading`.
pub(crate) fn rejected_fields() -> Vec<SchemaField> {
    vec![
        field("reading_id", ParseAsType::String),
        field("occurred_at", ParseAsType::Datetime),
        field("error_code", ParseAsType::String),
        field("error_message", ParseAsType::String),
    ]
}

/// The readings of one batch as the producer's schema, column by column.
pub(crate) fn record_batch(
    schema: Schema,
    readings: &[Reading],
) -> Result<RecordBatch, ArrowError> {
    let identities =
        StringArray::from_iter_values(readings.iter().map(|reading| &reading.reading_id));
    let sensors = StringArray::from_iter_values(readings.iter().map(|reading| &reading.sensor));
    let ticks = UInt64Array::from_iter_values(readings.iter().map(|reading| reading.tick));
    let occurred = TimestampNanosecondArray::from_iter_values(
        readings
            .iter()
            .map(|reading| reading.occurred_at.unix_nanos()),
    )
    .with_timezone(UTC);
    let values = Int64Array::from_iter_values(readings.iter().map(|reading| reading.value));
    RecordBatch::try_new(
        StdArc::new(schema),
        vec![
            StdArc::new(identities),
            StdArc::new(sensors),
            StdArc::new(ticks),
            StdArc::new(occurred),
            StdArc::new(values),
        ],
    )
}

/// One constructed reading the output emitter delivered.
#[derive(Debug, Clone)]
pub(crate) struct ObservedReading {
    pub(crate) reading_id: String,
    pub(crate) sensor: String,
    pub(crate) tick: u64,
    pub(crate) occurred_at: Timestamp,
    pub(crate) admitted_at: Timestamp,
    pub(crate) timestamp_source: String,
    pub(crate) value: i64,
}

/// One rejection notice the rejection emitter delivered.
#[derive(Debug, Clone)]
pub(crate) struct RejectionNotice {
    pub(crate) reading_id: String,
    pub(crate) occurred_at: Timestamp,
    pub(crate) error_code: String,
    pub(crate) error_message: String,
}

/// A column of a delivered batch. The client checked the batch against the fields the consumer
/// opened with, so every column the example's schemas declare is present with its type.
fn column<'batch, T: 'static>(batch: &'batch RecordBatch, name: &str) -> &'batch T {
    let array = batch
        .column_by_name(name)
        .verified("the client checked the delivery against the fields the consumer opened with");
    array
        .as_any()
        .downcast_ref::<T>()
        .verified("the client checked every column's type against the opened fields")
}

/// The readings of one delivery of the output emitter.
pub(crate) fn observed_readings(batch: &RecordBatch) -> Vec<ObservedReading> {
    let identities = column::<StringArray>(batch, "reading_id");
    let sensors = column::<StringArray>(batch, "sensor");
    let ticks = column::<UInt64Array>(batch, "tick");
    let occurred = column::<TimestampNanosecondArray>(batch, "occurred_at");
    let admitted = column::<TimestampNanosecondArray>(batch, "admitted_at");
    let sources = column::<StringArray>(batch, "timestamp_source");
    let values = column::<Int64Array>(batch, "value");
    let mut readings = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        readings.push(ObservedReading {
            reading_id: identities.value(row).to_string(),
            sensor: sensors.value(row).to_string(),
            tick: ticks.value(row),
            occurred_at: Timestamp::from_unix_nanos(occurred.value(row)),
            admitted_at: Timestamp::from_unix_nanos(admitted.value(row)),
            timestamp_source: sources.value(row).to_string(),
            value: values.value(row),
        });
    }
    readings
}

/// The notices of one delivery of the rejection emitter.
pub(crate) fn rejection_notices(batch: &RecordBatch) -> Vec<RejectionNotice> {
    let identities = column::<StringArray>(batch, "reading_id");
    let occurred = column::<TimestampNanosecondArray>(batch, "occurred_at");
    let codes = column::<StringArray>(batch, "error_code");
    let messages = column::<StringArray>(batch, "error_message");
    let mut notices = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        notices.push(RejectionNotice {
            reading_id: identities.value(row).to_string(),
            occurred_at: Timestamp::from_unix_nanos(occurred.value(row)),
            error_code: codes.value(row).to_string(),
            error_message: messages.value(row).to_string(),
        });
    }
    notices
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(tick: u64, sensor: u32, position: u32) -> ReadingSlot {
        ReadingSlot {
            generation: 2,
            tick,
            sensor,
            position,
        }
    }

    #[test]
    fn a_reading_is_named_and_valued_by_where_it_is_planned() {
        let occurred_at = Timestamp::from_unix_nanos(1_000_000_000);
        let reading = Reading::planned(slot(12, 1, 3), occurred_at, Stamp::Center);
        assert_eq!(reading.reading_id, "g2-t12-s1-3");
        assert_eq!(reading.sensor, "sensor-1");
        // 12 × 31 + 1 × 17 + 3 × 7
        assert_eq!(reading.value, 410);
        let replayed = Reading::planned(slot(12, 1, 3), occurred_at, Stamp::Center);
        assert_eq!(replayed, reading);
        let largest = Reading::planned(
            slot(u64::MAX, u32::MAX, u32::MAX),
            occurred_at,
            Stamp::Center,
        );
        assert!((0..1000).contains(&largest.value));
    }

    #[test]
    fn readings_round_trip_through_the_producer_schema() {
        let readings = vec![
            Reading::planned(
                slot(1, 0, 0),
                Timestamp::from_unix_nanos(100),
                Stamp::Center,
            ),
            Reading::planned(
                slot(1, 1, 0),
                Timestamp::from_unix_nanos(-5),
                Stamp::BeforeWindow,
            ),
        ];
        let schema = SchemaField::arrow_schema(&reading_fields());
        let batch = record_batch(schema, &readings).assured("the columns match the schema");
        assert_eq!(batch.num_rows(), 2);
        let identities = column::<StringArray>(&batch, "reading_id");
        let occurred = column::<TimestampNanosecondArray>(&batch, "occurred_at");
        assert_eq!(identities.value(1), "g2-t1-s1-0");
        assert_eq!(occurred.value(1), -5);
    }

    #[test]
    fn delivered_batches_read_back_as_readings_and_notices() {
        let observed_schema = SchemaField::arrow_schema(&observed_fields());
        let observed = RecordBatch::try_new(
            StdArc::new(observed_schema),
            vec![
                StdArc::new(StringArray::from(vec!["g1-t3-s0-0"])),
                StdArc::new(StringArray::from(vec!["sensor-0"])),
                StdArc::new(UInt64Array::from(vec![3])),
                StdArc::new(TimestampNanosecondArray::from(vec![300]).with_timezone(UTC)),
                StdArc::new(TimestampNanosecondArray::from(vec![310]).with_timezone(UTC)),
                StdArc::new(StringArray::from(vec!["at"])),
                StdArc::new(Int64Array::from(vec![93])),
            ],
        )
        .assured("the columns match the schema");
        let readings = observed_readings(&observed);
        assert_eq!(readings.len(), 1);
        assert_eq!(readings[0].reading_id, "g1-t3-s0-0");
        assert_eq!(readings[0].admitted_at, Timestamp::from_unix_nanos(310));
        assert_eq!(readings[0].timestamp_source, "at");

        let rejected_schema = SchemaField::arrow_schema(&rejected_fields());
        let rejected = RecordBatch::try_new(
            StdArc::new(rejected_schema),
            vec![
                StdArc::new(StringArray::from(vec!["g1-t4-s0-0"])),
                StdArc::new(TimestampNanosecondArray::from(vec![-1]).with_timezone(UTC)),
                StdArc::new(StringArray::from(vec!["validation"])),
                StdArc::new(StringArray::from(vec!["outside the window"])),
            ],
        )
        .assured("the columns match the schema");
        let notices = rejection_notices(&rejected);
        assert_eq!(notices[0].error_code, "validation");
        assert_eq!(notices[0].occurred_at, Timestamp::from_unix_nanos(-1));
    }
}
