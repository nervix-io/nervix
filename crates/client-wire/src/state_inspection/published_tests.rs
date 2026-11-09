//! Lossless published-key and window-metadata sections through the production session codec.
//!
//! Layer: test harness.
//! - **Owns.** Complete typed section equality, nullable headers and exact large counts/timestamps.
//! - **Depends on.** Vocabulary metadata fields, Row encoding and the production reply codec.
//! - **Must not know.** Runtime window objects, snapshots or owner coordination.

use nervix_models::ProcessorStateSection;

use super::*;

fn published_section(
    kind: ProcessorStateSection,
    value: u64,
    time: i64,
    absent: u8,
) -> StateInspectionSection {
    let limits = SessionLimits::DEFAULT;
    let (fields, mut encoder) = (
        kind.metadata_fields(),
        StateRowsEncoder::unbranched(&limits),
    );
    let fields = match kind {
        ProcessorStateSection::Header => {
            encoder
                .push_row(|row| {
                    row.push_u64(value)?;
                    row.push_u64(2)?;
                    if absent & 1 != 0 {
                        row.push_null()?;
                    } else {
                        row.push_u64(value)?;
                    }
                    row.push_u64(value)?;
                    if absent & 2 != 0 {
                        row.push_null()?;
                    } else {
                        row.push_u64(value)?;
                    }
                    if absent & 4 != 0 {
                        row.push_null()?;
                    } else {
                        row.push_datetime(Timestamp::from_unix_nanos(time))?;
                    }
                    row.push_u64(value)
                })
                .assured("one fixed header fits");
            fields
        }
        ProcessorStateSection::Accumulators => {
            encoder
                .push_row(|row| {
                    row.push_u32(u32::MAX)?;
                    row.push_string("histogram")?;
                    row.push_u64(value)
                })
                .assured("one structure fits");
            fields
        }
        ProcessorStateSection::Keys => {
            encoder
                .push_row(|row| {
                    row.push_u64(value)?;
                    row.push_redacted()?;
                    row.push_datetime(Timestamp::from_unix_nanos(time))
                })
                .assured("one typed key fits");
            vec![
                field("key_1", ParseAsType::U64, false),
                field("key_2", ParseAsType::String, true),
                field("seen_at", ParseAsType::Datetime, false),
            ]
        }
        _ => panic!("the fixture names a published key or metadata section"),
    };
    StateInspectionSection {
        name: kind.as_ref().to_string(),
        schema: RowSchema {
            fields,
            branch: None,
        },
        frames: vec![encoder.finish(&limits).assured("accepted rows finish")],
        omitted_rows: 0,
        watermarks: None,
    }
}

#[test]
fn bolero_published_processor_sections_round_trip() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary =
                nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Vocabulary);
            let value = arbitrary.positive_u64().get();
            let time = i64::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()));
            let absent = arbitrary.entropy().byte();
            let window = arbitrary.entropy().flag();
            let mut state = inspection(value);
            let kind = if window {
                ModelKind::WindowProcessor
            } else {
                ModelKind::Deduplicator
            };
            state.entity = DomainNodeRef::node_in(
                name("sales"),
                kind,
                name::<nervix_models::ModelName>("held"),
            );
            let StateInspectionResult::Page(page) = &mut state.result else {
                panic!("the fixture is a page");
            };
            let unit = &mut page.units[0];
            unit.sections = if window {
                vec![
                    published_section(ProcessorStateSection::Header, value, time, absent),
                    published_section(ProcessorStateSection::Accumulators, value, time, absent),
                ]
            } else {
                vec![published_section(
                    ProcessorStateSection::Keys,
                    value,
                    time,
                    absent,
                )]
            };
            let original = outcome(state);
            let decoded = round_trip_reply(&Reply {
                request_id: request(1),
                body: ReplyBody::Command(Box::new(original.clone())),
            });
            assert_eq!(decoded.body, ReplyBody::Command(Box::new(original)));
        });
}
