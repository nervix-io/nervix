//! Owner checks for the header model an entity declares on each side.
//!
//! Layer: test harness.
//! - **Owns.** Evidence that a header model refuses exactly the literal writes it would lose.
//! - **Depends on.** The header model and standard collections.
//! - **Must not know.** Any connector, its driver, or how a statement is parsed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns header model evidence")
)]

use std::collections::{BTreeMap, BTreeSet};

use strum::IntoEnumIterator as _;

use super::{
    HeaderLayout, HeaderModel, HeaderNames, HeaderValueType, HeaderValues, LiteralHeaderWrite,
    ReplacedHeaderWrite,
};
use crate::PropertyType;

fn writes<'a>(names: &[&'a str]) -> Vec<LiteralHeaderWrite<'a>> {
    let mut writes = Vec::new();
    for (index, name) in names.iter().enumerate() {
        writes.push(LiteralHeaderWrite {
            position: index + 1,
            name,
        });
    }
    writes
}

#[test]
fn only_the_layouts_that_keep_one_value_per_name_replace_a_repeated_name() {
    assert!(!HeaderLayout::OrderedRepeats.keeps_one_value_per_name());
    assert!(!HeaderLayout::RepeatsByName.keeps_one_value_per_name());
    assert!(HeaderLayout::LastWins.keeps_one_value_per_name());
    assert!(HeaderLayout::SortedByName.keeps_one_value_per_name());
}

#[test]
fn lowercase_names_record_ascii_lowercase_and_exact_names_record_the_name_as_written() {
    assert_eq!(HeaderNames::Exact.recorded("X-Tenant"), "X-Tenant");
    assert_eq!(HeaderNames::Lowercase.recorded("X-Tenant"), "x-tenant");
    // Only ASCII letters fold: a header name is a token of ASCII characters on every transport
    // that records names in lowercase.
    assert_eq!(HeaderNames::Lowercase.recorded("Ä-Tenant"), "Ä-tenant");
}

#[test]
fn a_one_value_model_reports_the_first_repeat_beside_the_write_it_replaces() {
    let model = HeaderModel::exact(HeaderLayout::LastWins);
    let written = writes(&["tenant", "route", "Tenant", "route", "tenant"]);
    assert_eq!(
        model.replaced_write(written),
        Some(ReplacedHeaderWrite {
            earlier: LiteralHeaderWrite {
                position: 2,
                name: "route",
            },
            later: LiteralHeaderWrite {
                position: 4,
                name: "route",
            },
        })
    );
    assert_eq!(
        model.replaced_write(writes(&["tenant", "Tenant", "route"])),
        None,
        "names that differ in letter case are two names where names match exactly"
    );
}

#[test]
fn lowercase_names_repeat_a_name_written_in_another_letter_case() {
    let model = HeaderModel::lowercase(HeaderLayout::SortedByName);
    assert_eq!(
        model.replaced_write(writes(&["X-Replaced", "X-Other", "x-replaced"])),
        Some(ReplacedHeaderWrite {
            earlier: LiteralHeaderWrite {
                position: 1,
                name: "X-Replaced",
            },
            later: LiteralHeaderWrite {
                position: 3,
                name: "x-replaced",
            },
        })
    );
}

#[test]
fn a_model_that_keeps_every_value_has_no_replaced_write() {
    for layout in [HeaderLayout::OrderedRepeats, HeaderLayout::RepeatsByName] {
        for names in HeaderNames::iter() {
            let model = HeaderModel {
                layout,
                names,
                values: HeaderValues::Text,
            };
            assert_eq!(
                model.replaced_write(writes(&["route", "route", "Route"])),
                None,
                "{model}"
            );
        }
    }
}

#[test]
fn a_model_displays_its_layout_and_names_and_every_layout_and_names_states_a_meaning() {
    assert_eq!(
        HeaderModel::exact(HeaderLayout::OrderedRepeats).to_string(),
        "ordered_repeats, exact names"
    );
    assert_eq!(
        HeaderModel::lowercase(HeaderLayout::SortedByName).to_string(),
        "sorted_by_name, lowercase names"
    );
    let mut meanings = BTreeSet::new();
    for layout in HeaderLayout::iter() {
        assert!(meanings.insert(layout.meaning()), "{layout:?}");
    }
    for names in HeaderNames::iter() {
        assert!(meanings.insert(names.meaning()), "{names:?}");
    }
}

/// Values typed the way a transport with string, number and binary values types them.
const TYPED_VALUES: &[HeaderValueType] = &[
    HeaderValueType {
        ty: PropertyType::String,
        carried_as: "String",
    },
    HeaderValueType {
        ty: PropertyType::U8,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::I64,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::Bytes,
        carried_as: "Binary",
    },
];

#[test]
fn text_values_hold_only_a_string_and_state_no_carried_type() {
    let values = HeaderModel::exact(HeaderLayout::LastWins).values;
    assert_eq!(values, HeaderValues::Text);
    assert_eq!(values.types(), vec![PropertyType::String]);
    assert_eq!(values.listed(), "STRING");
    assert_eq!(values.carried(), None);
}

#[test]
fn typed_values_hold_exactly_their_declared_types_and_state_what_carries_each() {
    let model = HeaderModel::exact(HeaderLayout::LastWins).with_typed_values(TYPED_VALUES);
    let values = model.values;
    assert_eq!(
        values.types(),
        vec![
            PropertyType::String,
            PropertyType::U8,
            PropertyType::I64,
            PropertyType::Bytes
        ]
    );
    assert_eq!(values.listed(), "STRING, U8, I64 or BYTES");
    assert_eq!(
        values.carried().as_deref(),
        Some("STRING as String; U8, I64 as Number; BYTES as Binary")
    );
    assert_eq!(
        model.to_string(),
        "last_wins, exact names; STRING as String; U8, I64 as Number; BYTES as Binary"
    );
}

/// The header names a generated write draws from: one name in three letter cases, and two more.
const GENERATED_NAMES: [&str; 6] = ["tenant", "Tenant", "TENANT", "route", "x-id", "X-Id"];

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedWrite {
    name: u8,
    value: u8,
}

#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedHeaders {
    layout: u8,
    names: u8,
    #[generator(bolero::generator::produce_with::<Vec<GeneratedWrite>>().len(0_usize..=12))]
    writes: Vec<GeneratedWrite>,
}

/// One header a message carries, under the name its model recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CarriedHeader {
    name: String,
    value: u8,
}

impl GeneratedHeaders {
    fn model(&self) -> HeaderModel {
        let layouts: Vec<_> = HeaderLayout::iter().collect();
        let names: Vec<_> = HeaderNames::iter().collect();
        HeaderModel {
            layout: layouts[usize::from(self.layout) % layouts.len()],
            names: names[usize::from(self.names) % names.len()],
            values: HeaderValues::Text,
        }
    }

    fn name(write: &GeneratedWrite) -> &'static str {
        GENERATED_NAMES[usize::from(write.name) % GENERATED_NAMES.len()]
    }

    /// What a message carries for these writes under `model`, as the model's meaning states it:
    /// every write in order, every value of a name together under the name, or the last value of
    /// each name, with the names of a sorted layout in ascending byte order. Where the meaning
    /// leaves the order of names open, they follow their first write.
    fn carried(&self, model: HeaderModel) -> Vec<CarriedHeader> {
        let mut first_writes = Vec::new();
        let mut values: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut in_order = Vec::new();
        for write in &self.writes {
            let name = model.names.recorded(Self::name(write)).into_owned();
            if !values.contains_key(&name) {
                first_writes.push(name.clone());
            }
            values.entry(name.clone()).or_default().push(write.value);
            in_order.push(CarriedHeader {
                name,
                value: write.value,
            });
        }
        let names = match model.layout {
            HeaderLayout::OrderedRepeats => return in_order,
            HeaderLayout::RepeatsByName | HeaderLayout::LastWins => first_writes,
            HeaderLayout::SortedByName => values.keys().cloned().collect(),
        };
        let mut carried = Vec::new();
        for name in names {
            let written = &values[&name];
            let kept = if model.layout.keeps_one_value_per_name() {
                &written[written.len() - 1..]
            } else {
                &written[..]
            };
            for value in kept {
                carried.push(CarriedHeader {
                    name: name.clone(),
                    value: *value,
                });
            }
        }
        carried
    }
}

/// What `read_headers(name)` returns from `carried`: every value of the name as its model records
/// it, in the order the message carries them.
fn read_headers(model: HeaderModel, carried: &[CarriedHeader], name: &str) -> Vec<u8> {
    let recorded = model.names.recorded(name);
    let mut values = Vec::new();
    for header in carried {
        if header.name == recorded {
            values.push(header.value);
        }
    }
    values
}

/// A header model refuses exactly the literal writes it would lose. Carrying generated writes
/// through any model and reading every written name back returns each name's values in the order
/// written where the model keeps every value, and the last one where it keeps one; a write is lost
/// exactly when the model keeps one value per name and a later write repeats its name as the model
/// records it, and that is when `replaced_write` reports the first repeat beside the first write
/// it replaces.
#[test]
fn bolero_a_header_model_refuses_exactly_the_literal_writes_a_round_trip_would_lose() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .with_type::<GeneratedHeaders>()
        .for_each(|generated| {
            let model = generated.model();
            let carried = generated.carried(model);

            let mut written: BTreeMap<String, Vec<u8>> = BTreeMap::new();
            for write in &generated.writes {
                let name = model
                    .names
                    .recorded(GeneratedHeaders::name(write))
                    .into_owned();
                written.entry(name).or_default().push(write.value);
            }
            let mut every_write_survives = true;
            for (name, values) in &written {
                let read = read_headers(model, &carried, name);
                if model.layout.keeps_one_value_per_name() {
                    assert_eq!(read, values[values.len() - 1..], "{model} {name}");
                    every_write_survives &= values.len() == 1;
                } else {
                    assert_eq!(&read, values, "{model} {name}");
                }
            }

            let literal: Vec<_> = generated
                .writes
                .iter()
                .enumerate()
                .map(|(index, write)| LiteralHeaderWrite {
                    position: index + 1,
                    name: GeneratedHeaders::name(write),
                })
                .collect();
            let replaced = model.replaced_write(literal.iter().copied());
            assert_eq!(replaced.is_none(), every_write_survives, "{model}");
            let Some(replaced) = replaced else {
                return;
            };
            let recorded =
                |write: LiteralHeaderWrite<'_>| model.names.recorded(write.name).into_owned();
            assert_eq!(
                recorded(replaced.earlier),
                recorded(replaced.later),
                "{model}"
            );
            let later = replaced.later.position - 1;
            let mut seen = BTreeMap::new();
            for write in &literal[..later] {
                assert!(
                    seen.insert(recorded(*write), write.position).is_none(),
                    "{model}: a write before {} already repeats a name",
                    replaced.later.position
                );
            }
            assert_eq!(
                seen.get(&recorded(replaced.later)),
                Some(&replaced.earlier.position),
                "{model}: the earlier write is the name's first"
            );
        });
}
