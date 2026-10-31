//! Bounded, valid vocabulary values and semantic Models built from the bytes a property receives.
//!
//! A Bolero property hands its bytes to [`Arbitrary`], which reads them as a sequence of bounded
//! choices and builds a current value from them: a name, a literal, an expression, a Model or a
//! statement. Every value is valid for its type and inside the [`Domain`] the property asks for,
//! so a property never skips an input and never passes because its generator produced something
//! invalid. The same bytes always build the same value, which is what lets a saved failure replay.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The byte cursor properties read their choices from, the generators of current
//!   vocabulary values, expressions, Models and statements, and the documented domain each draws
//!   from.
//! - **Depends on.** The vocabulary and the current guest message protocol.
//! - **Must not know.** The language or its parser. What Plexus can spell is stated here as a rule
//!   over values, never learned by calling the parser, so the vocabulary's own properties can use
//!   these generators without a language dependency.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "bounded properties construct current semantic values"
    )
)]

mod client;
mod client_io;
mod cluster_parameters;
mod codec;
mod control;
mod emitter;
mod entropy;
mod expression;
mod external_name;
mod impact;
mod infrastructure;
mod ingestor;
mod model;
mod prerequisites;
mod processor;
mod remote;
mod route;
mod schema;
mod statement;
mod stored;
mod text;
mod wasm;

pub use client::ClientParts;
pub use entropy::Entropy;
pub use expression::{EXPRESSION_DEPTH, ExpressionForm};
pub use model::ModelVariant;
pub use remote::assert_remote_keys_eq;
pub use route::{RouteBranch, RouteFlush, RouteShape};
pub use schema::fields_of_every_type;
pub use statement::StatementVariant;
pub use text::{GeneratedName, KEYWORDS};
pub use wasm::WasmValues;

/// Which values a generator may produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Domain {
    /// Values canonical Plexus spells and reads back as themselves.
    ///
    /// This excludes vocabulary states Plexus has no spelling for: a negative or non-finite numeric
    /// literal (a negative number is written as a negation of its magnitude), an empty array or a
    /// `CASE` without a `WHEN`, a cast to a collection type, a `DROP` of a kind Plexus cannot drop, a
    /// batching HTTP emitter, a correlator filter, a relay named `message` or `branch`, a
    /// placement member named like an `ALTER` operation keyword, and a Postgres update whose
    /// conflict target includes every mapped column.
    Plexus,
    /// Every value the vocabulary types hold, including the states Plexus cannot spell. Stored and
    /// archived forms carry these, so their round trips draw from this domain.
    ///
    /// In both domains a generated name is one lower-case identifier, and any Plexus keyword may be
    /// one. Only the Plexus domain avoids the keywords Plexus refuses for a kind of name or at a
    /// position, such as `message` for a relay. The name properties cover the rest of the name rule
    /// on each name type directly.
    Vocabulary,
}

/// Builds current vocabulary values from the bytes a property received.
#[derive(Debug, Clone)]
pub struct Arbitrary<'bytes> {
    entropy: Entropy<'bytes>,
    domain: Domain,
}

impl<'bytes> Arbitrary<'bytes> {
    pub fn new(bytes: &'bytes [u8], domain: Domain) -> Self {
        Self {
            entropy: Entropy::new(bytes),
            domain,
        }
    }

    /// The domain every value this generator builds belongs to.
    pub fn domain(&self) -> Domain {
        self.domain
    }

    /// The byte cursor, for a property that makes a choice of its own between generated values.
    pub fn entropy(&mut self) -> &mut Entropy<'bytes> {
        &mut self.entropy
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{EmitterEntityKind, IngestorEntityKind, PostgresEmitterEntity};
    use strum::IntoEnumIterator as _;

    use super::{Arbitrary, Domain, Entropy, ExpressionForm, ModelVariant, StatementVariant};

    /// Bytes that differ from seed to seed, so each check sees more than one shape of a value.
    fn seeded_bytes(seed: u8) -> Vec<u8> {
        (0..512_u16)
            .map(|index| {
                let low = index.to_le_bytes()[0];
                low.wrapping_mul(31).wrapping_add(seed.wrapping_mul(17))
            })
            .collect()
    }

    #[test]
    fn every_variant_list_names_each_variant_once_in_declaration_order() {
        assert_eq!(
            ModelVariant::ALL.to_vec(),
            ModelVariant::iter().collect::<Vec<_>>()
        );
        assert_eq!(
            ExpressionForm::ALL.to_vec(),
            ExpressionForm::iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn every_emitter_is_built_as_requested_in_both_domains() {
        for domain in [Domain::Plexus, Domain::Vocabulary] {
            for kind in EmitterEntityKind::all() {
                for seed in 0..4 {
                    let bytes = seeded_bytes(seed);
                    let mut arbitrary = Arbitrary::new(&bytes, domain);
                    let emitter = arbitrary.create_emitter_to(kind);
                    assert_eq!(emitter.connector.kind(), kind, "{domain:?}");
                    assert!(
                        kind.capabilities()
                            .publishing_modes
                            .contains(&emitter.connector.publishing().kind()),
                        "{domain:?} {kind:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn every_source_is_built_as_requested_in_both_domains() {
        for domain in [Domain::Plexus, Domain::Vocabulary] {
            for kind in IngestorEntityKind::all() {
                for seed in 0..4 {
                    let bytes = seeded_bytes(seed);
                    let mut arbitrary = Arbitrary::new(&bytes, domain);
                    let ingestor = arbitrary.create_ingestor_from(kind);
                    assert_eq!(ingestor.input.entity_kind(), kind, "{domain:?}");
                    assert!(
                        ingestor.input.supports_quiesce(ingestor.input.quiesce()),
                        "{domain:?} {kind:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn every_model_family_is_built_as_requested_in_both_domains() {
        for domain in [Domain::Plexus, Domain::Vocabulary] {
            for variant in ModelVariant::ALL {
                for seed in 0..4 {
                    let bytes = seeded_bytes(seed);
                    let mut arbitrary = Arbitrary::new(&bytes, domain);
                    assert_eq!(arbitrary.domain(), domain);
                    let model = arbitrary.model_of(variant);
                    assert_eq!(ModelVariant::of(&model), variant, "{domain:?}");
                }
            }
        }
    }

    #[test]
    fn every_statement_form_is_built_as_requested_in_both_domains() {
        for domain in [Domain::Plexus, Domain::Vocabulary] {
            for variant in StatementVariant::iter() {
                for seed in 0..4 {
                    let bytes = seeded_bytes(seed);
                    let mut arbitrary = Arbitrary::new(&bytes, domain);
                    let statement = arbitrary.statement_of(variant);
                    assert_eq!(StatementVariant::of(&statement), variant, "{domain:?}");
                }
            }
        }
    }

    #[test]
    fn every_expression_form_is_built_as_requested_in_both_domains() {
        for domain in [Domain::Plexus, Domain::Vocabulary] {
            for form in ExpressionForm::ALL {
                for depth in 0..=super::EXPRESSION_DEPTH {
                    let bytes = seeded_bytes(depth);
                    let mut arbitrary = Arbitrary::new(&bytes, domain);
                    let expression = arbitrary.expression_of(form, depth);
                    assert_eq!(ExpressionForm::of(&expression), form, "{domain:?}");
                }
            }
        }
    }

    #[test]
    fn exhausted_bytes_take_every_first_and_smallest_option() {
        let mut entropy = Entropy::new(&[]);
        assert_eq!(entropy.byte(), 0);
        assert!(!entropy.flag());
        assert_eq!(entropy.up_to(u64::MAX), 0);
        assert_eq!(entropy.between(5..=9), 5);
        assert_eq!(entropy.boundary_biased(3..=7), 3);
        assert_eq!(entropy.count(4), 0);
        assert_eq!(entropy.pick(['a', 'b']), 'a');
        assert_eq!(entropy.any_i64(), 0);
    }

    #[test]
    fn choices_stay_inside_their_bounds() {
        for seed in 0..=u8::MAX {
            let bytes = [
                seed,
                seed.wrapping_mul(7),
                seed.wrapping_add(3),
                0xff,
                0x80,
                seed,
            ];
            let mut entropy = Entropy::new(&bytes);
            let value = entropy.between(10..=20);
            assert!((10..=20).contains(&value));
            let biased = entropy.boundary_biased(u64::MAX - 1..=u64::MAX);
            assert!(biased >= u64::MAX - 1);
            let single = entropy.boundary_biased(4..=4);
            assert_eq!(single, 4);
            let positive = entropy.positive_count(std::num::NonZeroUsize::MIN);
            assert_eq!(positive, 1);
        }
    }

    #[test]
    fn a_postgres_update_leaves_a_mapped_column_to_update_in_plexus() {
        for seed in 0..=u8::MAX {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
            let emitter = arbitrary
                .create_emitter_to(EmitterEntityKind::Postgres(PostgresEmitterEntity::Table));
            let nervix_models::EmitterEntity::Postgres(nervix_models::PostgresEmitter::Table(
                nervix_models::PostgresTableEmitter {
                    values,
                    conflict_action,
                    ..
                },
            )) = emitter.connector.as_ref()
            else {
                panic!(
                    "a Postgres emitter was requested, found {:?}",
                    emitter.connector
                );
            };
            assert_eq!(
                conflict_action.validate(values),
                Ok(()),
                "{values:?} {conflict_action:?}"
            );
        }
    }

    #[test]
    fn names_reach_both_ends_of_the_name_bound() {
        let mut lengths = std::collections::BTreeSet::new();
        for seed in 0..=u8::MAX {
            // The first byte declines a keyword spelling, so the rest draws an identifier.
            let bytes = [0, seed, seed.wrapping_mul(13), seed.wrapping_add(1), 0, 0];
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
            let name = arbitrary.name_text();
            assert!(!name.is_empty() && name.len() <= 128, "{name:?}");
            lengths.insert(name.len());
        }
        assert!(lengths.contains(&1));
        assert!(lengths.contains(&128));
    }

    #[test]
    fn a_name_spells_every_keyword() {
        let mut spelled = std::collections::BTreeSet::new();
        for high in 0..=u8::MAX {
            for low in 0..=u8::MAX {
                let bytes = [3, high, low];
                let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
                spelled.insert(arbitrary.name_text());
            }
        }
        for keyword in super::KEYWORDS {
            assert!(spelled.contains(keyword), "{keyword} is never drawn");
        }
    }

    /// The position of a declared type among the types a field declares. The match is exhaustive,
    /// so a new type fails to compile here until the complete list below holds it.
    fn type_class(ty: &nervix_models::ParseAsType) -> usize {
        match ty {
            nervix_models::ParseAsType::U8 => 0,
            nervix_models::ParseAsType::I8 => 1,
            nervix_models::ParseAsType::U16 => 2,
            nervix_models::ParseAsType::I16 => 3,
            nervix_models::ParseAsType::U32 => 4,
            nervix_models::ParseAsType::I32 => 5,
            nervix_models::ParseAsType::U64 => 6,
            nervix_models::ParseAsType::I64 => 7,
            nervix_models::ParseAsType::Bool => 8,
            nervix_models::ParseAsType::String => 9,
            nervix_models::ParseAsType::Datetime => 10,
            nervix_models::ParseAsType::F32 => 11,
            nervix_models::ParseAsType::F64 => 12,
            nervix_models::ParseAsType::Bytes => 13,
            nervix_models::ParseAsType::Vec { .. } => 14,
            nervix_models::ParseAsType::Array { .. } => 15,
        }
    }

    #[test]
    fn the_fields_of_every_type_hold_every_type_and_every_flag_combination() {
        let fields = super::fields_of_every_type();
        let names: std::collections::BTreeSet<&str> =
            fields.iter().map(|field| field.name.as_str()).collect();
        assert_eq!(
            names.len(),
            fields.len(),
            "every field has a name of its own"
        );
        let mut top_level = std::collections::BTreeSet::new();
        let mut listed = std::collections::BTreeSet::new();
        let mut flags = std::collections::BTreeSet::new();
        let mut lengths = std::collections::BTreeSet::new();
        for field in &fields {
            top_level.insert(type_class(&field.ty));
            flags.insert((field.optional, field.sensitive));
            match &field.ty {
                nervix_models::ParseAsType::Vec { element } => {
                    listed.insert(type_class(element));
                }
                nervix_models::ParseAsType::Array { len, .. } => {
                    lengths.insert(len.get());
                }
                _ => {}
            }
        }
        assert_eq!(
            top_level.into_iter().collect::<Vec<_>>(),
            (0..16).collect::<Vec<_>>()
        );
        assert_eq!(
            listed.into_iter().collect::<Vec<_>>(),
            (0..14).collect::<Vec<_>>()
        );
        assert_eq!(
            flags.len(),
            4,
            "optional and sensitive take all four combinations"
        );
        assert_eq!(lengths.into_iter().collect::<Vec<_>>(), vec![1, u32::MAX]);
    }

    #[test]
    fn a_physical_duration_reaches_zero_one_nanosecond_and_the_longest() {
        let mut drawn = std::collections::BTreeSet::new();
        for first in 0..=u8::MAX {
            for second in [0, 1, 4, u8::MAX] {
                let bytes = [
                    first, second, first, second, first, second, first, second, first,
                ];
                let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
                drawn.insert(arbitrary.physical_duration());
            }
        }
        for boundary in [
            std::time::Duration::ZERO,
            std::time::Duration::from_nanos(1),
            std::time::Duration::MAX,
        ] {
            assert!(drawn.contains(&boundary), "{boundary:?} is never drawn");
        }
        assert!(
            drawn
                .iter()
                .any(|duration| duration.subsec_nanos() == 999_999_999
                    && *duration != std::time::Duration::MAX),
            "no duration ends just below a whole second"
        );
    }

    #[test]
    fn a_variant_reaches_every_variant_and_only_variants() {
        let mut drawn = std::collections::BTreeSet::new();
        for first in 0..=u8::MAX {
            let bytes = [first];
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let defect: nervix_models::ClientBatchDefect = arbitrary.variant();
            drawn.insert(<&'static str>::from(defect));
        }
        let every: std::collections::BTreeSet<&'static str> =
            nervix_models::ClientBatchDefect::iter()
                .map(<&'static str>::from)
                .collect();
        assert_eq!(drawn, every);
    }

    /// The position of an outcome among the outcomes a submitted batch has. The match is
    /// exhaustive, so a new outcome fails to compile here until the generator draws it.
    fn outcome_class(outcome: &nervix_models::ClientSubmissionOutcome) -> usize {
        match outcome {
            nervix_models::ClientSubmissionOutcome::NotAdmitted(_) => 0,
            nervix_models::ClientSubmissionOutcome::Completed => 1,
            nervix_models::ClientSubmissionOutcome::ProcessingFailed(_) => 2,
            nervix_models::ClientSubmissionOutcome::OutcomeUnknown(_) => 3,
        }
    }

    /// The position of a refusal among the reasons a batch is not admitted, exhaustive for the
    /// same reason as [`outcome_class`].
    fn refusal_class(refusal: &nervix_models::ClientSubmissionRefusal) -> usize {
        match refusal {
            nervix_models::ClientSubmissionRefusal::InvalidBatch(_) => 0,
            nervix_models::ClientSubmissionRefusal::Suspended => 1,
            nervix_models::ClientSubmissionRefusal::Busy => 2,
            nervix_models::ClientSubmissionRefusal::Draining => 3,
            nervix_models::ClientSubmissionRefusal::ProducerEnded => 4,
            nervix_models::ClientSubmissionRefusal::CreditExceeded => 5,
        }
    }

    #[test]
    fn a_submission_outcome_reaches_every_outcome_and_every_refusal() {
        let mut outcomes = std::collections::BTreeSet::new();
        let mut refusals = std::collections::BTreeSet::new();
        for first in 0..=u8::MAX {
            for second in 0..=u8::MAX {
                let bytes = [first, second, first];
                let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
                let outcome = arbitrary.client_submission_outcome();
                outcomes.insert(outcome_class(&outcome));
                if let nervix_models::ClientSubmissionOutcome::NotAdmitted(refusal) = &outcome {
                    refusals.insert(refusal_class(refusal));
                }
            }
        }
        assert_eq!(outcomes.into_iter().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        assert_eq!(
            refusals.into_iter().collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn client_limits_reach_one_their_declared_bound_and_the_largest_count() {
        let mut producer_batches = std::collections::BTreeSet::new();
        let mut producer_bytes = std::collections::BTreeSet::new();
        let mut consumer_batches = std::collections::BTreeSet::new();
        let mut consumer_bytes = std::collections::BTreeSet::new();
        for first in 0..=u8::MAX {
            for second in 0..=u8::MAX {
                let bytes = [first, second, first, second];
                let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
                let producer = arbitrary.client_producer_limits();
                producer_batches.insert(producer.batches.get());
                producer_bytes.insert(producer.bytes.get());
                let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
                let consumer = arbitrary.client_consumer_limits();
                consumer_batches.insert(consumer.batches.get());
                consumer_bytes.insert(consumer.bytes.get());
            }
        }
        for bound in [1, nervix_models::MAX_CLIENT_PRODUCER_BATCHES, u32::MAX] {
            assert!(
                producer_batches.contains(&bound),
                "{bound} producer batches"
            );
        }
        for bound in [
            1,
            nervix_models::DEFAULT_CLIENT_PRODUCER_SESSION_BYTES,
            u64::MAX,
        ] {
            assert!(producer_bytes.contains(&bound), "{bound} producer bytes");
        }
        for bound in [1, nervix_models::MAX_CLIENT_CONSUMER_BATCHES, u32::MAX] {
            assert!(
                consumer_batches.contains(&bound),
                "{bound} consumer batches"
            );
        }
        for bound in [
            1,
            nervix_models::DEFAULT_CLIENT_CONSUMER_SESSION_BYTES,
            u64::MAX,
        ] {
            assert!(consumer_bytes.contains(&bound), "{bound} consumer bytes");
        }
    }

    #[test]
    fn the_plexus_domain_names_a_relay_by_no_word_plexus_reserves_for_one() {
        for high in 0..=u8::MAX {
            for low in 0..=u8::MAX {
                let bytes = [3, high, low];
                let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
                let relay = arbitrary.name::<nervix_models::RelayName>();
                assert!(!matches!(relay.as_str(), "message" | "branch"), "{relay}");
            }
        }
    }
}
