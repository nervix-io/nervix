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
//! - **Depends on.** The vocabulary.
//! - **Must not know.** The language or its parser. What NSPL can spell is stated here as a rule
//!   over values, never learned by calling the parser, so the vocabulary's own properties can use
//!   these generators without a language dependency.

mod client;
mod codec;
mod emitter;
mod entropy;
mod expression;
mod infrastructure;
mod ingestor;
mod model;
mod processor;
mod route;
mod schema;
mod statement;
mod text;

pub use client::ClientParts;
pub use emitter::SinkVariant;
pub use entropy::Entropy;
pub use expression::{EXPRESSION_DEPTH, ExpressionForm};
pub use model::ModelVariant;
pub use route::{RouteBranch, RouteFlush, RouteShape};
pub use statement::StatementVariant;
pub use text::{GeneratedName, KEYWORDS};

/// Which values a generator may produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Domain {
    /// Values canonical NSPL spells and reads back as themselves.
    ///
    /// This excludes vocabulary states NSPL has no spelling for: a negative or non-finite numeric
    /// literal (a negative number is written as a negation of its magnitude), an empty array or a
    /// `CASE` without a `WHEN`, a cast to a collection type, a `DROP` of a kind NSPL cannot drop, a
    /// batching HTTP emitter, a correlator filter, a relay named `message` or `branch`, and a
    /// placement member named like an `ALTER` operation keyword.
    Nspl,
    /// Every value the vocabulary types hold, including the states NSPL cannot spell. Stored and
    /// archived forms carry these, so their round trips draw from this domain.
    ///
    /// In both domains a generated name is one lower-case identifier, and any NSPL keyword may be
    /// one. Only the NSPL domain avoids the keywords NSPL refuses for a kind of name or at a
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

    /// The largest count a `usize` field of a Model or statement holds in this domain.
    ///
    /// NSPL spells any 64-bit count. The archived form of a `usize` is 32 bits wide and truncates a
    /// larger count without an error, so the vocabulary's archived round trips draw counts only
    /// from the range that form keeps; the truncation is a recorded storage defect, not a domain
    /// the archive claims.
    pub(crate) fn largest_archived_count(&self) -> u64 {
        match self.domain {
            Domain::Nspl => u64::MAX,
            Domain::Vocabulary => u64::from(u32::MAX),
        }
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator as _;

    use super::{
        Arbitrary, Domain, Entropy, ExpressionForm, ModelVariant, SinkVariant, StatementVariant,
    };

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
        assert_eq!(
            SinkVariant::ALL.to_vec(),
            SinkVariant::iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn every_sink_is_built_as_requested_in_both_domains() {
        for domain in [Domain::Nspl, Domain::Vocabulary] {
            for variant in SinkVariant::ALL {
                for seed in 0..4 {
                    let bytes = seeded_bytes(seed);
                    let mut arbitrary = Arbitrary::new(&bytes, domain);
                    let emitter = arbitrary.create_emitter_to(variant);
                    assert_eq!(SinkVariant::of(&emitter.sink), variant, "{domain:?}");
                    assert!(
                        emitter
                            .sink
                            .accepts_publishing_mode(&emitter.publishing_mode),
                        "{domain:?} {variant:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn every_model_family_is_built_as_requested_in_both_domains() {
        for domain in [Domain::Nspl, Domain::Vocabulary] {
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
        for domain in [Domain::Nspl, Domain::Vocabulary] {
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
        for domain in [Domain::Nspl, Domain::Vocabulary] {
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
    fn a_postgres_update_leaves_a_mapped_column_to_update_in_nspl() {
        for seed in 0..=u8::MAX {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Nspl);
            let emitter = arbitrary.create_emitter_to(SinkVariant::Postgres);
            let nervix_models::EmitSink::Postgres {
                values,
                conflict_action,
                ..
            } = emitter.sink.as_ref()
            else {
                panic!("a Postgres sink was requested, found {:?}", emitter.sink);
            };
            if let nervix_models::PostgresConflictAction::DoUpdate { target } = conflict_action {
                assert!(!target.is_empty(), "{values:?}");
                assert!(
                    values
                        .iter()
                        .any(|mapping| !target.contains(&mapping.column)),
                    "{values:?} {target:?}"
                );
            }
        }
    }

    #[test]
    fn names_reach_both_ends_of_the_name_bound() {
        let mut lengths = std::collections::BTreeSet::new();
        for seed in 0..=u8::MAX {
            // The first byte declines a keyword spelling, so the rest draws an identifier.
            let bytes = [0, seed, seed.wrapping_mul(13), seed.wrapping_add(1), 0, 0];
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Nspl);
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
                let mut arbitrary = Arbitrary::new(&bytes, Domain::Nspl);
                spelled.insert(arbitrary.name_text());
            }
        }
        for keyword in super::KEYWORDS {
            assert!(spelled.contains(keyword), "{keyword} is never drawn");
        }
    }

    #[test]
    fn the_nspl_domain_names_a_relay_by_no_word_nspl_reserves_for_one() {
        for high in 0..=u8::MAX {
            for low in 0..=u8::MAX {
                let bytes = [3, high, low];
                let mut arbitrary = Arbitrary::new(&bytes, Domain::Nspl);
                let relay = arbitrary.name::<nervix_models::RelayName>();
                assert!(!matches!(relay.as_str(), "message" | "branch"), "{relay}");
            }
        }
    }
}
