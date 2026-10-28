//! Emitter envelopes keep every clause and property through their stored forms.
//!
//! Layer: test harness.
//! - **Owns.** The round trip of a generated emitter envelope through JSON and the archive.
//! - **Depends on.** The vocabulary's envelope and the generators of `nervix-arbitrary`.
//! - **Must not know.** The grammar that spells an envelope or the host that evaluates one.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{EmitterEntityKind, EmitterEnvelope, KafkaEmitterEntity};

/// Every generated envelope, of every clause kind and of properties of any name in written order,
/// keeps each clause, duration and property through its JSON and archived forms.
#[test]
fn bolero_envelopes_keep_every_clause_and_property_through_json_and_the_archive() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let envelope =
                arbitrary.emitter_envelope_of(EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic));

            let json = serde_json::to_vec(&envelope).expect("an envelope serializes");
            let from_json: EmitterEnvelope =
                serde_json::from_slice(&json).expect("a serialized envelope reads back");
            assert_eq!(from_json, envelope);

            let from_archive = crate::archive_round_trip!(&envelope, EmitterEnvelope);
            assert_eq!(from_archive, envelope);
        });
}
