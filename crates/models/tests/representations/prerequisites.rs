//! A node's prerequisite record keeps every value it carries through its gossip text and its
//! archived form, its fingerprints keep their digests through their text, and the verdicts a
//! leader publishes keep every run's verdict through theirs.
//!
//! The valid domain is any record: any cluster identity text, any digests, any consensus floors
//! and any timings a duration holds. The judgment between two records is a function of their
//! values, so a record read back must also judge exactly as the one written. The verdicts' domain
//! is any term and up to a handful of runs, each accepted or refused naming any voter and any
//! mismatch.

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    AdmissionVerdicts, NodePrerequisites, TrustRootFingerprint, WireContractFingerprint,
};

#[test]
fn bolero_prerequisite_records_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let record = arbitrary.node_prerequisites();
            let peer = arbitrary.node_prerequisites();

            let text = record.to_gossip_text();
            let read = NodePrerequisites::from_gossip_text(&text)
                .unwrap_or_else(|error| panic!("{text} must read back: {error:?}"));
            assert_eq!(read, record);
            assert_eq!(read.verify_peer(&peer), record.verify_peer(&peer));

            assert_eq!(
                crate::archive_round_trip!(&record, NodePrerequisites),
                record
            );

            let trust_root = record.trust_root.to_string();
            assert_eq!(
                trust_root.parse::<TrustRootFingerprint>().ok(),
                Some(record.trust_root)
            );
            let wire_contract = record.wire_contract.to_string();
            assert_eq!(
                wire_contract.parse::<WireContractFingerprint>().ok(),
                Some(record.wire_contract)
            );

            let mismatch = arbitrary.prerequisite_mismatch();
            assert_eq!(
                crate::archive_round_trip!(&mismatch, nervix_models::PrerequisiteMismatch),
                mismatch
            );
            assert_eq!(mismatch.clone().reversed().reversed(), mismatch);
        });
}

#[test]
fn bolero_admission_verdicts_round_trip_through_their_gossip_text() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let verdicts = arbitrary.admission_verdicts();

            let text = verdicts.to_gossip_text();
            let read = AdmissionVerdicts::from_gossip_text(&text)
                .unwrap_or_else(|error| panic!("{text} must read back: {error:?}"));
            assert_eq!(read, verdicts);
            assert_eq!(read.leader_term(), verdicts.leader_term());
            for (run, verdict) in verdicts.verdicts() {
                assert_eq!(read.verdict_of(run), Some(verdict));
            }
        });
}
