//! Differential checks for the vocabulary's NATS subject contract.
//!
//! Layer: test harness.
//! - **Owns.** Comparison of Nervix's typed subject names with the pinned NATS client's check.
//! - **Depends on.** Public NATS name types and `async-nats::Subject`.
//! - **Must not know.** Plexus parsing, connector runtime state or a live NATS server.

use async_nats::Subject;
use nervix_models::{NatsPublishSubjectName, NatsSubjectName};

#[test]
fn accepted_nervix_subjects_pass_the_nats_client_check() {
    let tokens = [
        "orders", "Created", "*", ">", "New*", "x>", "", "é", "$JS", "topic ",
    ];
    for first in tokens {
        for second in tokens {
            for third in tokens {
                let raw = format!("{first}.{second}.{third}");
                let client_accepts = Subject::from(raw.clone()).is_valid();
                if NatsSubjectName::try_from(raw.as_str()).is_ok() {
                    assert!(client_accepts, "subscription accepted by Nervix: {raw:?}");
                }
                if NatsPublishSubjectName::try_from(raw.as_str()).is_ok() {
                    assert!(client_accepts, "publication accepted by Nervix: {raw:?}");
                    assert!(NatsSubjectName::try_from(raw.as_str()).is_ok());
                }
            }
        }
    }
}

#[test]
fn the_client_framing_check_does_not_override_nervix_wildcard_rules() {
    for raw in ["time.New*.east", "orders.>.created", "orders.x>"] {
        assert!(Subject::from(raw.to_string()).is_valid(), "{raw:?}");
        assert!(NatsSubjectName::try_from(raw).is_err(), "{raw:?}");
        assert!(NatsPublishSubjectName::try_from(raw).is_err(), "{raw:?}");
    }
}
