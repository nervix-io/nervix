//! Tests of the composed Plexus grammar for acknowledged endpoint requests.
//!
//! Layer: test harness.
//! - **Owns.** Endpoint delivery, quiesce and completion assertions.
//! - **Depends on.** The composed statement grammar and its public semantic Models.
//! - **Must not know.** Request execution, network transports or graph scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the composed endpoint grammar")
)]

use crate::conformance_support::{assert_offers_exactly, created_model, refused};

fn statement(protocol: &str, clauses: &str) -> String {
    format!(
        "CREATE {protocol} ENDPOINT INGESTOR events FROM intake {clauses} DECODE USING \
         event_codec TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
         GENERAL ERROR LOG;"
    )
}

#[test]
fn endpoint_ack_accepts_a_bounded_request_window_for_both_protocols() {
    for protocol in ["HTTP", "WEBSOCKETS", "http", "websockets"] {
        let text = statement(
            protocol,
            "MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE REJECT RETRY AFTER 1s",
        );
        assert!(created_model(&text).is_some(), "{text}");
    }
}

#[test]
fn endpoint_ack_requires_positive_capacity_and_a_compatible_quiesce_clause() {
    for protocol in ["HTTP", "WEBSOCKETS"] {
        for clauses in [
            "MODE ACK MAX 0 ACK TIMEOUT 5s ON QUIESCE REJECT RETRY AFTER 1s",
            "MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE",
            "MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE SUSPEND",
            "MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE DROP",
            "MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE REJECT RETRY \
             AFTER 1s",
        ] {
            assert!(refused(&statement(protocol, clauses)), "{clauses}");
        }
    }
}

#[test]
fn endpoint_buffer_responses_follow_the_transport_and_delivery_mode() {
    for (protocol, delivery, response) in [
        ("HTTP", "NO ACK", "RESPOND IMMEDIATE"),
        ("HTTP", "NO ACK", "RESPOND AFTER RESUME"),
        ("WEBSOCKETS", "NO ACK", "RESPOND IMMEDIATE"),
        (
            "WEBSOCKETS",
            "ACK MAX 8 ACK TIMEOUT 5s",
            "RESPOND AFTER RESUME",
        ),
        ("HTTP", "ACK MAX 8 ACK TIMEOUT 5s", "RESPOND AFTER RESUME"),
    ] {
        let clauses = format!("MODE {delivery} ON QUIESCE BUFFER MAX SIZE 1MiB {response}");
        let text = statement(protocol, &clauses);
        assert!(created_model(&text).is_some(), "{text}");
    }
    assert!(refused(&statement(
        "WEBSOCKETS",
        "MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND AFTER RESUME",
    )));

    for protocol in ["HTTP", "WEBSOCKETS"] {
        let buffer = "BUFFER MAX SIZE 1MiB";
        for delivery in ["NO ACK", "ACK MAX 8 ACK TIMEOUT 5s"] {
            let text = statement(protocol, &format!("MODE {delivery} ON QUIESCE {buffer}"));
            assert!(refused(&text), "requires a response choice: {text}");
        }
        let text = format!("ALTER {protocol} ENDPOINT INGESTOR events SET QUIESCE {buffer};");
        assert!(refused(&text), "requires a response choice: {text}");
    }
}

#[test]
fn endpoint_ack_completion_follows_the_request_mode() {
    for protocol in ["HTTP", "WEBSOCKETS"] {
        let prefix = format!("CREATE {protocol} ENDPOINT INGESTOR events FROM intake MODE ");
        assert_offers_exactly(&prefix, &["NO ACK", "ACK MAX 8 ACK TIMEOUT 5s"]);
        assert_offers_exactly(&format!("{prefix}ACK "), &["MAX"]);
        assert_offers_exactly(
            &format!("{prefix}ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB "),
            &["RESPOND AFTER RESUME"],
        );
        assert_offers_exactly(
            &format!("{prefix}NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB "),
            if protocol == "HTTP" {
                &["RESPOND AFTER RESUME", "RESPOND IMMEDIATE"]
            } else {
                &["RESPOND IMMEDIATE"]
            },
        );
    }
}
