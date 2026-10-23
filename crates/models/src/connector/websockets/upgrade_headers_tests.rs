//! Owner checks for the literal WebSockets upgrade fields.
//!
//! Layer: test harness.
//! - **Owns.** Current WebSockets vocabulary validation evidence.
//! - **Depends on.** The public vocabulary and standard value types.
//! - **Must not know.** Network drivers or runtime plans.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns current vocabulary validation evidence")
)]

use meticulous::ResultExt as _;

use super::upgrade_headers::{
    WebsocketsUpgradeHeader, WebsocketsUpgradeHeaders, WebsocketsUpgradeHeadersError,
};

fn header(name: &str, value: &str) -> WebsocketsUpgradeHeader {
    WebsocketsUpgradeHeader {
        name: name.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn application_headers_keep_values_and_normalize_field_names() {
    let headers = WebsocketsUpgradeHeaders::parse(&[
        header("X-Event", "orders"),
        header("Authorization", "Bearer fixture"),
    ])
    .assured("two valid application headers fit the HTTP bounds");
    let values: Vec<_> = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    assert_eq!(
        values,
        [("authorization", "Bearer fixture"), ("x-event", "orders")]
    );
}

#[test]
fn upgrade_fields_refuse_protocol_fields_duplicates_injection_and_oversize_headers() {
    for name in [
        "Host",
        "Connection",
        "Upgrade",
        "Sec-WebSocket-Key",
        "sec-websocket-protocol",
        "x\nbad",
    ] {
        let Err(error) = WebsocketsUpgradeHeaders::parse(&[header(name, "value")]) else {
            panic!("the protocol owns {name}, or the name breaks HTTP field rules");
        };
        assert_eq!(
            error.current_context(),
            &WebsocketsUpgradeHeadersError::InvalidName {
                name: name.to_string()
            }
        );
    }
    let Err(repeated) =
        WebsocketsUpgradeHeaders::parse(&[header("X-Event", "one"), header("x-event", "two")])
    else {
        panic!("an upgrade sets each case-insensitive application field once");
    };
    assert_eq!(
        repeated.current_context(),
        &WebsocketsUpgradeHeadersError::RepeatedName {
            name: "x-event".to_string()
        }
    );
    let Err(injection) = WebsocketsUpgradeHeaders::parse(&[header("x-event", "hidden\r\nvalue")])
    else {
        panic!("a header cannot inject a second field");
    };
    assert_eq!(
        injection.current_context(),
        &WebsocketsUpgradeHeadersError::InvalidValue {
            name: "x-event".to_string()
        }
    );
    assert!(!format!("{injection:#}").contains("hidden"));
    let Err(limit) = WebsocketsUpgradeHeaders::parse(&[header("x-event", &"x".repeat(32 * 1024))])
    else {
        panic!("the name and value exceed 32 KiB");
    };
    assert_eq!(
        limit.current_context(),
        &WebsocketsUpgradeHeadersError::Limit
    );
}

#[test]
fn upgrade_header_limits_cover_count_and_total_bytes() {
    for headers in [
        (0..129)
            .map(|index| header(&format!("x-field-{index}"), "v"))
            .collect(),
        vec![
            header("x-one", &"a".repeat(16 * 1024)),
            header("x-two", &"b".repeat(16 * 1024)),
        ],
    ] {
        let Err(error) = WebsocketsUpgradeHeaders::parse(&headers) else {
            panic!("the count or total field bytes exceed the current HTTP bounds");
        };
        assert_eq!(
            error.current_context(),
            &WebsocketsUpgradeHeadersError::Limit
        );
    }
}

// Lowercase, ordered, unique fields are the lossless domain of HTTP name canonicalization.
#[test]
fn bolero_validated_upgrade_headers_preserve_every_canonical_field() {
    bolero::check!().with_type::<[u16; 4]>().for_each(|input| {
        let original: Vec<_> = input
            .iter()
            .enumerate()
            .map(|(index, value)| header(&format!("x-field-{index}"), &format!("fixture-{value}")))
            .collect();
        let validated = WebsocketsUpgradeHeaders::parse(&original)
            .assured("four short canonical application fields fit the current bounds");
        let decoded: Vec<_> = validated
            .iter()
            .map(|(name, value)| header(name.as_str(), value.as_str()))
            .collect();
        assert_eq!(decoded, original);
    });
}
