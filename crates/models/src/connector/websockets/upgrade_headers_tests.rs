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

use meticulous::{OptionExt as _, ResultExt as _};

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
fn upgrade_fields_refuse_protocol_fields_duplicates_and_injection() {
    for name in [
        "Host",
        "Connection",
        "Upgrade",
        "Sec-WebSocket-Key",
        "Sec-WebSocket-Version",
        "sec-websocket-extensions",
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
}

#[test]
fn upgrade_headers_support_the_committed_policy_range() {
    let headers: Vec<_> = (0..129)
        .map(|index| header(&format!("x-field-{index}"), "v"))
        .collect();
    let parsed = WebsocketsUpgradeHeaders::parse(&headers)
        .assured("the representation can hold a policy allowing129fields");
    assert_eq!(parsed.iter().count(), 129);
    let value = "v".repeat(32 * 1024);
    let parsed = WebsocketsUpgradeHeaders::parse(&[header("x-key", &value)])
        .assured("registry policy decides the admissible byte cap");
    assert_eq!(parsed.iter().next().assured("one field").1.as_str(), value);
}

#[test]
fn upgrade_header_count_stays_within_the_request_representation() {
    let maximum = usize::try_from(crate::HttpHeaderFields::MAXIMUM)
        .assured("the application-field representation bound fits usize");
    let headers: Vec<_> = (0..=maximum)
        .map(|index| header(&format!("x-field-{index}"), "v"))
        .collect();
    let error = WebsocketsUpgradeHeaders::parse(&headers)
        .expect_err("application fields leave capacity for transport-owned headers");
    assert_eq!(
        error.current_context(),
        &WebsocketsUpgradeHeadersError::Limit
    );
}

#[test]
fn a_subprotocol_offer_is_unique_tokens_written_without_whitespace() {
    let headers = WebsocketsUpgradeHeaders::parse(&[
        header("Sec-WebSocket-Protocol", "v2.events, v1.events\t,chat"),
        header("Authorization", "Bearer fixture"),
    ])
    .assured("an offer of three unique tokens and one application field fit the HTTP bounds");
    let values: Vec<_> = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    assert_eq!(
        values,
        [
            ("authorization", "Bearer fixture"),
            ("sec-websocket-protocol", "v2.events,v1.events,chat")
        ]
    );
    for (offer, expected) in [
        (
            "",
            WebsocketsUpgradeHeadersError::EmptySubprotocol { element: 1 },
        ),
        (
            "events-a,,events-b",
            WebsocketsUpgradeHeadersError::EmptySubprotocol { element: 2 },
        ),
        (
            "events-a, ",
            WebsocketsUpgradeHeadersError::EmptySubprotocol { element: 2 },
        ),
        (
            "events a",
            WebsocketsUpgradeHeadersError::InvalidSubprotocol { element: 1 },
        ),
        (
            "events-a,events/b",
            WebsocketsUpgradeHeadersError::InvalidSubprotocol { element: 2 },
        ),
        (
            "events-\u{e9}",
            WebsocketsUpgradeHeadersError::InvalidSubprotocol { element: 1 },
        ),
        (
            "events-a, events-b, events-a",
            WebsocketsUpgradeHeadersError::RepeatedSubprotocol { element: 3 },
        ),
    ] {
        let Err(error) =
            WebsocketsUpgradeHeaders::parse(&[header("sec-websocket-protocol", offer)])
        else {
            panic!("{offer:?} is not an offer of unique subprotocol tokens");
        };
        assert_eq!(error.current_context(), &expected);
        // An offer can carry a credential, so a refusal names a position and never a value.
        assert!(!format!("{error:#}").contains("events"));
    }
}

#[test]
fn a_subprotocol_offer_counts_toward_the_upgrade_header_bounds() {
    let maximum = usize::try_from(crate::HttpHeaderFields::MAXIMUM)
        .assured("the application-field representation bound fits usize");
    let mut headers: Vec<_> = (0..maximum - 1)
        .map(|index| header(&format!("x-field-{index}"), "v"))
        .collect();
    headers.push(header("sec-websocket-protocol", "chat"));
    WebsocketsUpgradeHeaders::parse(&headers)
        .assured("the offer fills the final application-field slot");
    headers.push(header("x-final", "v"));
    let Err(error) = WebsocketsUpgradeHeaders::parse(&headers) else {
        panic!("an extra field exceeds the request representation");
    };
    assert_eq!(
        error.current_context(),
        &WebsocketsUpgradeHeadersError::Limit
    );
}

/// The subprotocol written for generated position `index` and value `value`: a token unique to
/// its position.
fn subprotocol(index: usize, value: u16) -> String {
    format!("v{index}.events-{value}")
}

// Lowercase, ordered, unique fields, with an offer already written as tokens joined by commas,
// are the lossless domain of HTTP name and offer canonicalization.
#[test]
fn bolero_validated_upgrade_headers_preserve_every_canonical_field() {
    bolero::check!()
        .with_type::<([u16; 4], u8)>()
        .for_each(|(input, offered)| {
            let offered = usize::from(*offered % 4);
            let mut original = Vec::new();
            if offered > 0 {
                let mut tokens = Vec::new();
                for (index, value) in input.iter().take(offered).enumerate() {
                    tokens.push(subprotocol(index, *value));
                }
                original.push(header("sec-websocket-protocol", &tokens.join(",")));
            }
            for (index, value) in input.iter().enumerate() {
                original.push(header(
                    &format!("x-field-{index}"),
                    &format!("fixture-{value}"),
                ));
            }
            let validated = WebsocketsUpgradeHeaders::parse(&original).assured(
                "four short canonical application fields and an offer of at most three tokens fit \
                 the current bounds",
            );
            let decoded: Vec<_> = validated
                .iter()
                .map(|(name, value)| header(name.as_str(), value.as_str()))
                .collect();
            assert_eq!(decoded, original);
        });
}

/// The whitespace written before or after a comma, chosen by two bits of `choice`.
fn optional_whitespace(choice: u8) -> &'static str {
    match choice % 4 {
        0 => "",
        1 => " ",
        2 => "\t",
        _ => " \t ",
    }
}

// Canonicalizing contract: an offer written with any optional whitespace around its commas
// validates to its tokens, in written order, joined by commas alone.
#[test]
fn bolero_subprotocol_offers_validate_to_their_tokens_in_written_order() {
    bolero::check!()
        .with_type::<([u16; 4], [u8; 4])>()
        .for_each(|(values, spacing)| {
            let mut tokens = Vec::new();
            for (index, value) in values.iter().enumerate() {
                tokens.push(subprotocol(index, *value));
            }
            let mut written = String::new();
            for (index, token) in tokens.iter().enumerate() {
                if index > 0 {
                    written.push_str(optional_whitespace(spacing[index]));
                    written.push(',');
                    written.push_str(optional_whitespace(spacing[index] / 4));
                }
                written.push_str(token);
            }
            let validated =
                WebsocketsUpgradeHeaders::parse(&[header("Sec-WebSocket-Protocol", &written)])
                    .assured("four unique short tokens form an offer within the current bounds");
            let values: Vec<_> = validated
                .iter()
                .map(|(name, value)| (name.as_str().to_string(), value.as_str().to_string()))
                .collect();
            assert_eq!(
                values,
                [("sec-websocket-protocol".to_string(), tokens.join(","))]
            );
        });
}
