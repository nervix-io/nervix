//! MongoDB collection and configuration declaration evidence.
//!
//! Layer: test harness.
//! - **Owns.** Current collection-name and client configuration contracts.
//! - **Depends on.** The MongoDB vocabulary and its shared validation boundary.
//! - **Must not know.** The driver, network or runtime graph.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns MongoDB vocabulary conformance")
)]

use meticulous::ResultExt as _;

use super::*;

#[test]
fn collection_names_preserve_the_external_namespace() {
    for text in ["Orders", "sales.archive", "Orders 2026", "注文", "café"] {
        let name = MongoDbCollectionName::parse(text).assured("a legal collection name parses");
        assert_eq!(name.as_str(), text);
    }
    for text in [
        "",
        "bad$name",
        "bad\0name",
        "system.views",
        "orders.system.views",
    ] {
        assert!(MongoDbCollectionName::parse(text).is_err(), "{text:?}");
    }
    assert!(MongoDbCollectionName::parse(&"x".repeat(119)).is_err());
}

#[test]
fn namespace_budget_counts_database_separator_and_utf8_bytes() {
    let name = MongoDbCollectionName::parse(&"é".repeat(59))
        .assured("the collection occupies the maximum 118 bytes");
    name.validate_namespace("d")
        .assured("the complete namespace occupies exactly 120 bytes");
    let refusal = name
        .validate_namespace("db")
        .expect_err("the complete namespace occupies 121 bytes");
    assert!(matches!(
        refusal.current_context(),
        ExternalNameError::TooLong {
            max: 120,
            actual: 121
        }
    ));
}
