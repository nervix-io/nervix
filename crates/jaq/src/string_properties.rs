//! Complete string values through the shared YAML wire boundary.
//!
//! Layer: test harness.
//! - **Owns.** A bounded current object domain and its complete value and member-order oracle.
//! - **Depends on.** The production YAML writer and reader and JSON semantic values.
//! - **Must not know.** Connector frames, endpoint outcomes, schemas or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the representation harness declared above")
)]

use meticulous::ResultExt as _;
use serde_json::{Map, Value, json};

use super::JaqNativeFormat;

#[test]
fn bolero_yaml_objects_preserve_complete_string_values() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let text = String::from_utf8_lossy(bytes);
            let reversed = text.chars().rev().collect::<String>();
            let mut fields = Map::new();
            fields.insert(
                format!("key:{text}"),
                json!({ "text": text, "reversed": reversed, "items": [text, null, bytes.len()] }),
            );
            fields.insert(
                "tail".to_owned(),
                json!([bytes.first().is_some_and(|b| b & 1 != 0), text]),
            );
            let input = Value::Object(fields);
            let encoded = JaqNativeFormat::Yaml
                .write_value(input.clone())
                .assured("the bounded JSON object has a YAML representation");
            let decoded = JaqNativeFormat::Yaml
                .read_single_value(&encoded)
                .assured("the emitted YAML is one complete value");
            assert_eq!(decoded, input);
            assert_eq!(
                serde_json::to_vec(&decoded).assured("the decoded value is JSON"),
                serde_json::to_vec(&input).assured("the original value is JSON"),
                "the complete nested object also preserves declaration order"
            );
        });
}
