//! The Postgres declaration: its table rule, conflict rules, value bindings and client schema.
//!
//! Layer: test harness.
//! - **Owns.** Field-by-field evidence for the current POSTGRES TABLE and client declarations.
//! - **Depends on.** The Postgres vocabulary and the shared client schema.
//! - **Must not know.** The driver, network services or runtime insertion.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the Postgres declaration boundary")
)]

use std::num::NonZeroU32;

use meticulous::OptionExt as _;

use super::*;
use crate::{ClientConfigEntry, Expression, ExternalNameError, Literal};

fn entries(pairs: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    pairs
        .iter()
        .map(|(key, value)| ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        })
        .collect()
}

fn mapping(column: &str) -> PostgresValueMapping {
    PostgresValueMapping {
        column: column.to_string(),
        expression: Expression::Literal(Literal::I64(1)),
    }
}

#[test]
fn a_table_keeps_one_or_two_nonempty_parts_exactly_as_written() {
    for (text, parts) in [
        ("Orders", vec!["Orders"]),
        ("sales.Orders", vec!["sales", "Orders"]),
        ("Sales-1.Orders_2~", vec!["Sales-1", "Orders_2~"]),
    ] {
        let name = PostgresTableName::parse(text).assured("a current table rule admits it");
        assert_eq!(name.as_str(), text);
        assert_eq!(name.parts().collect::<Vec<_>>(), parts);
    }
    let boundary = format!("{}.{}", "S".repeat(63), "T".repeat(64));
    PostgresTableName::parse(&boundary).assured("both parts together reach the 128-byte bound");
    for text in [".Orders", "sales.", "sales..Orders", "."] {
        let refusal = PostgresTableName::parse(text).expect_err("an empty part names nothing");
        assert_eq!(
            refusal.current_context(),
            &ExternalNameError::EmptyLevel { separator: '.' },
            "{text}"
        );
    }
    let refusal = PostgresTableName::parse("region.sales.Orders")
        .expect_err("a table names at most its schema and itself");
    assert_eq!(
        refusal.current_context(),
        &ExternalNameError::TooManyLevels { max: 2, actual: 3 }
    );
}

#[test]
fn a_conflict_clause_names_its_columns_and_leaves_one_to_update() {
    let values = [mapping("id"), mapping("note")];
    for action in [
        PostgresConflictAction::None,
        PostgresConflictAction::DoNothing { target: vec![] },
        PostgresConflictAction::DoNothing {
            target: vec!["id".to_string()],
        },
        PostgresConflictAction::DoUpdate {
            target: vec!["id".to_string()],
        },
        // Target columns need not be mapped.
        PostgresConflictAction::DoUpdate {
            target: vec!["Id".to_string(), "tenant".to_string()],
        },
    ] {
        action
            .validate(&values)
            .assured("the clause names its columns and leaves one to update");
    }
    for (action, error) in [
        (
            PostgresConflictAction::DoUpdate { target: vec![] },
            PostgresConflictError::MissingTarget,
        ),
        (
            PostgresConflictAction::DoUpdate {
                target: vec!["id".to_string(), "note".to_string()],
            },
            PostgresConflictError::NothingToUpdate,
        ),
        (
            PostgresConflictAction::DoNothing {
                target: vec![String::new()],
            },
            PostgresConflictError::EmptyColumn,
        ),
        (
            PostgresConflictAction::DoUpdate {
                target: vec!["id".to_string(), String::new()],
            },
            PostgresConflictError::EmptyColumn,
        ),
    ] {
        assert_eq!(action.validate(&values), Err(error), "{action:?}");
    }
}

#[test]
fn every_plexus_type_has_its_declared_text_binding() {
    let length = NonZeroU32::new(2).assured("two is positive");
    for (ty, encoding) in [
        (ParseAsType::Bool, PostgresValueEncoding::BooleanText),
        (ParseAsType::U8, PostgresValueEncoding::DecimalInteger),
        (ParseAsType::I8, PostgresValueEncoding::DecimalInteger),
        (ParseAsType::U16, PostgresValueEncoding::DecimalInteger),
        (ParseAsType::I16, PostgresValueEncoding::DecimalInteger),
        (ParseAsType::U32, PostgresValueEncoding::DecimalInteger),
        (ParseAsType::I32, PostgresValueEncoding::DecimalInteger),
        (ParseAsType::U64, PostgresValueEncoding::DecimalInteger),
        (ParseAsType::I64, PostgresValueEncoding::DecimalInteger),
        (ParseAsType::F32, PostgresValueEncoding::DecimalFloat),
        (ParseAsType::F64, PostgresValueEncoding::DecimalFloat),
        (ParseAsType::String, PostgresValueEncoding::Text),
        (ParseAsType::Bytes, PostgresValueEncoding::ByteaHex),
        (ParseAsType::Datetime, PostgresValueEncoding::TimestampText),
        (
            ParseAsType::Array {
                element: Box::new(ParseAsType::Bytes),
                len: length,
            },
            PostgresValueEncoding::JsonText,
        ),
        (
            ParseAsType::Vec {
                element: Box::new(ParseAsType::F32),
            },
            PostgresValueEncoding::JsonText,
        ),
    ] {
        assert_eq!(PostgresValueEncoding::for_type(&ty), encoding, "{ty}");
    }
}

#[test]
fn every_binding_names_how_postgres_reads_it_back() {
    for (encoding, text) in [
        (PostgresValueEncoding::BooleanText, "true or false"),
        (PostgresValueEncoding::DecimalInteger, "full U64 range"),
        (
            PostgresValueEncoding::DecimalFloat,
            "non-finite value binds NULL",
        ),
        (PostgresValueEncoding::Text, "UTF-8"),
        (PostgresValueEncoding::ByteaHex, "bytea hex"),
        (PostgresValueEncoding::TimestampText, "RFC 3339"),
        (PostgresValueEncoding::JsonText, "padded base64"),
    ] {
        assert!(encoding.to_string().contains(text), "{encoding:?}");
    }
}

#[test]
fn the_batch_maximum_is_the_largest_message_the_server_reads() {
    assert_eq!(POSTGRES_MESSAGE_SIZE_MAXIMUM.bytes().get(), 1_073_741_822);
    assert_eq!(POSTGRES_MESSAGE_SIZE_MAXIMUM.to_string(), "1073741822B");
    assert_eq!(
        PostgresTableEmitter::CAPABILITIES.batch,
        EmitterBatchLegality::Required {
            maximum: Some(POSTGRES_MESSAGE_SIZE_MAXIMUM)
        }
    );
}

#[test]
fn the_address_selects_one_declared_tls_policy() {
    assert_eq!(
        PostgresSslMode::QUERY_OPTION.choices,
        ["disable", "verify-full"]
    );
    assert_eq!(
        PostgresSslMode::VERIFY_FULL.parse(),
        Ok(PostgresSslMode::VerifyFull)
    );
    for (address, mode) in [
        (
            "postgres://127.0.0.1/nervix?sslmode=disable",
            PostgresSslMode::Disable,
        ),
        (
            "postgresql://db.example.com/nervix?application_name=nervix&ssl-mode=verify-full",
            PostgresSslMode::VerifyFull,
        ),
    ] {
        let address = Url::parse(address).assured("a fixture URL");
        assert_eq!(
            PostgresSslMode::of_address(&address).assured("one declared policy"),
            mode
        );
    }
    for address in [
        "postgres://127.0.0.1/nervix",
        "postgres://127.0.0.1/nervix?sslmode=require",
        "postgres://127.0.0.1/nervix?sslmode=Disable",
        "postgres://127.0.0.1/nervix?sslmode=disable&sslmode=disable",
        "postgres://127.0.0.1/nervix?sslmode=disable&ssl-mode=verify-full",
    ] {
        let address = Url::parse(address).assured("a fixture URL");
        let refusal = PostgresSslMode::of_address(&address)
            .expect_err("an address selects exactly one declared policy once");
        assert!(
            matches!(
                refusal.current_context(),
                ClientConfigSchemaError::QueryOption { key, option: "sslmode", .. } if key == "addr"
            ),
            "{address}: {refusal:?}"
        );
    }
}

#[test]
fn the_client_schema_owns_the_scheme_and_holds_tls_files_to_verify_full() {
    let schema = ConnectorKind::Postgres
        .client()
        .assured("Postgres declares a configured client")
        .config;
    for accepted in [
        entries(&[(
            "addr",
            "postgres://user:password@127.0.0.1:5432/nervix?sslmode=disable",
        )]),
        entries(&[(
            "addr",
            "postgresql://127.0.0.1/nervix?sslmode=disable&application_name=nervix",
        )]),
        entries(&[
            (
                "addr",
                "postgresql://db.example.com/nervix?sslmode=verify-full",
            ),
            ("tls_ca_file", "/mount/ca.pem"),
            ("tls_cert_file", "/mount/client.pem"),
            ("tls_key_file", "/mount/client.key"),
        ]),
    ] {
        schema
            .validate(&accepted)
            .assured("the declared policy admits it");
        schema
            .validate_rendered(&accepted)
            .assured("the rendered value is the same");
    }
    schema
        .validate(&entries(&[
            ("addr", "{{ primary }}"),
            ("tls_ca_file", "/mount/ca.pem"),
        ]))
        .assured("a templated address is checked once it is rendered");

    let refusal = schema
        .validate_rendered(&entries(&[(
            "addr",
            "http://user:private-password@127.0.0.1/nervix?sslmode=disable",
        )]))
        .expect_err("the scheme is Nervix's");
    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::Scheme {
            key: "addr".to_string(),
            schemes: &["postgres", "postgresql"]
        }
    );
    assert!(!format!("{refusal:?}").contains("private-password"));

    let refusal = schema
        .validate_rendered(&entries(&[(
            "addr",
            "postgres://user:private-password@127.0.0.1/nervix?sslmode=prefer",
        )]))
        .expect_err("the TLS policy is Nervix's");
    assert_eq!(
        refusal.to_string(),
        "client config key 'addr' must set the sslmode query option once, to disable or \
         verify-full"
    );
    assert!(!format!("{refusal:?}").contains("private-password"));

    let plaintext = ("addr", "postgres://127.0.0.1/nervix?sslmode=disable");
    for (config, tls_key) in [
        (
            entries(&[plaintext, ("tls_ca_file", "/mount/ca.pem")]),
            "tls_ca_file",
        ),
        (
            entries(&[
                plaintext,
                ("tls_cert_file", "/mount/client.pem"),
                ("tls_key_file", "/mount/client.key"),
            ]),
            "tls_cert_file",
        ),
    ] {
        let refusal = schema
            .validate_rendered(&config)
            .expect_err("TLS files configure the verifying policy only");
        assert_eq!(
            refusal.current_context(),
            &ClientConfigSchemaError::RequiresQuery {
                key: tls_key,
                address: "addr",
                option: "sslmode",
                value: "verify-full",
            }
        );
    }
}
