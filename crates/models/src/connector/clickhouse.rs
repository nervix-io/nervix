//! The ClickHouse connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The ClickHouse client and table emitter Models, their configuration and locator
//!   rules, and the public mapped-value encoding contract.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The ClickHouse driver or the row writer's implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{DeadlineKey, DeclaredBounds, DeclaredWait, OmittedDeadline, WaitBound},
    configured_client::configured_client,
    entity_union::connector_emitters,
};
use crate::{
    ClientConfigEntry, ClientConfigSchemaError, ClientName, ConnectorKind, EmitterBatchLegality,
    EmitterBodyForm, EmitterClientReference, EmitterDeclaration, EmitterEntityBody,
    EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind, EmitterValueMapping,
    EmitterValueMappings, LocatorDeclaration, LocatorName,
};

configured_client!(ClickHouse: unpooled, config CLIENT_CONFIG, CreateClientClickHouse => ClickHouseConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::Url(&["http", "https"])),
            K::optional("database", V::String),
            K::optional("password", V::String).secret(),
            K::defaulted("pool_idle_timeout_ms", V::Milliseconds, "2000"),
            K::defaulted("tcp_keepalive_ms", V::TcpKeepaliveMilliseconds, "60000"),
            K::required("timeout_ms", V::Milliseconds),
            K::TLS_CA.required_for_scheme("addr", "https"),
            K::TLS_CERT,
            K::TLS_KEY,
            K::optional("user", V::String),
        ],
        &[],
        &[],
    )
};

/// The ClickHouse transport relationship in addition to the declared key and value schema.
/// Validation happens after configuration templates and resource mounts have been rendered.
pub fn validate_clickhouse_client_config(
    entries: &[ClientConfigEntry],
) -> Result<(), Report<ClientConfigSchemaError>> {
    if entries
        .iter()
        .find(|entry| entry.key == "addr")
        .and_then(|entry| url::Url::parse(&entry.value).ok())
        .is_some_and(|address| address.scheme() == "http")
    {
        for key in ["tls_ca_file", "tls_cert_file", "tls_key_file"] {
            if entries.iter().any(|entry| entry.key == key) {
                return Err(Report::new(ClientConfigSchemaError::InvalidValue {
                    key: key.to_string(),
                    expected: "an HTTPS addr when TLS files are configured",
                }));
            }
        }
    }
    CLIENT_CONFIG.validate_rendered(entries)
}

/// The public encoding of one family of mapped Plexus values in ClickHouse `JSONEachRow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickHouseValueEncoding {
    pub plexus_type: &'static str,
    pub json_each_row: &'static str,
}

/// The types a `CLICKHOUSE TABLE` mapping admits and the bytes its row writer emits.
/// `MappedValueType::Column` rejects any other inferred Arrow type before the emitter opens,
/// including null-only, decimal, structured, dictionary, large-string and non-nanosecond
/// timestamp values. A list is admitted only when its element type is admitted recursively.
pub const CLICKHOUSE_VALUE_ENCODINGS: &[ClickHouseValueEncoding] = &[
    ClickHouseValueEncoding {
        plexus_type: "BOOL",
        json_each_row: "JSON boolean",
    },
    ClickHouseValueEncoding {
        plexus_type: "U8, I8, U16, I16, U32, I32, U64, I64",
        json_each_row: "JSON integer number, including the full U64 range",
    },
    ClickHouseValueEncoding {
        plexus_type: "F32",
        json_each_row: "JSON number widened to F64; non-finite values are null",
    },
    ClickHouseValueEncoding {
        plexus_type: "F64",
        json_each_row: "JSON number; non-finite values are null",
    },
    ClickHouseValueEncoding {
        plexus_type: "STRING",
        json_each_row: "escaped JSON string",
    },
    ClickHouseValueEncoding {
        plexus_type: "BYTES",
        json_each_row: "raw octets in a JSON string, including inside lists",
    },
    ClickHouseValueEncoding {
        plexus_type: "DATETIME",
        json_each_row: "RFC 3339 JSON string",
    },
    ClickHouseValueEncoding {
        plexus_type: "ARRAY<T>, VEC<T>",
        json_each_row: "JSON array with recursive element encoding",
    },
];

pub type ClickHouseValueMapping = EmitterValueMapping;

/// One table a ClickHouse client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClickHouseTableEmitter {
    pub mode: ClickHouseTablePublishingMode,
    pub client: ClientName,
    pub table: ClickHouseTableName,
    pub values: Vec<ClickHouseValueMapping>,
}

/// What the connector of a CLICKHOUSE TABLE holds and how long it waits.
const CLICKHOUSE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[DeclaredWait {
        wait: "an insert: connection, TLS, upload and the response",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Required,
        },
    }],
    driver_limits: &[],
};

impl EmitterDeclaration for ClickHouseTableEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::ClickHouse),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: ClickHouseTablePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TABLE",
            name: LocatorName::External(ClickHouseTableName::DECLARATION),
        }),
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: CLICKHOUSE_BOUNDS,
    };
}

impl EmitterEntityBody for ClickHouseTableEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::ClickHouse,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }

    fn mapped_value_types(&self) -> Option<crate::MappedValueTypes> {
        Some(crate::MappedValueTypes::Columns)
    }

    fn value_mappings(&self) -> EmitterValueMappings<'_> {
        EmitterValueMappings::values(&self.values)
    }
}

connector_emitters! {
    connector ClickHouse = "CLICKHOUSE";
    /// What a ClickHouse emitter writes.
    pub enum ClickHouseEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum ClickHouseTableEmitterMode;
    /// The entities a ClickHouse emitter writes.
    pub enum ClickHouseEmitterEntity {
        Table(ClickHouseTableEmitter, ClickHouseTablePublishingMode) = "TABLE";
    }
}

super::publishing::emitter_modes! {
    pub enum ClickHouseTablePublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving external table identifier this connector publishes to.
    pub struct ClickHouseTableName => clickhouse_table_name,
        label "clickhouse_table", example "sales.Orders";
}

fn clickhouse_table_name(text: &str) -> Result<(), super::external_name::ExternalNameError> {
    use super::external_name::ExternalNameError;

    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "ClickHouse table locators name a table or database.table"
        )
    )]
    const MAX_TABLE_LEVELS: usize = 2;

    super::external_name::external_identifier(text)?;
    let mut levels = 0;
    for level in text.split('.') {
        if level.is_empty() {
            return Err(ExternalNameError::EmptyLevel { separator: '.' });
        }
        levels += 1;
    }
    if levels > MAX_TABLE_LEVELS {
        return Err(ExternalNameError::TooManyLevels {
            max: MAX_TABLE_LEVELS,
            actual: levels,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    #[test]
    fn client_declares_positive_connection_intervals() {
        let entries = vec![
            ClientConfigEntry {
                key: "addr".into(),
                value: "http://localhost:8123".into(),
            },
            ClientConfigEntry {
                key: "timeout_ms".into(),
                value: "5000".into(),
            },
            ClientConfigEntry {
                key: "tcp_keepalive_ms".into(),
                value: "30000".into(),
            },
            ClientConfigEntry {
                key: "pool_idle_timeout_ms".into(),
                value: "250".into(),
            },
        ];
        CLIENT_CONFIG
            .validate_rendered(&entries)
            .assured("the declared connection intervals configure a ClickHouse client");
        for name in ["tcp_keepalive_ms", "pool_idle_timeout_ms"] {
            for invalid in ["0", "invalid", "-1", "1.5", "18446744073709551615"] {
                let mut invalid_entries = entries.clone();
                let entry = invalid_entries
                    .iter_mut()
                    .find(|entry| entry.key == name)
                    .assured("the fixture contains both connection intervals");
                entry.value = invalid.to_string();
                let refusal = CLIENT_CONFIG
                    .validate_rendered(&invalid_entries)
                    .expect_err("connection intervals must be positive schedulable milliseconds");
                assert!(
                    matches!(refusal.current_context(),
                    ClientConfigSchemaError::InvalidValue { key, .. } if key == name),
                    "{name}={invalid}: {refusal:#}"
                );
            }
        }
    }

    #[test]
    fn table_name_keeps_one_or_two_nonempty_case_sensitive_levels() {
        for name in ["Orders", "sales.Orders", "A-1.B_2"] {
            assert_eq!(ClickHouseTableName::parse(name).unwrap().as_str(), name);
        }
        for name in ["", ".", ".Orders", "sales.", "sales..Orders", "a.b.c"] {
            assert!(ClickHouseTableName::parse(name).is_err(), "{name:?}");
        }
    }

    #[test]
    fn client_requires_a_positive_timeout_and_trust_for_https() {
        let entries = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(key, value)| crate::ClientConfigEntry {
                    key: (*key).to_string(),
                    value: (*value).to_string(),
                })
                .collect::<Vec<_>>()
        };
        let missing =
            CLIENT_CONFIG.validate_rendered(&entries(&[("addr", "http://localhost:8123")]));
        assert!(matches!(
            missing.unwrap_err().current_context(),
            crate::ClientConfigSchemaError::MissingRequired { key: "timeout_ms" }
        ));
        let zero = CLIENT_CONFIG.validate_rendered(&entries(&[
            ("addr", "http://localhost:8123"),
            ("timeout_ms", "0"),
        ]));
        assert!(matches!(
            zero.unwrap_err().current_context(),
            crate::ClientConfigSchemaError::InvalidValue { key, .. } if key == "timeout_ms"
        ));
        let no_trust = CLIENT_CONFIG.validate_rendered(&entries(&[
            ("addr", "https://localhost:8123"),
            ("timeout_ms", "100"),
        ]));
        assert!(matches!(
            no_trust.unwrap_err().current_context(),
            crate::ClientConfigSchemaError::RequiresKey {
                key: "addr",
                required: "tls_ca_file"
            }
        ));
        for key in ["tls_ca_file", "tls_cert_file", "tls_key_file"] {
            let config = entries(&[
                ("addr", "http://localhost:8123"),
                ("timeout_ms", "100"),
                (key, "/tmp/tls.pem"),
            ]);
            let error = validate_clickhouse_client_config(&config).unwrap_err();
            assert!(matches!(
                error.current_context(),
                crate::ClientConfigSchemaError::InvalidValue { key: actual, .. } if actual == key
            ));
        }
        assert!(
            validate_clickhouse_client_config(&entries(&[
                ("addr", "https://localhost:8123"),
                ("timeout_ms", "100"),
                ("tls_ca_file", "/tmp/ca.pem"),
            ]))
            .is_ok()
        );
    }

    #[test]
    fn bolero_clickhouse_tls_pairing_matches_the_transport_contract() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(32)
            .with_type::<[u8; 16]>()
            .for_each(|bytes| {
                let hosts = ["localhost", "127.0.0.1", "[::1]", "warehouse.example.test"];
                let host = hosts[usize::from(bytes[0]) % hosts.len()];
                let port = u16::from_le_bytes([bytes[1], bytes[2]]).max(1);
                let timeout = u32::from(u16::from_le_bytes([bytes[3], bytes[4]])) + 1;
                for scheme in ["http", "https"] {
                    for mask in 0_u8..8 {
                        let trust = mask & 1 != 0;
                        let certificate = mask & 2 != 0;
                        let private_key = mask & 4 != 0;
                        let mut entries = vec![
                            ClientConfigEntry {
                                key: "addr".into(),
                                value: format!("{scheme}://{host}:{port}/warehouse/{}", bytes[5]),
                            },
                            ClientConfigEntry {
                                key: "timeout_ms".into(),
                                value: timeout.to_string(),
                            },
                        ];
                        for (present, key) in [
                            (trust, "tls_ca_file"),
                            (certificate, "tls_cert_file"),
                            (private_key, "tls_key_file"),
                        ] {
                            if present {
                                entries.push(ClientConfigEntry {
                                    key: key.into(),
                                    value: format!("/mounted trust/{}/{key}.pem", bytes[6]),
                                });
                            }
                        }
                        let offset = usize::from(bytes[7]) % entries.len();
                        entries.rotate_left(offset);
                        if bytes[8] & 1 != 0 {
                            entries.reverse();
                        }

                        let result = validate_clickhouse_client_config(&entries);
                        let accepted = match scheme {
                            "http" => mask == 0,
                            _ => trust && certificate == private_key,
                        };
                        assert_eq!(result.is_ok(), accepted, "{scheme}, TLS mask {mask}");
                        if let Err(report) = result {
                            let expected = if scheme == "http" {
                                let key = match mask {
                                    1 | 3 | 5 | 7 => "tls_ca_file",
                                    2 | 6 => "tls_cert_file",
                                    _ => "tls_key_file",
                                };
                                matches!(report.current_context(), ClientConfigSchemaError::InvalidValue { key: actual, .. } if actual == key)
                            } else {
                                let (key, required) = match (certificate, private_key) {
                                    (true, false) => ("tls_cert_file", "tls_key_file"),
                                    (false, true) => ("tls_key_file", "tls_cert_file"),
                                    _ => ("addr", "tls_ca_file"),
                                };
                                matches!(report.current_context(), ClientConfigSchemaError::RequiresKey { key: actual, required: actual_required } if *actual == key && *actual_required == required)
                            };
                            assert!(expected, "{scheme}, TLS mask {mask}: {report:#}");
                        }
                    }
                }
            });
    }
}
