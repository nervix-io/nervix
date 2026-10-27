//! The MySQL connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The MySQL client Model with its connection pool, the table emitter Model with its
//!   conflict action, and what it declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The MySQL driver, or how rows are inserted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeclaredBounds, DeclaredBuffer, DeclaredWait,
        WaitBound,
    },
    configured_client::configured_client,
    entity_union::connector_emitters,
};
use crate::{
    ClientName, ConnectorKind, EmitterBatchLegality, EmitterBodyForm, EmitterClientReference,
    EmitterDeclaration, EmitterEntityBody, EmitterEntityCapabilities, EmitterReference,
    EmitterReferenceKind, EmitterValueMapping, EmitterValueMappings, LocatorDeclaration,
    LocatorName, OptionChoice, OptionDeclaration, OptionForm, OptionPresence, ParseAsType,
};

configured_client!(MySql: pooled, config CLIENT_CONFIG, CreateClientMySql => MySqlConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[K::required("addr", V::DriverAddress), K::TLS_CA],
        &[],
        &[],
    )
};

pub type MySqlValueMapping = EmitterValueMapping;

/// One table a MySQL client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MySqlTableEmitter {
    pub mode: MySqlTablePublishingMode,
    pub client: ClientName,
    pub table: MySqlTableName,
    pub values: Vec<MySqlValueMapping>,
    pub conflict_action: MySqlConflictAction,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MySqlConflictAction {
    /// An ordinary insert, with the server's duplicate-key outcome.
    None,
    /// A duplicate-key update assigning the first mapped column to itself, including triggers.
    DoNothing,
    /// A duplicate-key update of every mapped column, including mapped primary/unique keys.
    DoUpdate,
}

/// The two-byte prepared-statement parameter count bounds every MySQL insert.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "MySQL COM_STMT_PREPARE_OK encodes num_params in two bytes"
    )
)]
pub const MYSQL_MAX_PLACEHOLDERS: usize = 65_535;

/// The binary-protocol representation of a declared direct-value type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum MySqlValueEncoding {
    #[strum(serialize = "integer 0 or 1")]
    BooleanInteger,
    #[strum(serialize = "signed 64-bit integer")]
    SignedInteger,
    #[strum(serialize = "signed integer through i64::MAX, unsigned integer above it")]
    UnsignedInteger,
    #[strum(serialize = "double, including non-finite values")]
    Double,
    #[strum(serialize = "UTF-8 bytes")]
    Utf8Bytes,
    #[strum(serialize = "original octets")]
    Bytes,
    #[strum(serialize = "RFC 3339 timestamp bytes")]
    TimestampBytes,
    #[strum(
        serialize = "recursive JSON text; bytes use padded base64 and non-finite floats use null"
    )]
    JsonText,
}

impl MySqlValueEncoding {
    /// Every current Plexus scalar or collection has an explicit binding; nullability is separate.
    pub fn for_type(ty: &ParseAsType) -> Self {
        match ty {
            ParseAsType::Bool => Self::BooleanInteger,
            ParseAsType::U8
            | ParseAsType::I8
            | ParseAsType::U16
            | ParseAsType::I16
            | ParseAsType::U32
            | ParseAsType::I32
            | ParseAsType::I64 => Self::SignedInteger,
            ParseAsType::U64 => Self::UnsignedInteger,
            ParseAsType::F32 | ParseAsType::F64 => Self::Double,
            ParseAsType::String => Self::Utf8Bytes,
            ParseAsType::Bytes => Self::Bytes,
            ParseAsType::Datetime => Self::TimestampBytes,
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => Self::JsonText,
        }
    }
}

/// What the connector of a MYSQL TABLE holds and how long it waits.
const MYSQL_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "connections the pool holds",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("POOL SIZE MAX"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[DeclaredWait {
        wait: "connection, its handshake and an insert",
        bound: WaitBound::Unbounded,
    }],
    driver_limits: &[
        "The driver has no connect, read or write timeout.",
        "A prepared insert binds at most 65,535 placeholders; mapped column count narrows its row \
         limit.",
    ],
};

impl EmitterDeclaration for MySqlTableEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::MySql),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: MySqlTablePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TABLE",
            name: LocatorName::External(MySqlTableName::DECLARATION),
        }),
        options: &[OptionDeclaration {
            keywords: "ON CONFLICT",
            presence: OptionPresence::Optional { default: None },
            form: OptionForm::Choice(&[
                OptionChoice {
                    name: "DO NOTHING",
                    value: None,
                    example: "ON CONFLICT DO NOTHING",
                },
                OptionChoice {
                    name: "DO UPDATE",
                    value: None,
                    example: "ON CONFLICT DO UPDATE",
                },
            ]),
        }],
        envelope: &[],
        properties: &[],
        bounds: MYSQL_BOUNDS,
    };
}

impl EmitterEntityBody for MySqlTableEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::MySql,
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
    connector MySql = "MYSQL";
    /// What a MySQL emitter writes.
    pub enum MySqlEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum MySqlTableEmitterMode;
    /// The entities a MySQL emitter writes.
    pub enum MySqlEmitterEntity {
        Table(MySqlTableEmitter, MySqlTablePublishingMode) = "TABLE";
    }
}

super::publishing::emitter_modes! {
    pub enum MySqlTablePublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving external table identifier this connector publishes to.
    pub struct MySqlTableName => table_path,
        label "mysql_table", example "Orders";
}

impl MySqlTableName {
    /// The validated database/table parts, with their exact spelling, for separate SQL quoting.
    pub fn parts(&self) -> impl Iterator<Item = &str> {
        self.as_str().split('.')
    }
}

fn table_path(text: &str) -> Result<(), super::external_name::ExternalNameError> {
    use super::external_name::{ExternalNameError, external_identifier};

    external_identifier(text)?;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the Plexus MYSQL TABLE locator has at most database and table parts"
        )
    )]
    const MAX_PARTS: usize = 2;
    // The shared lexical rule bounds the complete path to 128 bytes.
    if text.split('.').any(str::is_empty) {
        return Err(ExternalNameError::EmptyLevel { separator: '.' });
    }
    let levels = text.split('.').count();
    if levels > MAX_PARTS {
        return Err(ExternalNameError::TooManyLevels {
            max: MAX_PARTS,
            actual: levels,
        });
    }
    Ok(())
}
