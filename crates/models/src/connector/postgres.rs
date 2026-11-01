//! The PostgreSQL connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The PostgreSQL client Model with its connection pool, the keys that set how the pool
//!   treats a connection and the TLS policy its address selects, the table emitter Model with its
//!   table rule and conflict action, the encoding each mapped value binds with, and what it
//!   declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The PostgreSQL driver, or how rows are inserted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{collections::BTreeSet, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::VariantNames as _;
use url::Url;

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeadlineKey, DeclaredBounds, DeclaredBuffer,
        DeclaredWait, OmittedDeadline, WaitBound,
    },
    config_schema::{DriverUrl, OwnedQueryOption},
    configured_client::configured_client,
    entity_union::connector_emitters,
};
use crate::{
    ByteSizeUnit, ClientConfigSchemaError, ClientName, ConnectorKind, EmitterBatchLegality,
    EmitterBodyForm, EmitterClientReference, EmitterDeclaration, EmitterEntityBody,
    EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind, EmitterValueMapping,
    EmitterValueMappings, LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration,
    OptionForm, OptionPresence, OptionValue, ParseAsType, PayloadSizeLimit,
};

configured_client!(Postgres: pooled, config CLIENT_CONFIG, CreateClientPostgres => PostgresConfigEntry);

/// The client key that holds the connection URL.
const ADDRESS: &str = "addr";

/// How long one pool checkout may take, in milliseconds, where a client leaves
/// `pool_acquire_timeout_ms` out. The client schema holds this default as `CONFIG` text and the
/// declared wait holds it as a duration, so both expand this one literal.
macro_rules! default_pool_acquire_timeout_ms {
    () => {
        30000
    };
}

/// The current keys this connector reads, shared by validation, completion and inspection.
///
/// Nervix owns the address's scheme and its `sslmode`; the driver reads every other query option,
/// and its offline parser checks them before a client is committed. TLS files configure only the
/// verifying policy. The pool keys belong to Nervix because the right values depend on the database
/// and whatever stands in front of it, which differ per client.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    const OPTION: &str = PostgresSslMode::QUERY_OPTION.name;
    const VERIFY_FULL: &str = PostgresSslMode::VERIFY_FULL;
    ClientConfigSchema::new(
        &[
            K::required(
                ADDRESS,
                V::DriverUrl(DriverUrl {
                    schemes: &["postgres", "postgresql"],
                    owned: &[PostgresSslMode::QUERY_OPTION],
                }),
            ),
            K::defaulted(
                "pool_acquire_timeout_ms",
                V::Milliseconds,
                concat!(default_pool_acquire_timeout_ms!()),
            ),
            K::defaulted("pool_idle_timeout_ms", V::Milliseconds, "600000"),
            K::defaulted("pool_max_lifetime_ms", V::Milliseconds, "1800000"),
            K::defaulted("pool_test_before_acquire", V::Boolean, "true"),
            K::TLS_CA.only_for_query(ADDRESS, OPTION, VERIFY_FULL),
            K::TLS_CERT.only_for_query(ADDRESS, OPTION, VERIFY_FULL),
            K::TLS_KEY.only_for_query(ADDRESS, OPTION, VERIFY_FULL),
        ],
        &[],
        &[],
    )
};

/// The TLS policy a Postgres address selects with its `sslmode` query option, which Nervix owns:
/// there is no opportunistic fallback and no encrypted connection without peer verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames)]
#[strum(serialize_all = "kebab-case")]
pub enum PostgresSslMode {
    /// An unencrypted connection.
    Disable,
    /// TLS with certificate-chain and hostname verification, the one policy TLS files configure.
    VerifyFull,
}

impl PostgresSslMode {
    /// The `sslmode` query option, which the driver also reads as `ssl-mode`.
    pub const QUERY_OPTION: OwnedQueryOption = OwnedQueryOption {
        name: "sslmode",
        aliases: &["ssl-mode"],
        choices: Self::VARIANTS,
    };

    /// How the option spells the verifying policy, which the client schema holds TLS files to.
    /// The schema's constructor refuses at compile time a spelling the option does not offer.
    pub const VERIFY_FULL: &'static str = "verify-full";

    /// The policy `address` selects, read through the declared option as validation reads it.
    pub fn of_address(address: &Url) -> Result<Self, Report<ClientConfigSchemaError>> {
        let spelling = Self::QUERY_OPTION.value_in(ADDRESS, address)?;
        let mode = spelling
            .parse()
            .assured("the option offers exactly this policy's variant names");
        Ok(mode)
    }
}

pub type PostgresValueMapping = EmitterValueMapping;

/// One table a PostgreSQL client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PostgresTableEmitter {
    pub mode: PostgresTablePublishingMode,
    pub client: ClientName,
    pub table: PostgresTableName,
    pub values: Vec<PostgresValueMapping>,
    pub conflict_action: PostgresConflictAction,
}

/// What an insert does with a row whose key the table already holds.
///
/// A conflict target names the columns of the unique index the conflict is checked on, in written
/// order; the columns need not be mapped. An empty target is how `DO NOTHING` writes none, which
/// then skips a row that conflicts on any unique constraint.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PostgresConflictAction {
    /// An ordinary insert, with the server's unique-violation outcome.
    None,
    /// Skip a conflicting row.
    DoNothing { target: Vec<String> },
    /// Update every mapped column outside the target from the conflicting row.
    DoUpdate { target: Vec<String> },
}

/// Why an `ON CONFLICT` clause cannot be written as one Postgres insert.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "conflict-clause validation returns pure declaration refusals"
    )
)]
pub enum PostgresConflictError {
    #[error("ON CONFLICT DO UPDATE requires a conflict target")]
    MissingTarget,
    #[error("ON CONFLICT target holds an empty column name")]
    EmptyColumn,
    #[error("ON CONFLICT DO UPDATE requires a VALUES column outside its conflict target")]
    NothingToUpdate,
}

impl PostgresConflictAction {
    /// Checks this clause against the columns its insert maps: every target column has a name,
    /// and `DO UPDATE` names its target and leaves a mapped column outside it to update.
    pub fn validate(&self, values: &[PostgresValueMapping]) -> Result<(), PostgresConflictError> {
        match self {
            Self::None => Ok(()),
            Self::DoNothing { target } => Self::name_every_column(target),
            Self::DoUpdate { target } => {
                Self::name_every_column(target)?;
                if target.is_empty() {
                    return Err(PostgresConflictError::MissingTarget);
                }
                let target: BTreeSet<&str> = target.iter().map(String::as_str).collect();
                let updates_a_column = values
                    .iter()
                    .any(|mapping| !target.contains(mapping.column.as_str()));
                if !updates_a_column {
                    return Err(PostgresConflictError::NothingToUpdate);
                }
                Ok(())
            }
        }
    }

    /// A conflict target names each of its columns.
    fn name_every_column(target: &[String]) -> Result<(), PostgresConflictError> {
        if target.iter().any(String::is_empty) {
            return Err(PostgresConflictError::EmptyColumn);
        }
        Ok(())
    }
}

/// The longest protocol message the Postgres server reads, `PQ_LARGE_MESSAGE_LIMIT`, and so the
/// largest `BATCH MAX SIZE` an insert may measure. A Bind message above it closes the connection.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "PostgreSQL's PQ_LARGE_MESSAGE_LIMIT, MaxAllocSize - 1, bounds every protocol \
                  message the server reads"
    )
)]
pub const POSTGRES_MESSAGE_SIZE_MAXIMUM: PayloadSizeLimit =
    match PayloadSizeLimit::new(nonzero_ext::nonzero!(1_073_741_822_u64), ByteSizeUnit::B) {
        Some(maximum) => maximum,
        None => panic!("1,073,741,822 bytes fit in a 64-bit byte count"),
    };

/// How an insert binds a mapped value: as text inside the one text array its column binds, which
/// the statement then casts to the column's declared type.
///
/// Every current Plexus scalar and recursively typed `ARRAY` or `VEC` has one; nullability is
/// separate, and a null binds as a NULL element. The registry refuses at `CREATE` a mapping
/// whose inferred type is outside [`crate::MappedValueType::Column`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum PostgresValueEncoding {
    #[strum(serialize = "text true or false")]
    BooleanText,
    #[strum(serialize = "decimal integer text, through the full U64 range")]
    DecimalInteger,
    #[strum(
        serialize = "the shortest decimal text that reads back as the same F64, an F32 widened \
                     first; a non-finite value binds NULL"
    )]
    DecimalFloat,
    #[strum(serialize = "UTF-8 text")]
    Text,
    #[strum(serialize = "bytea hex text: \\x, then two hex digits per octet")]
    ByteaHex,
    #[strum(serialize = "RFC 3339 timestamp text at UTC offset +00:00")]
    TimestampText,
    #[strum(
        serialize = "recursive JSON text; bytes use padded base64 and non-finite floats use null"
    )]
    JsonText,
}

impl PostgresValueEncoding {
    /// The binding of a declared type; every current Plexus type has one.
    pub fn for_type(ty: &ParseAsType) -> Self {
        match ty {
            ParseAsType::Bool => Self::BooleanText,
            ParseAsType::U8
            | ParseAsType::I8
            | ParseAsType::U16
            | ParseAsType::I16
            | ParseAsType::U32
            | ParseAsType::I32
            | ParseAsType::U64
            | ParseAsType::I64 => Self::DecimalInteger,
            ParseAsType::F32 | ParseAsType::F64 => Self::DecimalFloat,
            ParseAsType::String => Self::Text,
            ParseAsType::Bytes => Self::ByteaHex,
            ParseAsType::Datetime => Self::TimestampText,
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => Self::JsonText,
        }
    }
}

/// What the connector of a POSTGRES TABLE holds and how long it waits.
const POSTGRES_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "connections the pool holds",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("POOL SIZE MAX"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "a pool checkout: the wait for the pool, name resolution, connection, TLS, \
                   authentication and the borrow test",
            bound: WaitBound::Key {
                key: DeadlineKey::PoolAcquireTimeout,
                omitted: OmittedDeadline::Default(Duration::from_millis(
                    default_pool_acquire_timeout_ms!(),
                )),
            },
        },
        DeclaredWait {
            wait: "an insert and its metadata query",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &[],
};

impl EmitterDeclaration for PostgresTableEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::Postgres),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required {
            maximum: Some(POSTGRES_MESSAGE_SIZE_MAXIMUM),
        },
        requires_batch_transformation: false,
        publishing_modes: PostgresTablePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TABLE",
            name: LocatorName::External(PostgresTableName::DECLARATION),
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
                    // The conflict target, `('<column>', ...)`, which `DO UPDATE` requires.
                    value: Some(OptionValue::Clause),
                    example: "ON CONFLICT ('id') DO UPDATE",
                },
            ]),
        }],
        envelope: &[],
        properties: &[],
        bounds: POSTGRES_BOUNDS,
    };
}

impl EmitterEntityBody for PostgresTableEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Postgres,
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
    connector Postgres = "POSTGRES";
    /// What a PostgreSQL emitter writes.
    pub enum PostgresEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum PostgresTableEmitterMode;
    /// The entities a PostgreSQL emitter writes.
    pub enum PostgresEmitterEntity {
        Table(PostgresTableEmitter, PostgresTablePublishingMode) = "TABLE";
    }
}

super::publishing::emitter_modes! {
    pub enum PostgresTablePublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving table, or schema and table, this connector inserts into.
    pub struct PostgresTableName => super::external_name::qualified_table,
        label "postgres_table", example "sales.Orders";
}

impl PostgresTableName {
    /// The schema and table parts, exactly as written, each of which an insert and its catalog
    /// lookup send as one quoted identifier.
    pub fn parts(&self) -> impl Iterator<Item = &str> {
        self.as_str().split('.')
    }
}

#[cfg(test)]
mod tests;
