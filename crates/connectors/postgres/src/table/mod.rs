//! Postgres emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The node's shared Postgres pool, the policy its client's keys declare for it and its
//!   connection options, the one borrowed connection each insert runs on together with the write's
//!   metadata lookup and the row-by-row isolation of a refused insert, the declared type of each
//!   mapped column, the `unnest` insert every write becomes under the emitter's `BATCH` limits and
//!   the size of one protocol message, its text parameters and their exact encoded size, the
//!   `ON CONFLICT` clause of a conflict action, statement deadlines and SQLSTATE classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `sqlx` driver.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

use std::{num::NonZeroU64, ops::Range, time::Duration};

use ahash::HashMap;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::DateTime;
use error_stack::{Context, Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    ClientConfigError, ConfiguredDeadline, EmitterFinish, EmitterFinishResult, EmitterHost,
    EmitterLifecycle, EmitterOpenError, EmitterRecordPosition, EmitterStartResult,
    EmitterWriteError, EmitterWriteResult, MappedEmitterMember, MappedEmitterRows, MeasuredRequest,
    PerRecordOutcome, RejectedEmitterMember, RowEmitter, RowRequest, RowRequestLimits,
    client_tls_paths, optional_bool_client_config_value, optional_client_config_value,
};
use nervix_models::{
    ClientConfigEntry, ClientDeadline, ClientPoolBounds, ConnectorKind, EmitterBatchPolicy,
    POSTGRES_MESSAGE_SIZE_MAXIMUM, PostgresSslMode, PostgresTableName,
};
use nervix_primitives::time::Instant;
use sqlx::{
    AssertSqlSafe, Executor as _, Row as _,
    pool::PoolConnection,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode, Postgres},
};
use tracing::{debug, trace};
use url::Url;

mod statement_deadline;

use statement_deadline::PostgresStatementDeadline;

const POSTGRES: &str = "postgres";

/// What `MAX SIZE` measures on a Postgres write, which an oversized row's rejection names.
const MEASURED_REQUEST: &str = "Postgres insert";

/// The longest protocol message the Postgres server reads, which the vocabulary declares as the
/// largest `BATCH MAX SIZE` a Postgres emitter accepts. A Bind message above it closes the
/// connection.
///
/// A write's measured size — its statement text and the arrays it binds — is never smaller than
/// the Bind message that carries those arrays: the statement names every column several times,
/// while the message adds a few bytes per parameter around them. Keeping the measured size within
/// this limit therefore keeps every message within it, and every bound array within the 1 GB a
/// single value may hold.
const MESSAGE_LIMIT: NonZeroU64 = POSTGRES_MESSAGE_SIZE_MAXIMUM.bytes();

/// The binary header of a one-dimensional array parameter: its dimension count, flags, element
/// type, length and lower bound, four bytes each.
const ARRAY_HEADER_BYTES: u64 = 20;

/// The length word before every element of an array parameter, which alone encodes a NULL.
const ELEMENT_LENGTH_BYTES: u64 = 4;

/// The node's shared Postgres pool for one named client.
pub struct PostgresPool {
    connections: sqlx::postgres::PgPool,
    /// The deadline each checkout keeps, which one that runs out of it names.
    checkout: ConfiguredDeadline,
    statement: Option<PostgresStatementDeadline>,
}

/// One connection borrowed from the shared pool, returned to it when this is dropped.
pub struct PostgresConnection {
    connection: PoolConnection<Postgres>,
    statement: Option<PostgresStatementDeadline>,
}

/// Driver options and the statement bound validated together before opening the pool.
pub(crate) struct PostgresConnectionOptions {
    driver: PgConnectOptions,
    statement: Option<PostgresStatementDeadline>,
}

/// The node-owned pool this emitter borrows connections from while its host holds the lease.
#[async_trait]
pub trait PostgresConnections: Send + Sync + 'static {
    /// Borrow one connection for one operation, with the host's pool wait recorded around it.
    async fn connection(&self) -> EmitterWriteResult<PostgresConnection>;
}

/// What one Postgres emitter inserts with, from its typed emitter plan.
pub struct PostgresTableEmitterConfig {
    pub table: PostgresTableName,
    pub conflict_action: PostgresConflictAction,
    /// The emitter's `BATCH` limits, which bound the rows and the measured bytes of every insert.
    pub batch: EmitterBatchPolicy,
}

/// What an insert does with a row the target table already holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

/// The Postgres emitter, which binds each mapped column as one text array of an `unnest` insert.
pub struct PostgresTableEmitter {
    connections: Box<dyn PostgresConnections>,
    table: PostgresTableName,
    conflict_action: PostgresConflictAction,
    limits: RowRequestLimits,
}

#[derive(Debug, thiserror::Error)]
enum PostgresWriteError {
    #[error("failed to load Postgres table metadata")]
    Metadata(#[source] sqlx::Error),
    #[error("Postgres table '{table}' has no column '{column}'")]
    MissingColumn { table: String, column: String },
    #[error("Postgres ON CONFLICT DO UPDATE requires a conflict target")]
    MissingConflictTarget,
    #[error(
        "Postgres ON CONFLICT DO UPDATE requires at least one non-conflict VALUES column to update"
    )]
    MissingUpdateColumn,
    #[error("Postgres ON CONFLICT target columns must not be empty")]
    EmptyConflictColumn,
    #[error("Postgres insert failed with SQLSTATE {code:?}")]
    Server { code: Option<String> },
    #[error("Postgres insert failed")]
    Execute(#[source] sqlx::Error),
    /// The pool handed no connection over; what it reported is beneath this context.
    #[error("failed to borrow a Postgres connection")]
    Pool,
    #[error("{} statement response deadline expired", ConnectorKind::Postgres)]
    Deadline,
    #[error(
        "{} statement canceled with SQLSTATE 57014 ({deadline} configured)",
        ConnectorKind::Postgres
    )]
    StatementCanceled { deadline: PostgresStatementDeadline },
}

/// A SQLSTATE the server reports for a row, or for a set of rows, it will never accept.
///
/// Data exceptions and integrity violations name a row the table refuses. A cardinality violation
/// is how `ON CONFLICT DO UPDATE` refuses one insert whose rows share a conflict key; the same rows
/// inserted one at a time each update what the one before them wrote.
fn is_record_sqlstate(code: &str) -> bool {
    code.starts_with("21") || code.starts_with("22") || code.starts_with("23")
}

impl PostgresWriteError {
    fn report_cancellation(
        error: &sqlx::Error,
        deadline: Option<PostgresStatementDeadline>,
    ) -> Option<Report<Self>> {
        let deadline = deadline?;
        let database = error.as_database_error()?;
        let code = database.code()?;
        if code == "57014" {
            return Some(Report::new(Self::StatementCanceled { deadline }));
        }
        None
    }

    fn report_metadata(
        error: sqlx::Error,
        deadline: Option<PostgresStatementDeadline>,
    ) -> Report<Self> {
        if let Some(canceled) = Self::report_cancellation(&error, deadline) {
            return canceled;
        }
        Report::new(Self::Metadata(error))
    }

    fn report_execute(
        error: sqlx::Error,
        deadline: Option<PostgresStatementDeadline>,
    ) -> Report<Self> {
        if let Some(canceled) = Self::report_cancellation(&error, deadline) {
            return canceled;
        }
        match error {
            // A database response can quote rejected values. Retain its safe SQLSTATE only.
            sqlx::Error::Database(error) => Report::new(Self::Server {
                code: error.code().map(|code| code.into_owned()),
            }),
            error => Report::new(Self::Execute(error)),
        }
    }

    fn is_record_error(&self) -> bool {
        let Self::Server { code: Some(code) } = self else {
            return false;
        };
        is_record_sqlstate(code)
    }

    fn record_reason(&self) -> String {
        let code = match self {
            Self::Server { code } => code.as_deref(),
            _ => None,
        };
        match code {
            Some(code) => format!("Postgres rejected record with SQLSTATE {code}"),
            None => "Postgres rejected record".to_string(),
        }
    }

    fn into_report(report: Report<Self>) -> Report<EmitterWriteError> {
        let reason = match report.current_context() {
            Self::Server { code } => {
                let code = code.as_deref().unwrap_or("unknown");
                Some(format!("Postgres request failed with SQLSTATE {code}"))
            }
            Self::Execute(_) => Some("Postgres request failed with SQLSTATE unknown".to_string()),
            Self::Metadata(error) => {
                Some(format!("failed to load Postgres table metadata: {error}"))
            }
            // The pool describes its own failure beneath this context.
            Self::Pool | Self::Deadline => None,
            error => Some(error.to_string()),
        };
        let report = report.change_context(EmitterWriteError::Publish {
            connector: POSTGRES,
        });
        match reason {
            Some(reason) => report.attach_printable(reason),
            None => report,
        }
    }
}

/// How the pool treats its connections, as the client's effective configuration declares it: an
/// omitted key takes the default the client schema declares for it.
///
/// SQLx closes a connection idle for `idle`, the ones it keeps for the declared minimum included,
/// and one that has lived `maximum` when it is returned or while it is idle, then opens connections
/// again to keep the minimum. It checks its idle connections as often as the shorter lifetime.
#[derive(Debug, Clone, Copy)]
struct PostgresPoolPolicy {
    /// `pool_acquire_timeout_ms`: how long one checkout may take, the wait for the pool, opening a
    /// connection and testing one included. Independent of how long an accepted query then runs.
    checkout: ConfiguredDeadline,
    /// `pool_idle_timeout_ms`: how long a connection may sit idle in the pool.
    idle: Duration,
    /// `pool_max_lifetime_ms`: how old a connection may become.
    maximum: Duration,
    /// `pool_test_before_acquire`: whether a checkout tests an idle connection before handing it
    /// over.
    test_before_acquire: bool,
}

impl PostgresPoolPolicy {
    /// The policy `config` declares, each value held to its rule again before the pool opens.
    fn read(config: &[ClientConfigEntry]) -> EmitterStartResult<Self> {
        let invalid = || EmitterOpenError::InvalidConfiguration {
            connector: POSTGRES,
        };
        let effective = ConnectorKind::Postgres
            .client()
            .assured("the Postgres connector declares a configured client")
            .config
            .effective_entries(config);
        let checkout = ConfiguredDeadline::pool_acquire(&effective)
            .change_context_lazy(invalid)?
            .assured("the effective configuration supplies every defaulted key");
        let test_before_acquire =
            optional_bool_client_config_value(&effective, "pool_test_before_acquire")
                .change_context_lazy(invalid)?
                .assured("the effective configuration supplies every defaulted key");
        let idle =
            Self::lifetime(&effective, "pool_idle_timeout_ms").change_context_lazy(invalid)?;
        let maximum =
            Self::lifetime(&effective, "pool_max_lifetime_ms").change_context_lazy(invalid)?;
        Ok(Self {
            checkout,
            idle,
            maximum,
            test_before_acquire,
        })
    }

    /// The lifetime the declared key `name` sets in `effective`, held to the rule of its value.
    fn lifetime(
        effective: &[ClientConfigEntry],
        name: &'static str,
    ) -> Result<Duration, Report<ClientConfigError>> {
        let value = optional_client_config_value(effective, name)
            .assured("the effective configuration supplies every defaulted key");
        let lifetime = ClientDeadline::parse(value)
            .change_context(ClientConfigError::InvalidDeadline { key: name })?;
        Ok(lifetime.duration())
    }

    /// The driver's pool options for a client sized by `bounds`.
    fn pool_options(self, bounds: ClientPoolBounds) -> PgPoolOptions {
        PgPoolOptions::new()
            .min_connections(bounds.minimum())
            .max_connections(bounds.maximum().get())
            .acquire_timeout(self.checkout.bound())
            .idle_timeout(Some(self.idle))
            .max_lifetime(Some(self.maximum))
            .test_before_acquire(self.test_before_acquire)
    }
}

impl PostgresPool {
    /// Open the node's shared Postgres pool for one named client, sized by its declared bounds.
    ///
    /// SQLx's own pool enforces the ceiling and treats its connections as [`PostgresPoolPolicy`]
    /// describes, retiring them within the same maximum.
    pub async fn open(
        config: &[ClientConfigEntry],
        bounds: ClientPoolBounds,
    ) -> EmitterStartResult<Self> {
        let Some(addr) = optional_client_config_value(config, "addr") else {
            return Err(Report::new(EmitterOpenError::InvalidConfiguration {
                connector: POSTGRES,
            })
            .attach_printable("missing Postgres client config key 'addr'"));
        };
        let PostgresConnectionOptions { driver, statement } = connect_options(addr, config)?;
        let policy = PostgresPoolPolicy::read(config)?;
        let opened = policy.pool_options(bounds).connect_with(driver).await;
        match opened {
            Ok(connections) => Ok(Self {
                connections,
                checkout: policy.checkout,
                statement,
            }),
            Err(source) => Err(checkout_failure(
                source,
                policy.checkout,
                EmitterOpenError::Initialize {
                    connector: POSTGRES,
                },
            )),
        }
    }

    /// Borrow one connection, which returns to the pool when the borrow is dropped.
    pub async fn connection(&self) -> EmitterWriteResult<PostgresConnection> {
        let acquired = self.connections.acquire().await;
        match acquired {
            Ok(connection) => Ok(PostgresConnection {
                connection,
                statement: self.statement,
            }),
            Err(source) => Err(checkout_failure(
                source,
                self.checkout,
                EmitterWriteError::NotInitialized {
                    connector: POSTGRES,
                },
            )),
        }
    }
}

/// Why a checkout handed no connection over, beneath `context`: the deadline it ran out of, or the
/// driver's typed cause. Its description is attached too, which is how an emitter reports a failure.
fn checkout_failure<C: Context>(
    source: sqlx::Error,
    checkout: ConfiguredDeadline,
    context: C,
) -> Report<C> {
    match source {
        sqlx::Error::PoolTimedOut => {
            let elapsed = checkout.elapsed("checking out a Postgres connection");
            Report::new(elapsed)
                .change_context(context)
                .attach_printable(elapsed.to_string())
        }
        source => {
            let description = source.to_string();
            Report::new(source)
                .change_context(context)
                .attach_printable(description)
        }
    }
}

/// The connection options one Postgres client connects with.
///
/// The URL carries the endpoint, the user and the database explicitly, and selects one of two TLS
/// policies. Mounted files are the client's TLS-file interface: an opportunistic fallback and an
/// encrypted connection without peer verification are both refused rather than silently allowed.
pub(crate) fn connect_options(
    addr: &str,
    config: &[ClientConfigEntry],
) -> EmitterStartResult<PostgresConnectionOptions> {
    let invalid = |reason: String| {
        Report::new(EmitterOpenError::InvalidConfiguration {
            connector: POSTGRES,
        })
        .attach_printable(reason)
    };
    let url = Url::parse(addr).map_err(|source| invalid(format!("invalid addr: {source}")))?;
    if url.scheme() != "postgres" && url.scheme() != "postgresql" {
        return Err(invalid(format!(
            "addr must be a postgres:// or postgresql:// URL, found '{}'",
            url.scheme()
        )));
    }
    let mut options: PgConnectOptions = addr
        .parse()
        .map_err(|source| invalid(format!("invalid addr: {source}")))?;

    let ssl_mode = PostgresSslMode::of_address(&url).change_context(
        EmitterOpenError::InvalidConfiguration {
            connector: POSTGRES,
        },
    )?;
    let verify_full = ssl_mode == PostgresSslMode::VerifyFull;
    let driver_mode = match ssl_mode {
        PostgresSslMode::Disable => PgSslMode::Disable,
        PostgresSslMode::VerifyFull => PgSslMode::VerifyFull,
    };
    options = options.ssl_mode(driver_mode);

    let tls = client_tls_paths(config);
    if !tls.is_empty() && !verify_full {
        return Err(invalid(
            "TLS files require sslmode=verify-full on the client addr".to_string(),
        ));
    }
    match (&tls.cert_file, &tls.key_file) {
        (Some(cert_file), Some(key_file)) => {
            options = options.ssl_client_cert(cert_file).ssl_client_key(key_file);
        }
        (None, None) => {}
        _ => {
            return Err(invalid(
                "TLS client authentication requires both 'tls_cert_file' and 'tls_key_file'"
                    .to_string(),
            ));
        }
    }
    if let Some(ca_file) = &tls.ca_file {
        options = options.ssl_root_cert(ca_file);
    }
    let statement = PostgresStatementDeadline::read(config).change_context(
        EmitterOpenError::InvalidConfiguration {
            connector: POSTGRES,
        },
    )?;
    if let Some(deadline) = statement {
        options = deadline.configure(options);
    }
    Ok(PostgresConnectionOptions {
        driver: options,
        statement,
    })
}

impl PostgresTableEmitter {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow access and PostgreSQL driver preparation own their effects"
        )
    )]
    pub fn new(
        config: PostgresTableEmitterConfig,
        connections: Box<dyn PostgresConnections>,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let PostgresTableEmitterConfig {
            table,
            conflict_action,
            batch,
        } = config;
        Ok(Self {
            connections,
            table,
            conflict_action,
            limits: RowRequestLimits::from(batch).with_native_bytes(MESSAGE_LIMIT),
        })
    }

    fn quote_ident(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    /// The table as one SQL name, each declared part quoted as its own identifier, so the insert
    /// and its catalog lookup name the same case-preserved relation.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "visits the vocabulary's opaque iterator over at most two \
                                   immutable table parts bounded to 128 bytes total; local \
                                   quoting callbacks remain analyzed")
    )]
    fn table_sql(&self) -> String {
        let parts: Vec<String> = self.table.parts().map(Self::quote_ident).collect();
        parts.join(".")
    }

    /// One connection from the node's pool, returned to it when the borrow is dropped.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the host's pool lease hands out the borrowed connection")
    )]
    async fn borrow(&self) -> error_stack::Result<PostgresConnection, PostgresWriteError> {
        self.connections
            .connection()
            .await
            .change_context(PostgresWriteError::Pool)
    }

    /// The declared type of each mapped column, read on the connection the write's first insert
    /// then runs on.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow access and PostgreSQL driver preparation own their effects"
        )
    )]
    async fn column_types(
        &self,
        connection: &mut PostgresConnection,
        columns: &[String],
    ) -> error_stack::Result<Vec<String>, PostgresWriteError> {
        let table_name = self.table_sql();
        let column_refs = columns.to_vec();
        let statement = connection.statement;
        let reading = sqlx::query(
            "SELECT a.attname, a.atttypid::regtype::text FROM pg_attribute a WHERE a.attrelid = \
             to_regclass($1) AND a.attname = ANY($2::text[]) AND a.attnum > 0 AND NOT \
             a.attisdropped",
        )
        .bind(table_name)
        .bind(column_refs)
        .fetch_all(&mut *connection.connection);
        let response = match statement {
            Some(deadline) => match deadline.wait("the catalog query", reading).await {
                Ok(response) => response,
                Err(error) => {
                    // SQLx bounds close-on-drop by five seconds and retains the pool permit
                    // through closing. A pending response must never return to the idle pool.
                    connection.connection.close_on_drop();
                    return Err(error.change_context(PostgresWriteError::Deadline));
                }
            },
            None => reading.await,
        };
        let rows =
            response.map_err(|error| PostgresWriteError::report_metadata(error, statement))?;
        let types_by_column = rows
            .into_iter()
            .map(|row| {
                let column: String = row.get(0);
                let ty: String = row.get(1);
                (column, ty)
            })
            .collect::<HashMap<_, _>>();
        columns
            .iter()
            .map(|column| {
                types_by_column.get(column).cloned().ok_or_else(|| {
                    Report::new(PostgresWriteError::MissingColumn {
                        table: self.table.as_str().to_string(),
                        column: column.clone(),
                    })
                })
            })
            .collect()
    }

    /// The one statement every insert of a write executes: it binds one text array per mapped
    /// column, however many rows the insert carries, and casts each to the column's declared type.
    fn insert_sql(
        &self,
        columns: &[String],
        column_types: &[String],
    ) -> EmitterWriteResult<String> {
        let param_refs = (1..=columns.len())
            .map(|index| format!("${index}::text[]"))
            .collect::<Vec<_>>()
            .join(", ");
        let unnest_columns = columns
            .iter()
            .map(|column| Self::quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let select_columns = columns
            .iter()
            .zip(column_types.iter())
            .map(|(column, ty)| format!("t.{}::{}", Self::quote_ident(column), ty))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_columns = unnest_columns.clone();
        let conflict_clause = Self::conflict_clause(columns, &self.conflict_action)
            .map_err(PostgresWriteError::into_report)?;
        Ok(format!(
            "INSERT INTO {} ({insert_columns}) SELECT {select_columns} FROM unnest({param_refs}) \
             AS t({unnest_columns}){conflict_clause}",
            self.table_sql(),
        ))
    }

    /// One insert of `members` on `connection`.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow access and PostgreSQL driver preparation own their effects"
        )
    )]
    async fn insert(
        &self,
        connection: &mut PostgresConnection,
        inserts: &UnnestInserts,
        members: Range<usize>,
    ) -> error_stack::Result<u64, PostgresWriteError> {
        // Every value is a bound parameter and every identifier went through `quote_ident`, so the
        // only thing interpolated into this statement is a quoted name or a positional placeholder.
        let mut query = sqlx::query(AssertSqlSafe(inserts.sql.as_str()));
        for column in &inserts.columns {
            let texts = column
                .get(members.clone())
                .assured("an insert carries rows of the write whose texts were bound");
            query = query.bind(texts);
        }
        let statement = connection.statement;
        let execution = connection.connection.execute(query);
        let response = match statement {
            Some(deadline) => match deadline.wait("the insert", execution).await {
                Ok(response) => response,
                Err(error) => {
                    connection.connection.close_on_drop();
                    return Err(error.change_context(PostgresWriteError::Deadline));
                }
            },
            None => execution.await,
        };
        let result =
            response.map_err(|error| PostgresWriteError::report_execute(error, statement))?;
        Ok(result.rows_affected())
    }

    /// The connection a write's next insert runs on: the one `lent` still holds, which the write's
    /// first insert takes over from its metadata lookup, or a fresh borrow.
    async fn insert_connection(
        &self,
        lent: &mut Option<PostgresConnection>,
    ) -> error_stack::Result<PostgresConnection, PostgresWriteError> {
        if let Some(connection) = lent.take() {
            return Ok(connection);
        }
        self.borrow().await
    }

    /// Writes the rows `written` names in one insert on `connection`, answering for each of them,
    /// and returns the failure that ends the write when the insert fails for every row alike.
    ///
    /// When the server refuses a multi-row insert for one of its rows, each row is inserted alone
    /// on the same connection: healthy rows land, only the refused ones follow the error policy,
    /// and the write learns which rows those are without waiting on the pool again, where a shared
    /// client's other borrowers or a new connection's establishment could hold it for long.
    async fn write_insert(
        &self,
        connection: &mut PostgresConnection,
        inserts: &UnnestInserts,
        written: Range<usize>,
        rows: &MappedEmitterRows<'_>,
        members: &[MappedEmitterMember],
        outcome: &mut PerRecordOutcome<EmitterRecordPosition>,
    ) -> error_stack::Result<(), PostgresWriteError> {
        let error = match self.insert(connection, inserts, written.clone()).await {
            Ok(_) => {
                for index in written {
                    outcome.deliver(rows.position(members[index]));
                }
                return Ok(());
            }
            Err(error) => error,
        };
        if !error.current_context().is_record_error() {
            return Err(error);
        }
        if written.len() == 1 {
            let member = members[written.start];
            outcome.reject(RejectedEmitterMember::external(
                rows.position(member),
                rows.occurred_at(member),
                error.current_context().record_reason(),
            ));
            return Ok(());
        }
        for index in written {
            nervix_primitives::task::consume_budget().await;
            let member = members[index];
            let alone = index
                .checked_add(1)
                .assured("a row of the write is followed by at most its end");
            match self.insert(connection, inserts, index..alone).await {
                Ok(_) => outcome.deliver(rows.position(member)),
                Err(error) if error.current_context().is_record_error() => {
                    outcome.reject(RejectedEmitterMember::external(
                        rows.position(member),
                        rows.occurred_at(member),
                        error.current_context().record_reason(),
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn conflict_clause(
        columns: &[String],
        action: &PostgresConflictAction,
    ) -> error_stack::Result<String, PostgresWriteError> {
        match action {
            PostgresConflictAction::None => Ok(String::new()),
            PostgresConflictAction::DoNothing { target } => {
                let target = Self::conflict_target_sql(target)?;
                Ok(format!(" ON CONFLICT{target} DO NOTHING"))
            }
            PostgresConflictAction::DoUpdate { target } => {
                if target.is_empty() {
                    return Err(Report::new(PostgresWriteError::MissingConflictTarget));
                }
                let assignments = columns
                    .iter()
                    .filter(|column| !target.contains(column))
                    .map(|column| {
                        let quoted = Self::quote_ident(column);
                        format!("{quoted} = EXCLUDED.{quoted}")
                    })
                    .collect::<Vec<_>>();
                if assignments.is_empty() {
                    return Err(Report::new(PostgresWriteError::MissingUpdateColumn));
                }
                let target = Self::conflict_target_sql(target)?;
                Ok(format!(
                    " ON CONFLICT{target} DO UPDATE SET {}",
                    assignments.join(", ")
                ))
            }
        }
    }

    fn conflict_target_sql(target: &[String]) -> error_stack::Result<String, PostgresWriteError> {
        if target.is_empty() {
            Ok(String::new())
        } else if target.iter().any(|column| column.is_empty()) {
            Err(Report::new(PostgresWriteError::EmptyConflictColumn))
        } else {
            Ok(format!(
                " ({})",
                target
                    .iter()
                    .map(|column| Self::quote_ident(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    }
}

impl PostgresTableEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Postgres(nervix_models::PostgresEmitterEntity::Table);
}

#[async_trait]
impl EmitterLifecycle for PostgresTableEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each insert completes before the publish returns, so the pool holds nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RowEmitter for PostgresTableEmitter {
    /// Writes the rows of every carrier in `unnest` inserts of at most `MAX MESSAGES` rows whose
    /// statement and bound arrays measure at most `MAX SIZE` bytes.
    ///
    /// Each insert borrows one connection and returns it before the next, so other local emitters
    /// of the client get through between inserts. The metadata lookup runs on the first insert's
    /// borrow, and a refused insert isolates its rows on the connection it ran on, so no step of
    /// an insert waits on the pool once the insert has its connection.
    async fn publish(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> PerRecordOutcome<EmitterRecordPosition> {
        let members = rows.members();
        let mut outcome = PerRecordOutcome::with_capacity(members.len());
        let mut carriers = Vec::with_capacity(rows.carriers.len());
        for carrier in &rows.carriers {
            match MappedTextColumns::new(carrier.batch, rows.target_columns) {
                Ok(columns) => carriers.push(columns),
                Err(error) => {
                    outcome.fail(
                        Report::new(EmitterWriteError::Publish {
                            connector: POSTGRES,
                        })
                        .attach_printable(error.to_string()),
                    );
                    return outcome;
                }
            }
        }
        let mut connection = match self.borrow().await {
            Ok(connection) => connection,
            Err(error) => {
                outcome.fail(PostgresWriteError::into_report(error));
                return outcome;
            }
        };
        let column_types = match self
            .column_types(&mut connection, rows.target_columns)
            .await
        {
            Ok(column_types) => column_types,
            Err(error) => {
                outcome.fail(PostgresWriteError::into_report(error));
                return outcome;
            }
        };
        let mut lent = Some(connection);
        let sql = match self.insert_sql(rows.target_columns, &column_types) {
            Ok(sql) => sql,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let inserts = UnnestInserts::bind(sql, &carriers, &members, rows.target_columns.len());
        let requests = self
            .limits
            .divide(members.len(), |candidate| MeasuredRequest {
                size: inserts.measure(candidate),
                request: (),
            });
        if requests.subdivisions > 0 {
            debug!(
                table = self.table.as_str(),
                subdivisions = requests.subdivisions,
                "halved Postgres inserts that exceeded their byte limit"
            );
        }
        for request in requests.requests {
            nervix_primitives::task::consume_budget().await;
            let written = match request {
                RowRequest::Write {
                    members: written, ..
                } => written,
                RowRequest::Oversize { member, oversize } => {
                    let member = members[member];
                    outcome.reject(oversize.rejected(
                        rows.position(member),
                        rows.occurred_at(member),
                        MEASURED_REQUEST,
                    ));
                    continue;
                }
            };
            let mut connection = match self.insert_connection(&mut lent).await {
                Ok(connection) => connection,
                Err(error) => {
                    outcome.fail(PostgresWriteError::into_report(error));
                    return outcome;
                }
            };
            // The connection returns to the pool when this insert ends, before the next borrows.
            if let Err(error) = self
                .write_insert(
                    &mut connection,
                    &inserts,
                    written,
                    &rows,
                    &members,
                    &mut outcome,
                )
                .await
            {
                outcome.fail(PostgresWriteError::into_report(error));
                return outcome;
            }
        }
        trace!(
            table = self.table.as_str(),
            rows = members.len(),
            "emitter published postgres rows"
        );
        outcome
    }
}

/// The inserts of one write: the one `unnest` statement they all execute, the text every mapped
/// column binds for each of the write's rows, and the size those texts add to an insert.
struct UnnestInserts {
    sql: String,
    /// One text per row for each mapped column, in the order the write carries its rows.
    columns: Vec<Vec<Option<String>>>,
    /// The bytes the elements of the first `n + 1` rows add to the arrays an insert binds, for
    /// each `n`.
    element_ends: Vec<u64>,
}

impl UnnestInserts {
    fn bind(
        sql: String,
        carriers: &[MappedTextColumns<'_>],
        members: &[MappedEmitterMember],
        column_count: usize,
    ) -> Self {
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(Vec::with_capacity(members.len()));
        }
        let mut element_ends = Vec::with_capacity(members.len());
        let mut elements = 0_u64;
        for member in members {
            let mapped = carriers
                .get(member.carrier)
                .assured("every carrier of the write was mapped before its rows were bound");
            for (column, values) in columns.iter_mut().zip(&mapped.values) {
                let text = values.text(member.row);
                elements = elements
                    .checked_add(Self::element_bytes(text.as_deref()))
                    .assured("the texts one write binds are held in memory");
                column.push(text);
            }
            element_ends.push(elements);
        }
        Self {
            sql,
            columns,
            element_ends,
        }
    }

    /// The bytes one element adds to a text array as the extended-query protocol encodes it: a
    /// length word, then the text, or the length word alone for a NULL.
    fn element_bytes(text: Option<&str>) -> u64 {
        let text_bytes = match text {
            Some(text) => u64::try_from(text.len())
                .assured("Nervix builds for 64-bit targets only, where u64 holds usize"),
            None => 0,
        };
        ELEMENT_LENGTH_BYTES
            .checked_add(text_bytes)
            .assured("one text this node holds in memory")
    }

    /// The exact size an insert of `members` is measured at: its statement text and every array
    /// it binds, each as the extended-query protocol encodes it. The protocol messages around
    /// them, such as each parameter's own length word, are framing outside the measure.
    fn measure(&self, members: Range<usize>) -> u64 {
        let start = match members.start.checked_sub(1) {
            Some(previous) => *self
                .element_ends
                .get(previous)
                .assured("an insert starts at a row the write bound"),
            None => 0,
        };
        let last = members
            .end
            .checked_sub(1)
            .assured("an insert carries at least one row");
        let end = *self
            .element_ends
            .get(last)
            .assured("an insert ends at a row the write bound");
        let elements = end
            .checked_sub(start)
            .assured("the running size of later rows is at least that of earlier ones");
        let arrays = u64::try_from(self.columns.len())
            .assured("Nervix builds for 64-bit targets only, where u64 holds usize")
            .checked_mul(ARRAY_HEADER_BYTES)
            .assured("an insert binds one array per mapped column");
        let statement = u64::try_from(self.sql.len())
            .assured("Nervix builds for 64-bit targets only, where u64 holds usize");
        let framed = statement
            .checked_add(arrays)
            .assured("the statement and array headers of one insert are held in memory");
        framed
            .checked_add(elements)
            .assured("every term measures bytes this node holds in memory")
    }
}

/// A mapped column this emitter cannot bind, named with the exact type it carries.
#[derive(Debug, thiserror::Error)]
#[error("Postgres VALUES column '{column}' has unsupported exact type {data_type}")]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "unsupported mapped columns produce definitive semantic record rejections"
    )
)]
struct UnsupportedMappedColumn {
    column: String,
    data_type: arrow_schema::DataType,
}

/// The mapped columns of one batch, downcast once so every row binds from the column that holds it.
struct MappedTextColumns<'a> {
    values: Vec<MappedTextColumn<'a>>,
}

impl<'a> MappedTextColumns<'a> {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow access and PostgreSQL driver preparation own their effects"
        )
    )]
    fn new(
        batch: &'a RecordBatch,
        target_columns: &[String],
    ) -> Result<Self, UnsupportedMappedColumn> {
        let mut values = Vec::with_capacity(target_columns.len());
        for (index, column) in target_columns.iter().enumerate() {
            let array = batch.column(index);
            let mapped = MappedTextColumn::new(array).ok_or_else(|| UnsupportedMappedColumn {
                column: column.clone(),
                data_type: array.data_type().clone(),
            })?;
            values.push(mapped);
        }
        Ok(Self { values })
    }
}

/// One mapped column, held as the typed Arrow array it arrived in.
enum MappedTextColumn<'a> {
    Bool(&'a BooleanArray),
    U8(&'a UInt8Array),
    I8(&'a Int8Array),
    U16(&'a UInt16Array),
    I16(&'a Int16Array),
    U32(&'a UInt32Array),
    I32(&'a Int32Array),
    U64(&'a UInt64Array),
    I64(&'a Int64Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    String(&'a StringArray),
    /// Octets, bound in the hex format a `bytea` column reads back as those octets.
    Bytes(&'a BinaryArray),
    Datetime(&'a TimestampNanosecondArray),
    List {
        offsets: &'a ListArray,
        elements: Box<MappedTextColumn<'a>>,
    },
    /// A fixed-size array, such as an array literal, whose rows all hold the same element count.
    FixedList {
        list: &'a FixedSizeListArray,
        elements: Box<MappedTextColumn<'a>>,
    },
}

impl<'a> MappedTextColumn<'a> {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow access and PostgreSQL driver preparation own their effects"
        )
    )]
    fn new(array: &'a ArrayRef) -> Option<Self> {
        let array = array.as_ref();
        if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
            return Some(Self::Bool(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
            return Some(Self::U8(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
            return Some(Self::I8(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
            return Some(Self::U16(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
            return Some(Self::I16(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
            return Some(Self::U32(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
            return Some(Self::I32(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
            return Some(Self::U64(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            return Some(Self::I64(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
            return Some(Self::F32(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
            return Some(Self::F64(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            return Some(Self::String(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
            return Some(Self::Bytes(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Some(Self::Datetime(values));
        }
        if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            let elements = Self::new(list.values())?;
            return Some(Self::FixedList {
                list,
                elements: Box::new(elements),
            });
        }
        let values = array.as_any().downcast_ref::<ListArray>()?;
        let elements = Self::new(values.values())?;
        Some(Self::List {
            offsets: values,
            elements: Box::new(elements),
        })
    }

    /// The text this row binds as, which the insert casts to the column's declared type.
    fn text(&self, row: usize) -> Option<String> {
        if self.is_null(row) {
            return None;
        }
        let text = match self {
            Self::Bool(values) => values.value(row).to_string(),
            Self::U8(values) => values.value(row).to_string(),
            Self::I8(values) => values.value(row).to_string(),
            Self::U16(values) => values.value(row).to_string(),
            Self::I16(values) => values.value(row).to_string(),
            Self::U32(values) => values.value(row).to_string(),
            Self::I32(values) => values.value(row).to_string(),
            Self::U64(values) => values.value(row).to_string(),
            Self::I64(values) => values.value(row).to_string(),
            // A float binds in the shortest form that reads back as the same value, and a value
            // no decimal form represents binds as NULL rather than as a word Postgres rejects.
            Self::F32(values) => return float_text(f64::from(values.value(row))),
            Self::F64(values) => return float_text(values.value(row)),
            Self::String(values) => values.value(row).to_string(),
            Self::Bytes(values) => format!("\\x{}", faster_hex::hex_string(values.value(row))),
            Self::Datetime(values) => DateTime::from_timestamp_nanos(values.value(row))
                .fixed_offset()
                .to_rfc3339(),
            // A list binds as the JSON text of its elements, which is what a JSON column reads.
            Self::List { .. } | Self::FixedList { .. } => self.json_value(row).to_string(),
        };
        Some(text)
    }

    fn json_value(&self, row: usize) -> serde_json::Value {
        if self.is_null(row) {
            return serde_json::Value::Null;
        }
        match self {
            Self::Bool(values) => serde_json::Value::from(values.value(row)),
            Self::U8(values) => serde_json::Value::from(values.value(row)),
            Self::I8(values) => serde_json::Value::from(values.value(row)),
            Self::U16(values) => serde_json::Value::from(values.value(row)),
            Self::I16(values) => serde_json::Value::from(values.value(row)),
            Self::U32(values) => serde_json::Value::from(values.value(row)),
            Self::I32(values) => serde_json::Value::from(values.value(row)),
            Self::U64(values) => serde_json::Value::from(values.value(row)),
            Self::I64(values) => serde_json::Value::from(values.value(row)),
            Self::F32(values) => serde_json::Value::from(values.value(row)),
            Self::F64(values) => serde_json::Value::from(values.value(row)),
            Self::String(values) => serde_json::Value::from(values.value(row)),
            // Inside JSON, octets are the canonical padded base64 text every Nervix JSON value
            // carries them as.
            Self::Bytes(values) => serde_json::Value::from(BASE64.encode(values.value(row))),
            Self::Datetime(values) => serde_json::Value::from(
                DateTime::from_timestamp_nanos(values.value(row))
                    .fixed_offset()
                    .to_rfc3339(),
            ),
            Self::List { offsets, elements } => {
                let offsets = offsets.value_offsets();
                let non_negative = "Arrow builds list offsets as non-negative element positions";
                let start = usize::try_from(offsets[row]).assured(non_negative);
                let end = usize::try_from(
                    offsets[row.checked_add(1).assured(
                        "a list array holds one offset more than it holds rows, so the position \
                         after the last row is addressable",
                    )],
                )
                .assured(non_negative);
                elements.json_array(start..end)
            }
            Self::FixedList { list, elements } => {
                let non_negative = "Arrow builds fixed-size list offsets and widths as \
                                    non-negative element counts";
                let start = usize::try_from(list.value_offset(row)).assured(non_negative);
                let width = usize::try_from(list.value_length()).assured(non_negative);
                let end = start
                    .checked_add(width)
                    .assured("a fixed-size list row ends inside its element array");
                elements.json_array(start..end)
            }
        }
    }

    /// The elements `elements` of this column as one JSON array.
    fn json_array(&self, elements: Range<usize>) -> serde_json::Value {
        let mut items = Vec::with_capacity(elements.len());
        for element in elements {
            items.push(self.json_value(element));
        }
        serde_json::Value::Array(items)
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Bool(values) => values.is_null(row),
            Self::U8(values) => values.is_null(row),
            Self::I8(values) => values.is_null(row),
            Self::U16(values) => values.is_null(row),
            Self::I16(values) => values.is_null(row),
            Self::U32(values) => values.is_null(row),
            Self::I32(values) => values.is_null(row),
            Self::U64(values) => values.is_null(row),
            Self::I64(values) => values.is_null(row),
            Self::F32(values) => values.is_null(row),
            Self::F64(values) => values.is_null(row),
            Self::String(values) => values.is_null(row),
            Self::Bytes(values) => values.is_null(row),
            Self::Datetime(values) => values.is_null(row),
            Self::List { offsets, .. } => offsets.is_null(row),
            Self::FixedList { list, .. } => list.is_null(row),
        }
    }
}

/// The decimal text of a float, or nothing for a value no decimal form represents.
fn float_text(value: f64) -> Option<String> {
    let number = serde_json::Number::from_f64(value)?;
    Some(number.to_string())
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::num::NonZeroU32;

    use arrow_array::builder::{BinaryBuilder, ListBuilder};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use error_stack::{AttachmentKind, FrameKind};
    use nervix_connector::{DeadlineElapsed, MappedEmitterCarrier};
    use nervix_models::DeadlineKey;
    use nervix_primitives::sync::StdArc;

    use super::*;

    fn pool_bounds() -> ClientPoolBounds {
        let maximum = NonZeroU32::new(8).assured("eight is not zero");
        ClientPoolBounds::new(2, maximum).assured("two connections fit a maximum of eight")
    }

    fn client_config(pairs: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
        let mut entries = vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: "postgres://127.0.0.1/nervix?sslmode=disable".to_string(),
        }];
        for (key, value) in pairs {
            entries.push(ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            });
        }
        entries
    }

    /// The description the emitter host reports a failed write by.
    fn first_description<C>(report: &Report<C>) -> Option<String> {
        report.frames().find_map(|frame| match frame.kind() {
            FrameKind::Attachment(AttachmentKind::Printable(attachment)) => {
                Some(attachment.to_string())
            }
            FrameKind::Context(_) | FrameKind::Attachment(_) => None,
        })
    }

    #[test]
    fn a_client_that_leaves_the_pool_keys_out_takes_their_declared_defaults() {
        let policy = PostgresPoolPolicy::read(&client_config(&[]))
            .assured("every omitted key takes its declared default");
        let options = policy.pool_options(pool_bounds());
        assert_eq!(options.get_min_connections(), 2);
        assert_eq!(options.get_max_connections(), 8);
        assert_eq!(options.get_acquire_timeout(), Duration::from_secs(30));
        assert_eq!(options.get_idle_timeout(), Some(Duration::from_secs(600)));
        assert_eq!(options.get_max_lifetime(), Some(Duration::from_secs(1800)));
        assert!(options.get_test_before_acquire());
    }

    #[test]
    fn the_pool_takes_every_key_the_client_declares() {
        let policy = PostgresPoolPolicy::read(&client_config(&[
            ("pool_acquire_timeout_ms", "1500"),
            ("pool_idle_timeout_ms", "45000"),
            ("pool_max_lifetime_ms", "90000"),
            ("pool_test_before_acquire", "false"),
        ]))
        .assured("each declared value keeps its rule");
        assert_eq!(policy.checkout.key(), DeadlineKey::PoolAcquireTimeout);
        let options = policy.pool_options(pool_bounds());
        assert_eq!(options.get_acquire_timeout(), Duration::from_millis(1500));
        assert_eq!(options.get_idle_timeout(), Some(Duration::from_secs(45)));
        assert_eq!(options.get_max_lifetime(), Some(Duration::from_secs(90)));
        assert!(!options.get_test_before_acquire());
    }

    #[test]
    fn a_pool_key_that_breaks_its_rule_fails_the_open_and_is_named_beneath_it() {
        for (key, value) in [
            ("pool_acquire_timeout_ms", "0"),
            ("pool_idle_timeout_ms", "1.5s"),
            ("pool_max_lifetime_ms", "-1"),
            ("pool_test_before_acquire", "yes"),
        ] {
            let refusal = PostgresPoolPolicy::read(&client_config(&[(key, value)]))
                .expect_err("the committed value breaks its declared rule");
            assert_eq!(
                refusal.current_context(),
                &EmitterOpenError::InvalidConfiguration {
                    connector: POSTGRES
                },
                "{key}"
            );
            let cause = refusal
                .downcast_ref::<ClientConfigError>()
                .assured("the key's own refusal stays beneath the open");
            assert!(cause.to_string().contains(key), "{cause}");
        }
    }

    #[test]
    fn a_checkout_that_runs_out_of_its_deadline_reports_the_key_and_its_bound() {
        let checkout =
            PostgresPoolPolicy::read(&client_config(&[("pool_acquire_timeout_ms", "1000")]))
                .assured("one second is a valid deadline")
                .checkout;
        let failure = checkout_failure(
            sqlx::Error::PoolTimedOut,
            checkout,
            EmitterWriteError::NotInitialized {
                connector: POSTGRES,
            },
        );
        let elapsed = failure
            .downcast_ref::<DeadlineElapsed>()
            .assured("the deadline stays beneath the failure's context");
        assert_eq!(elapsed.key, DeadlineKey::PoolAcquireTimeout);
        assert_eq!(elapsed.bound, Duration::from_secs(1));
        assert_eq!(
            first_description(&failure).as_deref(),
            Some(
                "checking out a Postgres connection did not finish within pool_acquire_timeout_ms \
                 1s"
            )
        );
    }

    #[test]
    fn any_other_checkout_failure_keeps_the_driver_cause() {
        let checkout = PostgresPoolPolicy::read(&client_config(&[]))
            .assured("every omitted key takes its declared default")
            .checkout;
        let failure = checkout_failure(
            sqlx::Error::PoolClosed,
            checkout,
            EmitterOpenError::Initialize {
                connector: POSTGRES,
            },
        );
        assert!(failure.contains::<sqlx::Error>());
        assert!(!failure.contains::<DeadlineElapsed>());
        assert_eq!(
            first_description(&failure),
            Some(sqlx::Error::PoolClosed.to_string())
        );
    }

    #[test]
    fn invalid_conflict_target_keeps_its_typed_cause_at_the_host_boundary() {
        let failure = PostgresTableEmitter::conflict_target_sql(&[String::new()])
            .expect_err("a target column must be named");
        let report = PostgresWriteError::into_report(failure);
        assert!(report.contains::<PostgresWriteError>());
    }

    #[test]
    fn a_driver_failure_names_its_cause_once_and_keeps_it_at_the_host_boundary() {
        let cause = sqlx::Error::PoolTimedOut.to_string();
        let execute = PostgresWriteError::report_execute(sqlx::Error::PoolTimedOut, None);
        let metadata = Report::new(PostgresWriteError::Metadata(sqlx::Error::PoolTimedOut));

        assert_eq!(
            format!("{execute:#}"),
            format!("Postgres insert failed: {cause}")
        );
        assert_eq!(
            format!("{metadata:#}"),
            format!("failed to load Postgres table metadata: {cause}")
        );
        let published = PostgresWriteError::into_report(metadata);
        let mut descriptions = Vec::new();
        for frame in published.frames() {
            if let error_stack::FrameKind::Attachment(error_stack::AttachmentKind::Printable(
                attachment,
            )) = frame.kind()
            {
                descriptions.push(attachment.to_string());
            }
        }
        assert_eq!(
            descriptions,
            [format!("failed to load Postgres table metadata: {cause}")]
        );
    }

    #[test]
    fn classifies_only_data_and_constraint_sqlstates_as_record_errors() {
        for code in ["22001", "22003", "22P02", "23000", "23502", "23505"] {
            assert!(
                is_record_sqlstate(code),
                "{code} should be a definitive record error"
            );
        }
        for code in ["08006", "40001", "42P01", "53300", "57P01"] {
            assert!(
                !is_record_sqlstate(code),
                "{code} requires infrastructure retry"
            );
        }
    }

    #[test]
    fn binds_one_text_array_per_mapped_column() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("at", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![Some(7), Some(8), None])),
                StdArc::new(Float64Array::from(vec![Some(1.5), None, Some(2.0)])),
                StdArc::new(TimestampNanosecondArray::from(vec![
                    Some(1_700_000_000_123_456_789),
                    None,
                    None,
                ])),
            ],
        )
        .expect("the mapped batch should build");
        let names = ["id".to_string(), "score".to_string(), "at".to_string()];

        let columns = MappedTextColumns::new(&batch, &names).expect("columns should be mapped");
        let inserts = UnnestInserts::bind(
            String::new(),
            &[columns],
            &[
                MappedEmitterMember { carrier: 0, row: 0 },
                MappedEmitterMember { carrier: 0, row: 2 },
            ],
            names.len(),
        );

        assert_eq!(
            inserts.columns,
            vec![
                vec![Some("7".to_string()), None],
                vec![Some("1.5".to_string()), Some("2.0".to_string())],
                vec![
                    Some("2023-11-14T22:13:20.123456789+00:00".to_string()),
                    None
                ],
            ]
        );
    }

    #[test]
    fn binds_bytes_as_bytea_hex_and_arrays_as_json_text() {
        let pairs = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", DataType::Int64, false)),
            2,
            StdArc::new(Int64Array::from(vec![1, 10])),
            None,
        )
        .expect("one row of two elements builds");
        let mut blobs = ListBuilder::new(BinaryBuilder::new());
        blobs.values().append_value(b"\x00\xff");
        blobs.values().append_value(b"");
        blobs.append(true);
        let blobs = blobs.finish();
        let raw = BinaryArray::from(vec![b"\x00\xff\\".as_slice()]);
        let schema = StdArc::new(Schema::new(vec![
            Field::new("pair", pairs.data_type().clone(), true),
            Field::new("blobs", blobs.data_type().clone(), true),
            Field::new("raw", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(pairs), StdArc::new(blobs), StdArc::new(raw)],
        )
        .expect("the mapped batch should build");
        let names = ["pair".to_string(), "blobs".to_string(), "raw".to_string()];

        let columns = MappedTextColumns::new(&batch, &names).expect("columns should be mapped");

        let texts = columns
            .values
            .iter()
            .map(|column| column.text(0))
            .collect::<Vec<_>>();
        assert_eq!(
            texts,
            vec![
                Some("[1,10]".to_string()),
                Some(r#"["AP8=",""]"#.to_string()),
                Some(r"\x00ff5c".to_string()),
            ]
        );
    }

    #[test]
    fn every_mapped_type_binds_the_text_its_declared_encoding_names() {
        use nervix_models::{ParseAsType, PostgresValueEncoding};

        struct BindingCase {
            ty: ParseAsType,
            array: ArrayRef,
            text: Option<&'static str>,
            encoding: PostgresValueEncoding,
        }
        macro_rules! binding {
            ($ty:ident, $array:ident, $value:expr, $text:expr, $encoding:ident) => {
                BindingCase {
                    ty: ParseAsType::$ty,
                    array: StdArc::new($array::from(vec![$value])),
                    text: $text,
                    encoding: PostgresValueEncoding::$encoding,
                }
            };
        }
        let cases = [
            binding!(Bool, BooleanArray, true, Some("true"), BooleanText),
            binding!(U8, UInt8Array, u8::MAX, Some("255"), DecimalInteger),
            binding!(I8, Int8Array, i8::MIN, Some("-128"), DecimalInteger),
            binding!(U16, UInt16Array, u16::MAX, Some("65535"), DecimalInteger),
            binding!(I16, Int16Array, i16::MIN, Some("-32768"), DecimalInteger),
            binding!(
                U32,
                UInt32Array,
                u32::MAX,
                Some("4294967295"),
                DecimalInteger
            ),
            binding!(
                I32,
                Int32Array,
                i32::MIN,
                Some("-2147483648"),
                DecimalInteger
            ),
            binding!(
                U64,
                UInt64Array,
                u64::MAX,
                Some("18446744073709551615"),
                DecimalInteger
            ),
            binding!(
                I64,
                Int64Array,
                i64::MIN,
                Some("-9223372036854775808"),
                DecimalInteger
            ),
            binding!(
                F32,
                Float32Array,
                0.1_f32,
                Some("0.10000000149011612"),
                DecimalFloat
            ),
            binding!(F64, Float64Array, f64::NAN, None, DecimalFloat),
            binding!(F64, Float64Array, f64::NEG_INFINITY, None, DecimalFloat),
            binding!(String, StringArray, "é", Some("é"), Text),
            binding!(
                Bytes,
                BinaryArray,
                b"\x00\xff".as_slice(),
                Some(r"\x00ff"),
                ByteaHex
            ),
            binding!(
                Datetime,
                TimestampNanosecondArray,
                0_i64,
                Some("1970-01-01T00:00:00+00:00"),
                TimestampText
            ),
        ];
        for case in cases {
            assert_eq!(
                PostgresValueEncoding::for_type(&case.ty),
                case.encoding,
                "{}",
                case.ty
            );
            let column = MappedTextColumn::new(&case.array).expect("the declared type binds");
            assert_eq!(column.text(0).as_deref(), case.text, "{}", case.ty);
        }
    }

    #[test]
    fn an_insert_and_its_catalog_lookup_quote_each_declared_table_part() {
        let mut connector = emitter_without_connections();
        for (table, quoted) in [
            ("events", r#""events""#),
            ("Sales.Orders", r#""Sales"."Orders""#),
        ] {
            connector.table = PostgresTableName::parse(table).expect("a current table path");
            assert_eq!(connector.table_sql(), quoted);
            let sql = connector
                .insert_sql(&["id".to_string()], &["bigint".to_string()])
                .expect("an insert without a conflict action renders");
            assert!(sql.starts_with(&format!("INSERT INTO {quoted} (")), "{sql}");
        }
    }

    #[test]
    fn connection_options_read_the_declared_tls_policy() {
        let refusal = match connect_options("postgres://127.0.0.1/nervix?sslmode=require", &[]) {
            Ok(_) => panic!("only a declared policy connects"),
            Err(refusal) => refusal,
        };
        assert!(matches!(
            refusal.current_context(),
            EmitterOpenError::InvalidConfiguration { .. }
        ));
        assert!(refusal.contains::<nervix_models::ClientConfigSchemaError>());
        connect_options("postgres://127.0.0.1/nervix?ssl-mode=verify-full", &[])
            .expect("the driver's alias spells the same declared option");
    }

    /// The measured size is the statement text and every array exactly as the driver encodes
    /// it, so a scenario's `MAX SIZE` can be written to the byte.
    #[test]
    fn measures_the_statement_and_the_arrays_the_driver_encodes() {
        let connector = PostgresTableEmitter {
            connections: Box::new(NoConnections),
            table: PostgresTableName::parse("limit_a_t0192a1b2c3d4e5f60718293a4b5c6d7e")
                .expect("the test table name is valid"),
            conflict_action: PostgresConflictAction::None,
            limits: RowRequestLimits::from(EmitterBatchPolicy {
                max_messages: nervix_models::BatchMessageLimit::try_from(3_u32)
                    .expect("three is a valid message limit"),
                max_size: "1KiB".parse().expect("1KiB is a valid size"),
            }),
        };
        let names = ["seq".to_string(), "note".to_string()];
        let sql = connector
            .insert_sql(&names, &["bigint".to_string(), "text".to_string()])
            .expect("the insert statement builds");
        let schema = StdArc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
                StdArc::new(StringArray::from(vec![Some("abc"), None, Some("abc")])),
            ],
        )
        .expect("the mapped batch should build");
        let columns = MappedTextColumns::new(&batch, &names).expect("columns should be mapped");
        let members = [0, 1, 2].map(|row| MappedEmitterMember { carrier: 0, row });

        let inserts = UnnestInserts::bind(sql, &[columns], &members, names.len());

        assert_eq!(inserts.sql.len(), 166);
        for range in [0..1, 0..3, 1..3, 2..3] {
            let mut encoded = 0;
            for column in &inserts.columns {
                let mut buffer = sqlx::postgres::PgArgumentBuffer::default();
                let texts = &column[range.clone()];
                let written = <&[Option<String>] as sqlx::Encode<'_, Postgres>>::encode_by_ref(
                    &texts,
                    &mut buffer,
                )
                .expect("a text array encodes");
                assert!(matches!(written, sqlx::encode::IsNull::No));
                encoded += buffer.len();
            }
            let expected =
                u64::try_from(inserts.sql.len() + encoded).expect("a test insert fits u64");
            assert_eq!(inserts.measure(range), expected);
        }
    }

    /// The characters a generated text is drawn from: ASCII, quotes and backslashes the SQL text
    /// never carries, and characters of two, three and four UTF-8 bytes.
    const TEXT_PIECES: [&str; 8] = ["a", "Z", "'", "\"", "\\", "é", "日", "😀"];

    /// A generated mapped column: its name and the texts of its rows, NULL among them.
    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedColumn {
        #[generator(bolero::generator::produce_with::<Vec<u8>>().len(1_usize..=12))]
        name: Vec<u8>,
        #[generator(bolero::generator::produce_with::<Vec<Option<Vec<u8>>>>().len(1_usize..=8))]
        cells: Vec<Option<Vec<u8>>>,
    }

    /// A generated insert: one to four mapped columns and the number of rows the write holds.
    #[derive(Debug, bolero::TypeGenerator)]
    struct InsertCase {
        #[generator(bolero::generator::produce_with::<Vec<GeneratedColumn>>().len(1_usize..=4))]
        columns: Vec<GeneratedColumn>,
        rows: u8,
    }

    impl InsertCase {
        /// One to eight rows.
        fn rows(&self) -> usize {
            usize::from(self.rows % 8) + 1
        }
    }

    impl GeneratedColumn {
        fn name(&self) -> String {
            let mut name = String::new();
            for byte in &self.name {
                name.push(char::from(b"abcxyz_\"0"[usize::from(*byte) % 9]));
            }
            name
        }

        fn text(&self, row: usize) -> Option<String> {
            let cell = self.cells[row % self.cells.len()].as_ref()?;
            let mut text = String::new();
            for byte in cell {
                text.push_str(TEXT_PIECES[usize::from(*byte) % TEXT_PIECES.len()]);
            }
            Some(text)
        }
    }

    /// The bytes of the Bind message the driver sends for an insert binding `arrays`, by the
    /// extended-query protocol: the message type and length, an unnamed portal, a statement name
    /// of at most `sqlx_s_` and ten digits, one format code per parameter, the parameter count,
    /// each parameter's length word and bytes, and one result format code.
    fn bind_message_bytes(arrays: &[usize]) -> usize {
        let statement_name = "sqlx_s_".len() + 10 + 1;
        let parameters = arrays.len();
        let mut bytes = 1 + 4 + 1 + statement_name + 2 + 2 * parameters + 2 + 2 + 2;
        for array in arrays {
            bytes += 4 + array;
        }
        bytes
    }

    /// An insert of any run of a write's rows is measured at exactly its statement text and the
    /// text arrays the driver encodes for that run, NULLs, quotes and multi-byte characters
    /// included, and that size is never smaller than the Bind message carrying those arrays.
    #[test]
    fn bolero_measured_inserts_are_the_statement_and_arrays_the_driver_encodes() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(1024)
            .with_type::<InsertCase>()
            .for_each(|case| {
                let generated = &case.columns;
                let rows = case.rows();
                let connector = PostgresTableEmitter {
                    connections: Box::new(NoConnections),
                    table: PostgresTableName::parse("events").assured("the table name is valid"),
                    conflict_action: PostgresConflictAction::None,
                    limits: RowRequestLimits::from(EmitterBatchPolicy {
                        max_messages: nervix_models::BatchMessageLimit::try_from(8_u32)
                            .assured("eight is a valid message limit"),
                        max_size: "1MiB".parse().assured("1MiB is a valid size"),
                    }),
                };
                let names = generated
                    .iter()
                    .map(GeneratedColumn::name)
                    .collect::<Vec<_>>();
                let types = vec!["text".to_string(); names.len()];
                let sql = connector
                    .insert_sql(&names, &types)
                    .assured("the insert statement builds");
                let mut fields = Vec::with_capacity(generated.len());
                let mut arrays: Vec<arrow_array::ArrayRef> = Vec::with_capacity(generated.len());
                for (index, column) in generated.iter().enumerate() {
                    fields.push(Field::new(format!("column_{index}"), DataType::Utf8, true));
                    let texts = (0..rows).map(|row| column.text(row)).collect::<Vec<_>>();
                    arrays.push(StdArc::new(StringArray::from(texts)));
                }
                let batch = RecordBatch::try_new(StdArc::new(Schema::new(fields)), arrays)
                    .assured("every generated column has one text per row");
                let columns =
                    MappedTextColumns::new(&batch, &names).assured("text columns are mapped");
                let members = (0..rows)
                    .map(|row| MappedEmitterMember { carrier: 0, row })
                    .collect::<Vec<_>>();
                let inserts = UnnestInserts::bind(sql, &[columns], &members, names.len());
                for start in 0..rows {
                    for end in start + 1..=rows {
                        let mut array_bytes = Vec::with_capacity(inserts.columns.len());
                        for column in &inserts.columns {
                            let mut buffer = sqlx::postgres::PgArgumentBuffer::default();
                            let texts = &column[start..end];
                            let written =
                                <&[Option<String>] as sqlx::Encode<'_, Postgres>>::encode_by_ref(
                                    &texts,
                                    &mut buffer,
                                )
                                .assured("a text array encodes");
                            assert!(matches!(written, sqlx::encode::IsNull::No));
                            array_bytes.push(buffer.len());
                        }
                        let encoded = inserts.sql.len() + array_bytes.iter().sum::<usize>();
                        let measured = inserts.measure(start..end);
                        assert_eq!(
                            measured,
                            u64::try_from(encoded).assured("an encoded insert fits u64"),
                            "rows {start}..{end}"
                        );
                        let bind = u64::try_from(bind_message_bytes(&array_bytes))
                            .assured("a Bind message fits u64");
                        assert!(measured >= bind, "rows {start}..{end}: {measured} < {bind}");
                    }
                }
            });
    }

    /// Stands in for the pool where a test never opens a connection.
    struct NoConnections;

    #[async_trait]
    impl PostgresConnections for NoConnections {
        async fn connection(&self) -> EmitterWriteResult<PostgresConnection> {
            Err(Report::new(EmitterWriteError::NotInitialized {
                connector: POSTGRES,
            })
            .attach_printable("pool-acquire-detail"))
        }
    }

    fn emitter_without_connections() -> PostgresTableEmitter {
        PostgresTableEmitter {
            connections: Box::new(NoConnections),
            table: PostgresTableName::parse("events").expect("the test table name is valid"),
            conflict_action: PostgresConflictAction::None,
            limits: RowRequestLimits::from(EmitterBatchPolicy {
                max_messages: nervix_models::BatchMessageLimit::try_from(1_u32)
                    .expect("one is a valid message limit"),
                max_size: "1KiB".parse().expect("1KiB is a valid size"),
            }),
        }
    }

    #[nervix_primitives::test]
    async fn pool_failure_keeps_the_acquire_diagnostic_at_the_host_boundary() {
        let connector = emitter_without_connections();
        let Err(error) = connector.borrow().await else {
            panic!("the test pool has no connection");
        };
        assert!(matches!(error.current_context(), PostgresWriteError::Pool));
        let report = PostgresWriteError::into_report(error);
        assert!(report.contains::<PostgresWriteError>());
        assert_eq!(
            first_description(&report).as_deref(),
            Some("pool-acquire-detail"),
            "the host reports the pool's own description"
        );
    }

    /// A write borrows its connection before its metadata lookup, so one it cannot borrow fails as
    /// a whole with the pool's cause and answers for none of its rows.
    #[nervix_primitives::test]
    async fn a_write_without_a_connection_answers_no_row_and_keeps_the_acquire_diagnostic() {
        let mut connector = emitter_without_connections();
        let batch = RecordBatch::try_new(
            StdArc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)])),
            vec![StdArc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("the test batch matches its schema");
        let target_columns = ["seq".to_string()];
        let selected_rows = [0, 1];
        let outcome = connector
            .publish(MappedEmitterRows {
                target_columns: &target_columns,
                carriers: vec![MappedEmitterCarrier {
                    batch_index: 0,
                    batch: &batch,
                    selected_rows: &selected_rows,
                    occurred_at: nervix_models::Timestamp::from_unix_nanos(5),
                    acknowledgements: None,
                }],
            })
            .await;
        let parts = outcome.into_parts();
        assert!(parts.delivered.is_empty());
        assert!(parts.rejected.is_empty());
        let Some(error) = parts.infrastructure_error else {
            panic!("a write without a connection fails as a whole");
        };
        assert!(format!("{error:?}").contains("pool-acquire-detail"));
    }

    #[test]
    fn a_repeated_conflict_key_in_one_insert_is_isolated_row_by_row() {
        assert!(is_record_sqlstate("21000"));
    }
}
