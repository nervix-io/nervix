//! The configured bound on one PostgreSQL statement and its response.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The statement-timeout range, its startup option and the local response deadline.
//! - **Depends on.** The connector deadline contract, vocabulary entries, primitives and SQLx.
//! - **Must not know.** Pools, graph routing, registry state or acknowledgement ownership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the statement deadline boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies resolved client entries")
)]

use std::{fmt, future::Future};

use error_stack::{Report, ResultExt as _};
use nervix_connector::{ClientConfigError, ConfiguredDeadline, DeadlineElapsed};
use nervix_models::{ClientConfigEntry, ConnectorKind};
use nervix_primitives::time::timeout;
use sqlx::postgres::PgConnectOptions;

/// One validated bound shared by the server statement and the local response wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PostgresStatementDeadline(ConfiguredDeadline);

#[derive(Debug, thiserror::Error)]
enum StatementDeadlineError {
    #[error(
        "{} statement_timeout accepts at most {} milliseconds",
        ConnectorKind::Postgres,
        i32::MAX
    )]
    OutOfRange,
}

impl PostgresStatementDeadline {
    pub(super) fn read(
        config: &[ClientConfigEntry],
    ) -> error_stack::Result<Option<Self>, ClientConfigError> {
        let configured = ConfiguredDeadline::timeout(config)?;
        let Some(configured) = configured else {
            return Ok(None);
        };
        // PostgreSQL's statement_timeout is an integer GUC measured in milliseconds. Validate
        // its server range before opening a connection, with the shared positive deadline rule.
        i32::try_from(configured.bound().as_millis())
            .change_context(StatementDeadlineError::OutOfRange)
            .change_context(ClientConfigError::InvalidDeadline { key: "timeout_ms" })?;
        Ok(Some(Self(configured)))
    }

    pub(super) fn configure(self, options: PgConnectOptions) -> PgConnectOptions {
        // SQLx appends startup options. This last setting takes precedence over an options value
        // in the URL while retaining the URL's other driver-delegated options.
        options.options([("statement_timeout", self.0.bound().as_millis())])
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the external query future this deadline bounds"
        )
    )]
    pub(super) async fn wait<F: Future>(
        self,
        operation: &'static str,
        request: F,
    ) -> error_stack::Result<F::Output, DeadlineElapsed> {
        match timeout(self.0.bound(), request).await {
            Ok(response) => Ok(response),
            Err(source) => {
                let elapsed = self.0.elapsed(operation);
                Err(Report::new(source)
                    .change_context(elapsed)
                    .attach_printable(elapsed.to_string()))
            }
        }
    }
}

impl fmt::Display for PostgresStatementDeadline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {:?}", self.0.key().key(), self.0.bound())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the statement deadline owner")
)]
mod tests {
    use std::{future, time::Duration};

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::DeadlineKey;

    use super::*;

    fn entries(value: &str) -> [ClientConfigEntry; 1] {
        [ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: value.to_string(),
        }]
    }

    #[test]
    fn omission_sets_no_statement_deadline() {
        assert_eq!(
            PostgresStatementDeadline::read(&[]).assured("omission is valid"),
            None
        );
    }

    #[test]
    fn the_server_bound_keeps_other_options_and_takes_precedence() {
        for value in ["1", "1500", "2147483647"] {
            let deadline = PostgresStatementDeadline::read(&entries(value))
                .assured("the deadline fits PostgreSQL's integer range")
                .assured("the client declares the key");
            let options = PgConnectOptions::new()
                .options([("search_path", "public"), ("statement_timeout", "0")]);
            let configured = deadline.configure(options);
            let expected = format!(
                "-c search_path=public -c statement_timeout=0 -c statement_timeout={value}"
            );
            assert_eq!(configured.get_options(), Some(expected.as_str()));
        }
    }

    #[test]
    fn a_deadline_outside_the_server_range_keeps_its_typed_refusal() {
        for value in ["0", "-1", "1.5", "2147483648"] {
            let report = PostgresStatementDeadline::read(&entries(value))
                .expect_err("the value cannot bound a PostgreSQL statement");
            assert!(matches!(
                report.current_context(),
                ClientConfigError::InvalidDeadline { key: "timeout_ms" }
            ));
            if value == "2147483648" {
                assert!(report.contains::<StatementDeadlineError>());
                assert!(report.contains::<std::num::TryFromIntError>());
            }
        }
    }

    #[nervix_primitives::test]
    async fn an_unanswered_operation_reports_its_exact_key_bound_and_operation() {
        let deadline = PostgresStatementDeadline::read(&entries("1"))
            .assured("one millisecond is a valid deadline")
            .assured("the client declares the key");
        let report = deadline
            .wait("the catalog query", future::pending::<()>())
            .await
            .expect_err("an unanswered operation runs out of its deadline");
        assert_eq!(report.current_context().operation, "the catalog query");
        assert_eq!(report.current_context().key, DeadlineKey::Timeout);
        assert_eq!(report.current_context().bound, Duration::from_millis(1));
    }

    #[nervix_primitives::test]
    async fn a_driver_answer_keeps_its_original_outcome() {
        let deadline = PostgresStatementDeadline::read(&entries("1000"))
            .assured("one second is a valid deadline")
            .assured("the client declares the key");
        let answer = deadline
            .wait(
                "the insert",
                future::ready(Err::<(), _>(sqlx::Error::RowNotFound)),
            )
            .await
            .assured("the driver answers before its deadline");
        assert!(matches!(answer, Err(sqlx::Error::RowNotFound)));
    }

    #[derive(Debug, thiserror::Error)]
    #[error("a server diagnostic containing a sensitive value")]
    struct CanceledStatement;

    impl sqlx::error::DatabaseError for CanceledStatement {
        fn message(&self) -> &str {
            "a server diagnostic containing a sensitive value"
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some("57014".into())
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[test]
    fn a_server_cancellation_names_the_bound_and_stays_transient() {
        use super::super::PostgresWriteError;

        let deadline = PostgresStatementDeadline::read(&entries("1500"))
            .assured("the server can enforce the bound");
        for report in [
            PostgresWriteError::report_metadata(CanceledStatement.into(), deadline),
            PostgresWriteError::report_execute(CanceledStatement.into(), deadline),
        ] {
            assert!(!report.current_context().is_record_error());
            let rendered = format!("{:?}", PostgresWriteError::into_report(report));
            assert!(rendered.contains("SQLSTATE 57014"));
            assert!(rendered.contains("timeout_ms 1.5s configured"));
            assert!(!rendered.contains("sensitive value"));
        }
        let report = PostgresWriteError::report_execute(CanceledStatement.into(), None);
        assert!(!report.current_context().is_record_error());
        assert!(matches!(
            report.current_context(),
            PostgresWriteError::Server { code: Some(code) } if code == "57014"
        ));
    }
}
