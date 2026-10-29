//! The syslog connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `SYSLOG LISTENER` ingestor: its client, delivery mode and quiesce
//!   mode.
//! - **Depends on.** The shared ingestor clauses and the syslog vocabulary.
//! - **Must not know.** Syslog framing or how a listener is bound.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{SyslogIngestMode, SyslogListenerIngestor};

use super::delivery::{flexible_quiesce_clause, no_ack};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw},
};

/// `FROM <client> MODE NO ACK ON QUIESCE SUSPEND|BUFFER …|DROP`.
pub(super) fn listener_body<'src>()
-> impl Parser<'src, &'src [Token], SyslogListenerIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(SyslogIngestMode::NoAck))
        .then(flexible_quiesce_clause())
        .map(|((client, mode), quiesce)| SyslogListenerIngestor {
            client,
            mode,
            quiesce,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        IngestQuiesceMode, IngestorEntity, IngestorInput, Model, SyslogIngestMode, SyslogIngestor,
        SyslogListenerIngestor,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// A `SYSLOG LISTENER` ingestor statement with `options` after its client, in the delivery
    /// mode `mode` and the quiesce mode `quiesce`.
    fn statement(options: &str, mode: &str, quiesce: &str) -> String {
        format!(
            "CREATE SYSLOG LISTENER INGESTOR intake FROM syslog_main {options}MODE {mode} ON \
             QUIESCE {quiesce} DECODE USING syslog_codec TO events INHERIT ALL UNBRANCHED FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    /// The listener Model `statement` creates.
    fn created_listener(statement: &str) -> SyslogListenerIngestor {
        let Some(Model::Ingestor(ingestor)) = created_model(statement) else {
            panic!("a declared listener is accepted by the composed grammar: {statement}");
        };
        let IngestorInput::Transport(input) = ingestor.input else {
            panic!("the SYSLOG LISTENER head names a transport");
        };
        let IngestorEntity::Syslog(SyslogIngestor::Listener(source)) = input.source else {
            panic!("the SYSLOG LISTENER head constructs the Syslog listener Model");
        };
        source
    }

    #[rstest]
    #[case("SUSPEND")]
    #[case("BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST")]
    #[case("DROP")]
    fn a_listener_takes_no_ack_and_each_declared_quiesce_mode(#[case] quiesce: &str) {
        let source = created_listener(&statement("", "NO ACK", quiesce));

        assert_eq!(source.client.as_str(), "syslog_main");
        assert_eq!(source.mode, SyslogIngestMode::NoAck);
        let declared = match source.quiesce {
            IngestQuiesceMode::Suspend => "SUSPEND",
            IngestQuiesceMode::Buffer { .. } => "BUFFER",
            IngestQuiesceMode::Drop => "DROP",
            IngestQuiesceMode::Reject { .. } | IngestQuiesceMode::EndpointBuffer { .. } => {
                panic!("a listener declares no quiesce mode beyond SUSPEND, BUFFER and DROP")
            }
        };
        assert!(quiesce.starts_with(declared), "{quiesce} became {declared}");
    }

    #[test]
    fn listener_completion_follows_the_listener_declaration() {
        assert_offers_exactly(
            "CREATE SYSLOG LISTENER INGESTOR intake FROM syslog_main ",
            &["MODE"],
        );
        assert_offers_exactly(
            "CREATE SYSLOG LISTENER INGESTOR intake FROM syslog_main MODE ",
            &["NO ACK"],
        );
        assert_offers_exactly(
            "CREATE SYSLOG LISTENER INGESTOR intake FROM syslog_main MODE NO ACK ON QUIESCE ",
            &["SUSPEND", "BUFFER", "DROP"],
        );
    }

    #[rstest]
    #[case(
        "",
        "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        "SUSPEND"
    )]
    #[case(
        "",
        "ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
        "SUSPEND"
    )]
    #[case("INSTANCES 2 ", "NO ACK", "SUSPEND")]
    #[case("", "NO ACK", "REJECT RETRY AFTER 5s")]
    fn the_composed_grammar_refuses_what_the_listener_does_not_declare(
        #[case] options: &str,
        #[case] mode: &str,
        #[case] quiesce: &str,
    ) {
        let statement = statement(options, mode, quiesce);
        assert!(
            refused(&statement),
            "a listener runs once per node without acknowledgements: {statement}"
        );
    }
}
