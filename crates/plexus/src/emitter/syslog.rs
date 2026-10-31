//! SYSLOG emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of SYSLOG emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], SyslogCollectorPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    unconfirmed_mode!(SyslogCollectorPublishingMode)
}

pub(super) fn collector_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<SyslogCollectorEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|(client, mode)| SyslogCollectorEmitter { client, mode }),
    )
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        EmitterEntity, Model, SyslogCollectorEmitter, SyslogCollectorPublishingMode, SyslogEmitter,
    };
    use rstest::rstest;

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    /// A `SYSLOG COLLECTOR` emitter statement in the publishing mode `mode`, with `batch` before its
    /// flush policy.
    fn statement(mode: &str, batch: &str) -> String {
        format!(
            "CREATE SYSLOG COLLECTOR EMITTER publishing FROM events TO syslog_main MODE {mode} \
             ENCODE USING syslog_codec INHERIT ALL {batch}FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
             GENERAL ERROR LOG;"
        )
    }

    /// The collector Model `statement` creates.
    fn created_collector(statement: &str) -> SyslogCollectorEmitter {
        let Some(Model::Emitter(emitter)) = created_model(statement) else {
            panic!("a declared collector is accepted by the composed grammar: {statement}");
        };
        let EmitterEntity::Syslog(SyslogEmitter::Collector(destination)) = *emitter.connector
        else {
            panic!("the SYSLOG COLLECTOR head constructs the Syslog collector Model");
        };
        destination
    }

    #[rstest]
    #[case("")]
    #[case("BATCH MAX MESSAGES 10 MAX SIZE 64KiB ")]
    fn a_collector_publishes_no_ack_with_or_without_a_batch(#[case] batch: &str) {
        let destination =
            created_collector(&statement("NO ACK RETRY POLICY BACKOFF 1s MAX 30s", batch));

        assert_eq!(destination.client.as_str(), "syslog_main");
        assert!(matches!(
            destination.mode,
            SyslogCollectorPublishingMode::NoAck { .. }
        ));
    }

    #[test]
    fn collector_completion_offers_only_its_declared_mode() {
        assert_offers_exactly(
            "CREATE SYSLOG COLLECTOR EMITTER publishing FROM events TO syslog_main MODE ",
            &["NO ACK"],
        );
    }

    #[rstest]
    #[case("ACK RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case("ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s")]
    #[case("SINGLE RETRY POLICY BACKOFF 1s MAX 30s")]
    fn the_composed_grammar_refuses_publishing_the_collector_does_not_declare(#[case] mode: &str) {
        let statement = statement(mode, "");
        assert!(
            refused(&statement),
            "a collector sends without acknowledgements: {statement}"
        );
    }
}
