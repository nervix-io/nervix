//! The ZeroMQ connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The body of a `ZEROMQ PULL` ingestor: its client, delivery mode and quiesce mode.
//! - **Depends on.** The shared ingestor clauses and the ZeroMQ vocabulary.
//! - **Must not know.** The ZeroMQ driver or how a socket is bound or connected.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{ZeroMqIngestMode, ZeroMqPullIngestor};

use super::delivery::{flexible_quiesce_clause, no_ack};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw},
};

/// `FROM <client> MODE NO ACK ON QUIESCE SUSPEND|BUFFER …|DROP`.
pub(super) fn pull_body<'src>()
-> impl Parser<'src, &'src [Token], ZeroMqPullIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(ZeroMqIngestMode::NoAck))
        .then(flexible_quiesce_clause())
        .map(|((client, mode), quiesce)| ZeroMqPullIngestor {
            client,
            mode,
            quiesce,
        })
        .boxed()
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns ZeroMQ grammar conformance evidence")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** ZeroMQ ingestor round trips, refusals and composed completion checks.
    //! - **Depends on.** The public grammar and canonical vocabulary rendering.
    //! - **Must not know.** Connector drivers or graph execution.

    use meticulous::ResultExt as _;

    use crate::{conformance_support::assert_offers_exactly, ingestor::parse_create_ingestor};

    #[test]
    fn zeromq_pull_round_trips_every_declared_quiesce_mode() {
        for quiesce in [
            "SUSPEND",
            "BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST",
            "DROP",
        ] {
            let text = format!(
                "CREATE ZEROMQ PULL INGESTOR events FROM socket MODE NO ACK ON QUIESCE {quiesce} \
                 DECODE USING codec TO output UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
                 GENERAL ERROR LOG;"
            );
            let parsed = parse_create_ingestor(&text).assured("a declared quiesce mode parses");
            let canonical = parsed
                .to_canonical_plexus()
                .assured("a parsed ZeroMQ ingestor has a canonical form");
            assert_eq!(
                parse_create_ingestor(&canonical).assured("the canonical form parses"),
                parsed
            );
        }
    }

    #[test]
    fn zeromq_pull_completion_stays_with_its_declared_modes() {
        assert_offers_exactly(
            "CREATE ZEROMQ PULL INGESTOR events FROM socket MODE ",
            &["NO ACK"],
        );
        assert_offers_exactly(
            "CREATE ZEROMQ PULL INGESTOR events FROM socket MODE NO ACK ON QUIESCE ",
            &["SUSPEND", "BUFFER", "DROP"],
        );
    }

    #[test]
    fn zeromq_pull_refuses_acknowledged_delivery() {
        let text = "CREATE ZEROMQ PULL INGESTOR events FROM socket MODE ACK SEQUENTIAL ACK \
                    TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE SUSPEND DECODE USING \
                    codec TO output UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL \
                    ERROR LOG;";
        assert!(parse_create_ingestor(text).is_err());
    }
}
