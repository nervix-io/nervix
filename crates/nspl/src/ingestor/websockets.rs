//! The WebSockets connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The bodies of a `WEBSOCKETS CONNECTION` ingestor — its client, delivery mode and
//!   quiesce mode — and of a `WEBSOCKETS ENDPOINT` ingestor.
//! - **Depends on.** The shared ingestor clauses, the endpoint intake body and the WebSockets
//!   vocabulary.
//! - **Must not know.** The WebSockets driver, how a connection is opened or signaled, or how an
//!   endpoint is served.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    WebsocketsConnectionSource, WebsocketsEndpointSource, WebsocketsIngestMode,
    WebsocketsUpgradeHeader,
};

use super::{
    delivery::{no_ack, push_quiesce_clause},
    endpoint::{EndpointIntake, intake},
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, client_ref, kw, string_lit, tok},
};

fn upgrade_headers<'src>()
-> impl Parser<'src, &'src [Token], Vec<WebsocketsUpgradeHeader>, extra::Err<ParseError<'src>>> + Clone
{
    let header = string_lit()
        .labelled("upgrade_header_name")
        .then_ignore(tok(Token::Eq))
        .then(string_lit().labelled("string_literal"))
        .map(|(name, value)| WebsocketsUpgradeHeader { name, value });
    kw(Identifier::Headers).ignore_then(
        header
            .separated_by(tok(Token::Comma))
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(tok(Token::LBrace), tok(Token::RBrace)),
    )
}

/// `FROM <client> MODE NO ACK ON QUIESCE BUFFER …|DROP`.
pub(super) fn connection_body<'src>()
-> impl Parser<'src, &'src [Token], WebsocketsConnectionSource, extra::Err<ParseError<'src>>> + Clone
{
    client_ref()
        .then(upgrade_headers().or_not())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(WebsocketsIngestMode::NoAck))
        .then(push_quiesce_clause())
        .map(
            |(((client, headers), mode), quiesce)| WebsocketsConnectionSource {
                client,
                headers: headers.unwrap_or_default(),
                mode,
                quiesce,
            },
        )
        .boxed()
}

/// The body of an ingestor of a WebSockets endpoint this cluster serves.
pub(super) fn endpoint_body<'src>()
-> impl Parser<'src, &'src [Token], WebsocketsEndpointSource, extra::Err<ParseError<'src>>> + Clone
{
    intake().map(
        |EndpointIntake {
             endpoint,
             mode,
             quiesce,
         }| WebsocketsEndpointSource {
            endpoint,
            mode,
            quiesce,
        },
    )
}

#[cfg(test)]
mod tests {
    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    #[test]
    fn a_connection_accepts_literal_upgrade_headers_before_its_mode() {
        let statement = "CREATE WEBSOCKETS CONNECTION INGESTOR events FROM socket HEADERS { \
                         'authorization' = 'Bearer token' } MODE NO ACK ON QUIESCE DROP DECODE \
                         USING codec TO events UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
                         GENERAL ERROR LOG;";
        assert!(created_model(statement).is_some(), "{statement}");
    }

    #[test]
    fn a_connection_completes_only_its_headers_or_mode_after_its_client() {
        assert_offers_exactly(
            "CREATE WEBSOCKETS CONNECTION INGESTOR events FROM socket ",
            &[
                "HEADERS { 'authorization' = 'Bearer fixture' }",
                "MODE NO ACK",
            ],
        );
        assert_offers_exactly(
            "CREATE WEBSOCKETS CONNECTION INGESTOR events FROM socket HEADERS { 'x-event' = \
             'orders' } ",
            &["MODE NO ACK"],
        );
    }

    #[test]
    fn a_connection_requires_string_literals_in_its_upgrade_headers() {
        for headers in [
            "HEADERS { authorization = 'token' }",
            "HEADERS { 'x-event' = 1 }",
            "HEADERS { 'x-event' = message.event }",
        ] {
            let statement = format!(
                "CREATE WEBSOCKETS CONNECTION INGESTOR events FROM socket {headers} MODE NO ACK \
                 ON QUIESCE DROP DECODE USING codec TO events UNBRANCHED FLUSH IMMEDIATE ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            );
            assert!(refused(&statement), "{statement}");
        }
    }
}
