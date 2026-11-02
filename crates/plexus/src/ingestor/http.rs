//! The HTTP connector's ingestor grammar.
//!
//! Layer: language.
//!
//! - **Owns.** The bodies of an `HTTP REQUEST` ingestor — its client, method, headers, response
//!   bound, cadence, delivery mode and quiesce mode — and of an `HTTP ENDPOINT` ingestor.
//! - **Depends on.** The shared ingestor clauses, the endpoint intake body and the HTTP
//!   vocabulary.
//! - **Must not know.** The HTTP client library, how a request is sent, or how an endpoint is
//!   served.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{
    HttpEndpointIngestor, HttpRequestHeader, HttpRequestHeaderValue, HttpRequestIngestMode,
    HttpRequestIngestor,
};

use super::{
    delivery::{no_ack, polling_quiesce_clause},
    endpoint::{EndpointIntake, intake},
};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, client_ref, domain_clock_period_lit, kw, kw_phrase2, max_body_size_clause,
        string_lit, tok,
    },
};

/// `'<value>'`, or `CLIENT SECRET '<key>'` for a value the statement must not state.
fn request_header_value<'src>()
-> impl Parser<'src, &'src [Token], HttpRequestHeaderValue, extra::Err<ParseError<'src>>> + Clone {
    let literal = string_lit()
        .labelled("string_literal")
        .map(HttpRequestHeaderValue::Literal);
    let secret = kw_phrase2(Identifier::Client, Identifier::Secret)
        .ignore_then(string_lit().labelled("client_secret_key"))
        .map(HttpRequestHeaderValue::ClientSecret);
    choice((literal, secret))
}

fn request_headers<'src>()
-> impl Parser<'src, &'src [Token], Vec<HttpRequestHeader>, extra::Err<ParseError<'src>>> + Clone {
    let header = string_lit()
        .labelled("request_header_name")
        .then_ignore(tok(Token::Eq))
        .then(request_header_value())
        .map(|(name, value)| HttpRequestHeader { name, value });
    kw(Identifier::Headers).ignore_then(
        header
            .separated_by(tok(Token::Comma))
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(tok(Token::LBrace), tok(Token::RBrace)),
    )
}

/// `FROM <client> METHOD '<method>' [HEADERS {…}] [MAX BODY SIZE <bytes>] EVERY <period>
/// MODE NO ACK ON QUIESCE SUSPEND|BUFFER …`.
pub(super) fn request_body<'src>()
-> impl Parser<'src, &'src [Token], HttpRequestIngestor, extra::Err<ParseError<'src>>> + Clone {
    client_ref()
        .then_ignore(kw(Identifier::Method))
        .then(string_lit())
        .then(request_headers().or_not())
        .then(max_body_size_clause().or_not())
        .then_ignore(kw(Identifier::Every))
        .then(domain_clock_period_lit())
        .then_ignore(kw(Identifier::Mode))
        .then(no_ack().to(HttpRequestIngestMode::NoAck))
        .then(polling_quiesce_clause())
        .map(
            |((((((client, method), headers), max_body_size), every), mode), quiesce)| {
                HttpRequestIngestor {
                    client,
                    method,
                    headers: headers.unwrap_or_default(),
                    max_body_size,
                    every,
                    mode,
                    quiesce,
                }
            },
        )
        .boxed()
}

/// The body of an ingestor of an HTTP endpoint this cluster serves.
pub(super) fn endpoint_body<'src>()
-> impl Parser<'src, &'src [Token], HttpEndpointIngestor, extra::Err<ParseError<'src>>> + Clone {
    intake().map(
        |EndpointIntake {
             endpoint,
             mode,
             quiesce,
         }| HttpEndpointIngestor {
            endpoint,
            mode,
            quiesce,
        },
    )
}

#[cfg(test)]
mod tests {
    use nervix_models::{HttpRequestHeader, HttpRequestHeaderValue, IngestorEntity, Model};

    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    const TAIL: &str = "EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING codec TO events \
                        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;";

    #[test]
    fn polling_request_declares_method_headers_and_response_limit() {
        let statement = format!(
            "CREATE HTTP REQUEST INGESTOR events FROM source METHOD 'POST' HEADERS {{ 'accept' = \
             'application/json' }} MAX BODY SIZE 1MiB {TAIL}"
        );
        assert!(created_model(&statement).is_some(), "{statement}");
    }

    #[test]
    fn a_polling_header_reads_a_literal_or_a_client_secret_in_written_order() {
        let statement = format!(
            "CREATE HTTP REQUEST INGESTOR events FROM source METHOD 'GET' HEADERS {{ 'accept' = \
             'application/json', 'authorization' = CLIENT SECRET 'secret.api_token' }} {TAIL}"
        );
        let Some(Model::Ingestor(ingestor)) = created_model(&statement) else {
            panic!("the statement creates an ingestor: {statement}");
        };
        let Some(IngestorEntity::Http(nervix_models::HttpIngestor::Request(source))) =
            ingestor.input.transport_entity()
        else {
            panic!("the statement reads HTTP REQUEST");
        };
        assert_eq!(
            source.headers,
            [
                HttpRequestHeader {
                    name: "accept".to_string(),
                    value: HttpRequestHeaderValue::Literal("application/json".to_string()),
                },
                HttpRequestHeader {
                    name: "authorization".to_string(),
                    value: HttpRequestHeaderValue::ClientSecret("secret.api_token".to_string()),
                },
            ]
        );
    }

    /// `SHOW CREATE` prints the reference a statement wrote, so the printed statement reads back
    /// as the same Model and states no secret.
    #[test]
    fn a_client_secret_header_renders_as_its_reference_and_reads_back() {
        let statement = format!(
            "CREATE HTTP REQUEST INGESTOR events FROM source METHOD 'GET' HEADERS {{ 'accept' = \
             'application/json', 'authorization' = CLIENT SECRET 'secret.api_token' }} {TAIL}"
        );
        let model = created_model(&statement).expect("the statement creates an ingestor");
        let rendered = model
            .to_canonical_plexus()
            .expect("an ingestor renders canonically");
        assert!(
            rendered.contains(
                "HEADERS { 'accept' = 'application/json', 'authorization' = CLIENT SECRET \
                 'secret.api_token' }"
            ),
            "{rendered}"
        );
        assert_eq!(created_model(&rendered), Some(model));
    }

    #[test]
    fn polling_request_requires_a_method_and_a_literal_or_secret_for_each_header() {
        for body in [
            "",
            "METHOD 'GET' HEADERS { 'accept' = message.value }",
            "METHOD 'GET' HEADERS { accept = 'application/json' }",
            "METHOD 'GET' HEADERS { 'authorization' = CLIENT SECRET }",
            "METHOD 'GET' HEADERS { 'authorization' = CLIENT SECRET api_token }",
            "METHOD 'GET' HEADERS { 'authorization' = SECRET 'secret.api_token' }",
            "METHOD 'GET' HEADERS { 'authorization' = CLIENT 'secret.api_token' }",
        ] {
            let statement =
                format!("CREATE HTTP REQUEST INGESTOR events FROM source {body} {TAIL}");
            assert!(refused(&statement), "{statement}");
        }
    }

    #[test]
    fn a_polling_header_completes_a_literal_or_a_client_secret_as_one_phrase() {
        assert_offers_exactly(
            "CREATE HTTP REQUEST INGESTOR events FROM source METHOD 'GET' HEADERS { 'accept' = ",
            &["string_literal", "CLIENT SECRET"],
        );
        assert_offers_exactly(
            "CREATE HTTP REQUEST INGESTOR events FROM source METHOD 'GET' HEADERS { 'accept' = \
             CLIENT SECRET ",
            &["client_secret_key"],
        );
    }

    #[test]
    fn polling_request_completes_its_declared_options() {
        assert_offers_exactly(
            "CREATE HTTP REQUEST INGESTOR events FROM source ",
            &["METHOD 'GET'"],
        );
        assert_offers_exactly(
            "CREATE HTTP REQUEST INGESTOR events FROM source METHOD 'GET' ",
            &[
                "HEADERS { 'accept' = 'application/json' }",
                "MAX BODY SIZE 1MiB",
                "EVERY 1s",
            ],
        );
    }
}
