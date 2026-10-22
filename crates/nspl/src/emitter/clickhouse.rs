//! Clickhouse emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Clickhouse emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn clickhouse_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Clickhouse)
        .ignore_then(client_ref())
        .then_ignore(kw_phrase2(Identifier::Insert, Identifier::To))
        .then_ignore(kw(Identifier::Table))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .map(|((client, table), values)| {
            EmitSink::ClickHouse(ClickHouseSink::Table(ClickHouseTableSink {
                client,
                table,
                values,
            }))
        })
}
