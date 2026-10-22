//! Mysql emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Mysql emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mysql_conflict_action<'src>()
-> impl Parser<'src, &'src [Token], MySqlConflictAction, extra::Err<ParseError<'src>>> + Clone {
    on_conflict_phrase()
        .ignore_then(kw(Identifier::Do))
        .ignore_then(choice((
            kw(Identifier::Nothing).to(MySqlConflictAction::DoNothing),
            kw(Identifier::Update).to(MySqlConflictAction::DoUpdate),
        )))
        .or_not()
        .map(|action| action.unwrap_or(MySqlConflictAction::None))
}

pub(super) fn mysql_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mysql)
        .ignore_then(client_ref())
        .then_ignore(kw_phrase2(Identifier::Insert, Identifier::To))
        .then_ignore(kw(Identifier::Table))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(mysql_conflict_action())
        .map(|(((client, table), values), conflict_action)| {
            EmitSink::MySql(MySqlSink::Table(MySqlTableSink {
                client,
                table,
                values,
                conflict_action,
            }))
        })
}
