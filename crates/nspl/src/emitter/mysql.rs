//! MYSQL emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of MYSQL emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

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
pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], MySqlTablePublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(MySqlTablePublishingMode)
}
pub(super) fn table_body<'src>()
-> impl Parser<'src, &'src [Token], ParsedDestination<MySqlTableSink>, extra::Err<ParseError<'src>>>
+ Clone {
    values_destination(
        client_ref()
            .then_ignore(kw(Identifier::Table))
            .then(external_name::<MySqlTableName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .then_ignore(kw(Identifier::Values))
            .then(clickhouse_values())
            .then(mysql_conflict_action())
            .try_map(
                |((((client, table), mode), values), conflict_action), span| {
                    let _ = span;
                    Ok(MySqlTableSink {
                        client,
                        table,
                        mode,
                        values,
                        conflict_action,
                    })
                },
            ),
    )
}
