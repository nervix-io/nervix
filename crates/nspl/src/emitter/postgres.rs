//! Postgres emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Postgres emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn postgres_conflict_action<'src>()
-> impl Parser<'src, &'src [Token], PostgresConflictAction, extra::Err<ParseError<'src>>> + Clone {
    on_conflict_phrase()
        .ignore_then(conflict_target().or_not())
        .then_ignore(kw(Identifier::Do))
        .then(choice((
            kw(Identifier::Nothing).to(ConflictVerb::DoNothing),
            kw(Identifier::Update).to(ConflictVerb::DoUpdate),
        )))
        .try_map(|(target, verb), span| match verb {
            ConflictVerb::DoNothing => Ok(PostgresConflictAction::DoNothing {
                target: target.unwrap_or_default(),
            }),
            ConflictVerb::DoUpdate => match target {
                Some(target) => Ok(PostgresConflictAction::DoUpdate { target }),
                None => Err(Rich::custom(
                    span,
                    "Postgres ON CONFLICT DO UPDATE requires a conflict target",
                )),
            },
        })
        .or_not()
        .map(|action| action.unwrap_or(PostgresConflictAction::None))
}

pub(super) fn postgres_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Postgres)
        .ignore_then(client_ref())
        .then_ignore(kw_phrase2(Identifier::Insert, Identifier::To))
        .then_ignore(kw(Identifier::Table))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(postgres_conflict_action())
        .try_map(|(((client, table), values), conflict_action), span| {
            if let PostgresConflictAction::DoUpdate { target } = &conflict_action {
                let has_update_column = values
                    .iter()
                    .any(|mapping| !target.contains(&mapping.column));
                if !has_update_column {
                    return Err(Rich::custom(
                        span,
                        "Postgres ON CONFLICT DO UPDATE requires at least one non-conflict VALUES \
                         column to update",
                    ));
                }
            }
            Ok(EmitSink::Postgres(PostgresSink::Table(PostgresTableSink {
                client,
                table,
                values,
                conflict_action,
            })))
        })
}
