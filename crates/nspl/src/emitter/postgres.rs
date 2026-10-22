//! POSTGRES emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of POSTGRES emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

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
pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], PostgresTablePublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(PostgresTablePublishingMode)
}
pub(super) fn table_body<'src>()
-> impl Parser<'src, &'src [Token], ParsedDestination<PostgresTableSink>, extra::Err<ParseError<'src>>>
+ Clone {
    values_destination(
        client_ref()
            .then_ignore(kw(Identifier::Table))
            .then(external_name::<PostgresTableName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .then_ignore(kw(Identifier::Values))
            .then(clickhouse_values())
            .then(postgres_conflict_action())
            .try_map(
                |((((client, table), mode), values), conflict_action), span| {
                    if let PostgresConflictAction::DoUpdate { target } = &conflict_action
                        && !values
                            .iter()
                            .any(|mapping| !target.contains(&mapping.column))
                    {
                        return Err(Rich::custom(
                            span,
                            "Postgres ON CONFLICT DO UPDATE requires at least one non-conflict \
                             VALUES column to update",
                        ));
                    }
                    Ok(PostgresTableSink {
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
