//! Mongodb emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Mongodb emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mongodb_conflict_action<'src>()
-> impl Parser<'src, &'src [Token], MongoDbConflictAction, extra::Err<ParseError<'src>>> + Clone {
    on_conflict_phrase()
        .ignore_then(conflict_target())
        .then_ignore(kw(Identifier::Do))
        .then(choice((
            kw(Identifier::Nothing).to(ConflictVerb::DoNothing),
            kw(Identifier::Update).to(ConflictVerb::DoUpdate),
        )))
        .map(|(target, verb)| match verb {
            ConflictVerb::DoNothing => MongoDbConflictAction::DoNothing { target },
            ConflictVerb::DoUpdate => MongoDbConflictAction::DoUpdate { target },
        })
        .or_not()
        .map(|action| action.unwrap_or(MongoDbConflictAction::None))
}

pub(super) fn validate_mongodb_conflict_action<'src>(
    values: &[ClickHouseValueMapping],
    conflict_action: &MongoDbConflictAction,
    span: chumsky::span::SimpleSpan,
) -> Result<(), ParseError<'src>> {
    let target = match conflict_action {
        MongoDbConflictAction::None => return Ok(()),
        MongoDbConflictAction::DoNothing { target }
        | MongoDbConflictAction::DoUpdate { target } => target,
    };
    let mapped_columns = values
        .iter()
        .map(|mapping| mapping.column.as_str())
        .collect::<HashSet<_>>();
    for column in target {
        if !mapped_columns.contains(column.as_str()) {
            return Err(Rich::custom(
                span,
                format!("MongoDB ON CONFLICT target column '{column}' is not mapped in VALUES"),
            ));
        }
    }
    if let MongoDbConflictAction::DoUpdate { target } = conflict_action {
        let target_columns = target.iter().map(String::as_str).collect::<HashSet<_>>();
        let has_update_column = values
            .iter()
            .any(|mapping| !target_columns.contains(mapping.column.as_str()));
        if !has_update_column {
            return Err(Rich::custom(
                span,
                "MongoDB ON CONFLICT DO UPDATE requires at least one non-conflict VALUES field to \
                 update",
            ));
        }
    }
    Ok(())
}

pub(super) fn mongodb_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mongodb)
        .ignore_then(client_ref())
        .then_ignore(kw_phrase2(Identifier::Insert, Identifier::To))
        .then_ignore(kw(Identifier::Collection))
        .then(collection_ref())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(mongodb_conflict_action())
        .try_map(|(((client, collection), values), conflict_action), span| {
            validate_mongodb_conflict_action(&values, &conflict_action, span)?;
            Ok(EmitSink::MongoDb(MongoDbSink::Collection(
                MongoDbCollectionSink {
                    client,
                    collection,
                    values,
                    conflict_action,
                },
            )))
        })
}
