//! MONGODB emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of MONGODB emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

/// The conflict action a body writes: its `ON CONFLICT` clause, or the ordinary insert where it
/// writes none.
pub(super) fn mongodb_conflict_action<'src>()
-> impl Parser<'src, &'src [Token], MongoDbConflictAction, extra::Err<ParseError<'src>>> + Clone {
    mongodb_conflict_clause()
        .or_not()
        .map(|action| action.unwrap_or(MongoDbConflictAction::None))
}

/// `ON CONFLICT (<field>, …) DO NOTHING|UPDATE`, as a body or `SET` writes it.
fn mongodb_conflict_clause<'src>()
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
}
pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], MongoDbCollectionPublishingMode, extra::Err<ParseError<'src>>>
+ Clone {
    request_mode!(MongoDbCollectionPublishingMode)
}
pub(super) fn collection_body<'src>() -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<MongoDbCollectionEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    values_destination(
        client_ref()
            .then_ignore(kw(Identifier::Collection))
            .then(external_name::<MongoDbCollectionName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .then_ignore(kw(Identifier::Values))
            .then(clickhouse_values())
            .then(mongodb_conflict_action())
            .try_map(
                |((((client, collection), mode), values), conflict_action), span| {
                    conflict_action
                        .validate(&values)
                        .map_err(|error| Rich::custom(span, error))?;
                    Ok(MongoDbCollectionEmitter {
                        client,
                        collection,
                        mode,
                        values,
                        conflict_action,
                    })
                },
            ),
    )
}

/// `SET COLLECTION '<name>'`, `SET ON CONFLICT …`, `DROP ON CONFLICT` or `SET MODE <mode>`:
/// one part of a `MONGODB COLLECTION` body.
pub(super) fn change<'src>()
-> impl Parser<'src, &'src [Token], MongoDbCollectionEmitterChange, extra::Err<ParseError<'src>>> + Clone
{
    let locator = kw(Identifier::Collection)
        .ignore_then(external_name::<MongoDbCollectionName>())
        .map(MongoDbCollectionEmitterChange::Collection);
    let conflict = mongodb_conflict_clause().map(MongoDbCollectionEmitterChange::OnConflict);
    let publishing = kw(Identifier::Mode)
        .ignore_then(mode())
        .map(MongoDbCollectionEmitterChange::Mode);
    let set = kw(Identifier::Set).ignore_then(choice((locator, conflict, publishing)));
    let drop_conflict = kw(Identifier::Drop).ignore_then(on_conflict_phrase()).to(
        MongoDbCollectionEmitterChange::OnConflict(MongoDbConflictAction::None),
    );
    choice((set, drop_conflict)).boxed()
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns MongoDB grammar conformance")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Current collection, conflict and completion examples.
    //! - **Depends on.** The composed emitter grammar and semantic Models.
    //! - **Must not know.** Drivers or graph execution.

    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn mongodb_collection_names_and_conflicts_follow_the_declaration() {
        for action in [
            "",
            "ON CONFLICT ('id') DO NOTHING",
            "ON CONFLICT ('id') DO UPDATE",
        ] {
            let source = format!(
                "CREATE MONGODB COLLECTION EMITTER output FROM events TO mongo_client COLLECTION \
                 'Orders café' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES {{ 'id' = \
                 input.id, 'value' = input.value }} {action} BATCH MAX MESSAGES 2 MAX SIZE 1MiB \
                 FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            );
            let parsed =
                parse_create_emitter(&source).assured("the declared collection body parses");
            let canonical = parsed
                .to_canonical_plexus()
                .assured("the current model renders");
            assert_eq!(
                parse_create_emitter(&canonical).assured("the rendered model parses"),
                parsed
            );
        }
    }

    #[test]
    fn mongodb_conflict_targets_are_required_and_mapped() {
        for action in [
            "ON CONFLICT DO NOTHING",
            "ON CONFLICT ('missing') DO NOTHING",
            "ON CONFLICT ('id') DO UPDATE",
        ] {
            let source = format!(
                "CREATE MONGODB COLLECTION EMITTER output FROM events TO mongo_client COLLECTION \
                 'Orders' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES {{ 'id' = input.id }} \
                 {action} BATCH MAX MESSAGES 2 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG \
                 ON GENERAL ERROR LOG;"
            );
            assert!(parse_create_emitter(&source).is_err(), "{source}");
        }
    }

    #[test]
    fn mongodb_completion_uses_its_declared_locator_and_mode() {
        for (prefix, expected) in [
            (
                "CREATE MONGODB COLLECTION EMITTER output FROM events TO mongo_client ",
                "COLLECTION",
            ),
            (
                "CREATE MONGODB COLLECTION EMITTER output FROM events TO mongo_client COLLECTION ",
                "mongodb_collection",
            ),
            (
                "CREATE MONGODB COLLECTION EMITTER output FROM events TO mongo_client COLLECTION \
                 'Orders' MODE ",
                "ACK",
            ),
        ] {
            let suggestions = suggest_create_emitter(prefix, prefix.len());
            assert!(
                suggestions.iter().any(|item| item == expected),
                "{suggestions:?}"
            );
        }
    }
}
