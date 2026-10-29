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
        .map(|(target, verb)| {
            // No written target is an empty one, which the declaration's rule refuses for an
            // update once the destination knows its mapped columns.
            let target = target.unwrap_or_default();
            match verb {
                ConflictVerb::DoNothing => PostgresConflictAction::DoNothing { target },
                ConflictVerb::DoUpdate => PostgresConflictAction::DoUpdate { target },
            }
        })
        .or_not()
        .map(|action| action.unwrap_or(PostgresConflictAction::None))
}
pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], PostgresTablePublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(PostgresTablePublishingMode)
}
pub(super) fn table_body<'src>() -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<PostgresTableEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
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
                    conflict_action
                        .validate(&values)
                        .map_err(|error| Rich::custom(span, error))?;
                    Ok(PostgresTableEmitter {
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the composed Postgres destination grammar")
)]
mod tests {
    //! Harness layer.
    //! Owns: current Postgres table-path and conflict-clause parsing, canonical rendering and
    //! completion checks.
    //! Depends on: the composed emitter grammar and public vocabulary.
    //! Must not know: drivers or runtime insertion.

    use super::super::{ParseFromSourceError, parse_create_emitter, suggest_create_emitter};

    fn statement(table: &str, values: &str, conflict: &str) -> String {
        format!(
            "CREATE POSTGRES TABLE EMITTER outgoing FROM events TO db TABLE '{table}' MODE ACK \
             RETRY POLICY BACKOFF 10ms MAX 1s VALUES {{ {values} }} {conflict} BATCH MAX MESSAGES \
             2 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    #[test]
    fn postgres_table_literals_preserve_each_path_part() {
        for table in ["Orders", "sales.Orders", "Sales-1.Orders_2"] {
            let source = statement(
                table,
                "'seq' = input.seq, 'note' = input.note",
                "ON CONFLICT ('seq') DO UPDATE",
            );
            let model = parse_create_emitter(&source).expect("a current table path parses");
            let canonical = model.to_canonical_plexus().expect("the emitter formats");
            assert!(canonical.contains(&format!("TABLE '{table}'")));
            assert_eq!(
                parse_create_emitter(&canonical).expect("canonical table path parses"),
                model
            );
        }
    }

    #[test]
    fn postgres_table_literals_refuse_empty_or_excess_parts() {
        for table in [".Orders", "sales.", "sales..Orders", "region.sales.Orders"] {
            let source = statement(table, "'seq' = input.seq", "");
            assert!(parse_create_emitter(&source).is_err(), "{table}");
        }
    }

    #[test]
    fn postgres_conflict_clauses_refuse_what_one_insert_cannot_write() {
        for (values, conflict, refusal) in [
            (
                "'seq' = input.seq, 'note' = input.note",
                "ON CONFLICT DO UPDATE",
                "requires a conflict target",
            ),
            (
                "'seq' = input.seq",
                "ON CONFLICT ('seq') DO UPDATE",
                "requires a VALUES column outside its conflict target",
            ),
            (
                "'seq' = input.seq",
                "ON CONFLICT ('') DO NOTHING",
                "empty column name",
            ),
        ] {
            let source = statement("sales.Orders", values, conflict);
            let error = parse_create_emitter(&source).expect_err("the clause cannot be written");
            let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
                panic!("expected a parse refusal, got {error:?}");
            };
            assert!(
                diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.message.contains(refusal)),
                "{conflict}: {diagnostics:?}"
            );
        }
        for (values, conflict) in [
            ("'seq' = input.seq", "ON CONFLICT DO NOTHING"),
            ("'seq' = input.seq", "ON CONFLICT ('seq') DO NOTHING"),
            (
                "'seq' = input.seq",
                "ON CONFLICT ('Seq', 'tenant') DO UPDATE",
            ),
        ] {
            let source = statement("sales.Orders", values, conflict);
            parse_create_emitter(&source).expect("the clause names what its insert writes");
        }
    }

    #[test]
    fn postgres_table_completion_stays_in_the_composed_destination() {
        let head = "CREATE POSTGRES TABLE EMITTER outgoing FROM events TO db TABLE ";
        let suggestions = suggest_create_emitter(head, head.len());
        assert_eq!(suggestions, vec!["postgres_table".to_string()]);
        let path = format!("{head}'sales.Orders' ");
        let suggestions = suggest_create_emitter(&path, path.len());
        assert_eq!(suggestions, vec!["MODE".to_string()]);
    }
}
