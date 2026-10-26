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
-> impl Parser<'src, &'src [Token], ParsedDestination<MySqlTableEmitter>, extra::Err<ParseError<'src>>>
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
                    Ok(MySqlTableEmitter {
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
    nervix::layer(harness, reason = "tests the composed MySQL destination grammar")
)]
mod tests {
    //! Harness layer.
    //! Owns: current MySQL table-path parsing, canonical rendering and completion checks.
    //! Depends on: the composed emitter grammar and public vocabulary.
    //! Must not know: drivers or runtime insertion.

    use super::super::{parse_create_emitter, suggest_create_emitter};

    #[test]
    fn mysql_table_literals_preserve_each_path_part() {
        for table in ["Orders", "Shop.Orders", "shop-1.Orders_2"] {
            let source = format!(
                "CREATE MYSQL TABLE EMITTER outgoing FROM events TO db TABLE '{table}' MODE ACK \
                 RETRY POLICY BACKOFF 10ms MAX 1s VALUES {{ 'seq' = input.seq }} ON CONFLICT DO \
                 UPDATE BATCH MAX MESSAGES 2 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG \
                 ON GENERAL ERROR LOG;"
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
    fn mysql_table_literals_refuse_empty_or_excess_parts() {
        for table in [".Orders", "Shop.", "Shop..Orders", "region.Shop.Orders"] {
            let source = format!(
                "CREATE MYSQL TABLE EMITTER outgoing FROM events TO db TABLE '{table}' MODE ACK \
                 RETRY POLICY BACKOFF 10ms MAX 1s VALUES {{ 'seq' = input.seq }} BATCH MAX \
                 MESSAGES 2 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR \
                 LOG;"
            );
            assert!(parse_create_emitter(&source).is_err(), "{table}");
        }
    }

    #[test]
    fn mysql_table_completion_stays_in_the_composed_destination() {
        let head = "CREATE MYSQL TABLE EMITTER outgoing FROM events TO db TABLE ";
        let suggestions = suggest_create_emitter(head, head.len());
        assert_eq!(suggestions, vec!["mysql_table".to_string()]);
        let path = format!("{head}'Shop.Orders' ");
        let suggestions = suggest_create_emitter(&path, path.len());
        assert_eq!(suggestions, vec!["MODE".to_string()]);
    }
}
