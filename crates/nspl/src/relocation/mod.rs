use chumsky::prelude::*;
use nervix_models::{
    ModelKind, Relocation, RelocationMember, RelocationPreferenceOverride,
    RelocationPreferenceStrategy, RelocationSelection,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, boxed_choice, correlator_ref, deduplicator_ref,
        emitter_ref, generator_ref, inferencer_ref, ingestor_ref, into_parse_error, junction_ref,
        kw, kw_phrase2, lex_input, lookup_ref, node_id, reingestor_ref, relay_ref, reorderer_ref,
        suggest_from, tok, wasm_processor_ref, window_processor_ref,
    },
};

/// One kind-qualified runtime node. Only relocatable kinds are offered, because a relocation
/// moves scheduled runtime nodes and a name is unique only within its kind.
fn relocation_member<'src>()
-> impl Parser<'src, &'src [Token], RelocationMember, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        kw(Identifier::Ingestor)
            .ignore_then(ingestor_ref())
            .map(|name| RelocationMember::new(ModelKind::Ingestor, name)),
        kw(Identifier::Reingestor)
            .ignore_then(reingestor_ref())
            .map(|name| RelocationMember::new(ModelKind::Reingestor, name)),
        kw(Identifier::Generator)
            .ignore_then(generator_ref())
            .map(|name| RelocationMember::new(ModelKind::Generator, name)),
        kw(Identifier::Junction)
            .ignore_then(junction_ref())
            .map(|name| RelocationMember::new(ModelKind::Junction, name)),
        kw(Identifier::Deduplicator)
            .ignore_then(deduplicator_ref())
            .map(|name| RelocationMember::new(ModelKind::Deduplicator, name)),
        kw(Identifier::Correlator)
            .ignore_then(correlator_ref())
            .map(|name| RelocationMember::new(ModelKind::Correlator, name)),
        kw(Identifier::Reorderer)
            .ignore_then(reorderer_ref())
            .map(|name| RelocationMember::new(ModelKind::Reorderer, name)),
        kw_phrase2(Identifier::Window, Identifier::Processor)
            .ignore_then(window_processor_ref())
            .map(|name| RelocationMember::new(ModelKind::WindowProcessor, name)),
        kw(Identifier::Inferencer)
            .ignore_then(inferencer_ref())
            .map(|name| RelocationMember::new(ModelKind::Inferencer, name)),
        kw_phrase2(Identifier::Wasm, Identifier::Processor)
            .ignore_then(wasm_processor_ref())
            .map(|name| RelocationMember::new(ModelKind::WasmProcessor, name)),
        kw(Identifier::Emitter)
            .ignore_then(emitter_ref())
            .map(|name| RelocationMember::new(ModelKind::Emitter, name)),
        kw_phrase2(Identifier::Hash, Identifier::Map)
            .ignore_then(lookup_ref())
            .map(|name| RelocationMember::new(ModelKind::Lookup, name)),
        kw(Identifier::Relay)
            .ignore_then(relay_ref())
            .map(|name| RelocationMember::new(ModelKind::Relay, name)),
    )
}

fn relocation_members<'src>()
-> impl Parser<'src, &'src [Token], Vec<RelocationMember>, extra::Err<ParseError<'src>>> + Clone {
    relocation_member()
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
        .boxed()
}

fn relocation_selection<'src>()
-> impl Parser<'src, &'src [Token], RelocationSelection, extra::Err<ParseError<'src>>> + Clone {
    let corridor = kw(Identifier::From)
        .ignore_then(
            relocation_member()
                .then(
                    tok(Token::Comma)
                        .ignore_then(relocation_member())
                        .repeated()
                        .collect::<Vec<_>>(),
                )
                .map(|(first, mut rest)| {
                    rest.insert(0, first);
                    rest
                }),
        )
        .then_ignore(kw(Identifier::To))
        .then(relocation_members())
        .map(|(from, to)| RelocationSelection::Corridor { from, to });

    boxed_choice!(
        corridor,
        relocation_members().map(RelocationSelection::List)
    )
}

fn preference_strategy<'src>()
-> impl Parser<'src, &'src [Token], RelocationPreferenceStrategy, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        kw_phrase2(Identifier::Follow, Identifier::Preferences)
            .to(RelocationPreferenceStrategy::Follow),
        kw_phrase2(Identifier::Ignore, Identifier::Preferences)
            .to(RelocationPreferenceStrategy::Ignore),
    ))
    .boxed()
}

fn preference_override<'src>()
-> impl Parser<'src, &'src [Token], RelocationPreferenceOverride, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::For)
        .ignore_then(relocation_member())
        .then(preference_strategy())
        .map(|(member, strategy)| RelocationPreferenceOverride { member, strategy })
        .boxed()
}

/// The clauses `RELOCATE` and `DESCRIBE RELOCATION` share, so a plan can be inspected and then
/// executed by changing one word.
fn relocation_clauses<'src>()
-> impl Parser<'src, &'src [Token], Relocation, extra::Err<ParseError<'src>>> + Clone {
    relocation_selection()
        .then_ignore(kw_phrase2(Identifier::Onto, Identifier::Node))
        .then(node_id())
        .then(preference_strategy())
        .then(
            preference_override()
                .separated_by(tok(Token::Comma).or_not())
                .collect::<Vec<_>>(),
        )
        .map(
            |(((selection, destination), strategy), overrides)| Relocation {
                selection,
                destination,
                strategy,
                overrides,
            },
        )
        .boxed()
}

pub fn relocate_parser<'src>()
-> impl Parser<'src, &'src [Token], Relocation, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Relocate)
        .ignore_then(relocation_clauses())
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

pub fn describe_relocation_parser<'src>()
-> impl Parser<'src, &'src [Token], Relocation, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Relocation))
        .ignore_then(relocation_clauses())
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

pub fn parse_relocate(input: &str) -> Result<Relocation, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    let out = relocate_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if out.has_errors() {
        Err(into_parse_error(
            source,
            &spanned_tokens,
            input.len(),
            out.into_errors(),
        ))
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_describe_relocation(input: &str) -> Result<Relocation, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    let out = describe_relocation_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if out.has_errors() {
        Err(into_parse_error(
            source,
            &spanned_tokens,
            input.len(),
            out.into_errors(),
        ))
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn suggest_relocate(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, relocate_parser())
}

pub fn suggest_describe_relocation(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_relocation_parser())
}

#[cfg(test)]
mod tests {
    use nervix_models::Statement;

    use super::*;
    use crate::statement::{parse_statement, suggest_statement};

    fn member(kind: ModelKind, name: &str) -> RelocationMember {
        RelocationMember::new(
            kind,
            nervix_models::Identifier::try_from(name)
                .expect("test name must be a valid identifier"),
        )
    }

    #[test]
    fn parses_list_selection_with_every_relocatable_kind() {
        let relocation = parse_relocate(
            "RELOCATE INGESTOR kafka_orders, REINGESTOR replay, GENERATOR clock, JUNCTION merge, \
             DEDUPLICATOR dedup_txns, CORRELATOR pair, REORDERER order, WINDOW PROCESSOR latency, \
             INFERENCER score, WASM PROCESSOR enrich, EMITTER archive, HASH MAP zip_codes, RELAY \
             normalized ONTO NODE node-2 FOLLOW PREFERENCES;",
        )
        .expect("relocation must parse");

        let RelocationSelection::List(members) = &relocation.selection else {
            panic!("expected a list selection");
        };
        assert_eq!(
            members,
            &vec![
                member(ModelKind::Ingestor, "kafka_orders"),
                member(ModelKind::Reingestor, "replay"),
                member(ModelKind::Generator, "clock"),
                member(ModelKind::Junction, "merge"),
                member(ModelKind::Deduplicator, "dedup_txns"),
                member(ModelKind::Correlator, "pair"),
                member(ModelKind::Reorderer, "order"),
                member(ModelKind::WindowProcessor, "latency"),
                member(ModelKind::Inferencer, "score"),
                member(ModelKind::WasmProcessor, "enrich"),
                member(ModelKind::Emitter, "archive"),
                member(ModelKind::Lookup, "zip_codes"),
                member(ModelKind::Relay, "normalized"),
            ]
        );
        assert_eq!(relocation.destination, "node-2");
        assert_eq!(relocation.strategy, RelocationPreferenceStrategy::Follow);
        assert!(relocation.overrides.is_empty());
    }

    #[test]
    fn parses_corridor_selection_with_overrides() {
        let relocation = parse_relocate(
            "RELOCATE FROM JUNCTION feature_normalizer, INGESTOR kafka_orders TO JUNCTION \
             risk_scorer ONTO NODE nervix-1.internal FOLLOW PREFERENCES FOR DEDUPLICATOR \
             dedup_txns IGNORE PREFERENCES, FOR EMITTER archive FOLLOW PREFERENCES;",
        )
        .expect("corridor relocation must parse");

        let RelocationSelection::Corridor { from, to } = &relocation.selection else {
            panic!("expected a corridor selection");
        };
        assert_eq!(
            from,
            &vec![
                member(ModelKind::Junction, "feature_normalizer"),
                member(ModelKind::Ingestor, "kafka_orders"),
            ]
        );
        assert_eq!(to, &vec![member(ModelKind::Junction, "risk_scorer")]);
        assert_eq!(relocation.destination, "nervix-1.internal");
        assert_eq!(relocation.strategy, RelocationPreferenceStrategy::Follow);
        assert_eq!(
            relocation.overrides,
            vec![
                RelocationPreferenceOverride {
                    member: member(ModelKind::Deduplicator, "dedup_txns"),
                    strategy: RelocationPreferenceStrategy::Ignore,
                },
                RelocationPreferenceOverride {
                    member: member(ModelKind::Emitter, "archive"),
                    strategy: RelocationPreferenceStrategy::Follow,
                },
            ]
        );
    }

    #[test]
    fn parses_overrides_without_separating_commas() {
        let relocation = parse_relocate(
            "RELOCATE JUNCTION merge ONTO NODE node-2 IGNORE PREFERENCES FOR JUNCTION merge \
             FOLLOW PREFERENCES FOR EMITTER archive IGNORE PREFERENCES;",
        )
        .expect("relocation must parse");
        assert_eq!(relocation.overrides.len(), 2);
    }

    #[test]
    fn parses_describe_relocation_with_the_same_clauses() {
        let describe = parse_describe_relocation(
            "DESCRIBE RELOCATION FROM INGESTOR kafka_orders TO EMITTER archive_emitter ONTO NODE \
             nervix-1.internal IGNORE PREFERENCES;",
        )
        .expect("describe relocation must parse");
        assert_eq!(describe.destination, "nervix-1.internal");
        assert_eq!(describe.strategy, RelocationPreferenceStrategy::Ignore);
    }

    #[test]
    fn rejects_unqualified_members_and_non_relocatable_kinds() {
        parse_relocate("RELOCATE risk_scorer ONTO NODE node-2 FOLLOW PREFERENCES;")
            .expect_err("an unqualified member must not parse");
        parse_relocate("RELOCATE SCHEMA payload ONTO NODE node-2 FOLLOW PREFERENCES;")
            .expect_err("a non-relocatable kind must not parse");
        parse_relocate("RELOCATE ENDPOINT ingress ONTO NODE node-2 FOLLOW PREFERENCES;")
            .expect_err("an endpoint is not a runtime node");
    }

    #[test]
    fn rejects_missing_strategy_destination_and_wrong_destination_keyword() {
        parse_relocate("RELOCATE JUNCTION risk_scorer ONTO NODE node-2;")
            .expect_err("the default strategy is mandatory");
        parse_relocate("RELOCATE JUNCTION risk_scorer FOLLOW PREFERENCES;")
            .expect_err("the destination is mandatory");
        parse_relocate("RELOCATE JUNCTION risk_scorer TO NODE node-2 FOLLOW PREFERENCES;")
            .expect_err("the destination clause is ONTO NODE");
    }

    #[test]
    fn top_level_statement_parses_both_verbs() {
        assert!(matches!(
            parse_statement("RELOCATE JUNCTION risk_scorer ONTO NODE node-2 FOLLOW PREFERENCES;"),
            Ok(Statement::Relocate(_))
        ));
        assert!(matches!(
            parse_statement(
                "DESCRIBE RELOCATION JUNCTION risk_scorer ONTO NODE node-2 IGNORE PREFERENCES;"
            ),
            Ok(Statement::DescribeRelocation(_))
        ));
    }

    #[test]
    fn completes_selection_heads_without_cross_statement_leakage() {
        let input = "RELOCATE ";
        let suggestions = suggest_relocate(input, input.len());
        for expected in [
            "FROM",
            "INGESTOR",
            "REINGESTOR",
            "GENERATOR",
            "JUNCTION",
            "DEDUPLICATOR",
            "CORRELATOR",
            "REORDERER",
            "WINDOW PROCESSOR",
            "INFERENCER",
            "WASM PROCESSOR",
            "EMITTER",
            "HASH MAP",
            "RELAY",
        ] {
            assert!(
                suggestions.contains(&expected.to_string()),
                "expected '{expected}' in {suggestions:?}"
            );
        }
        assert!(!suggestions.contains(&"SCHEMA".to_string()));
        assert!(!suggestions.contains(&"ENDPOINT".to_string()));
        assert!(!suggestions.contains(&"NODE".to_string()));
    }

    #[test]
    fn completes_kind_specific_name_references() {
        let input = "RELOCATE DEDUPLICATOR ";
        assert_eq!(
            suggest_relocate(input, input.len()),
            vec!["ref:deduplicator".to_string()]
        );

        let input = "RELOCATE FROM JUNCTION a TO HASH MAP ";
        assert_eq!(
            suggest_relocate(input, input.len()),
            vec!["ref:lookup".to_string()]
        );
    }

    #[test]
    fn completes_corridor_destination_and_strategy_phrases() {
        let input = "RELOCATE FROM JUNCTION feature_normalizer ";
        let suggestions = suggest_relocate(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"ONTO NODE".to_string()));

        let input = "RELOCATE JUNCTION risk_scorer ";
        let suggestions = suggest_relocate(input, input.len());
        assert!(suggestions.contains(&"ONTO NODE".to_string()));
        assert!(!suggestions.contains(&"FOLLOW PREFERENCES".to_string()));

        let input = "RELOCATE JUNCTION risk_scorer ONTO NODE node-2 ";
        let suggestions = suggest_relocate(input, input.len());
        assert!(suggestions.contains(&"FOLLOW PREFERENCES".to_string()));
        assert!(suggestions.contains(&"IGNORE PREFERENCES".to_string()));
        assert!(!suggestions.contains(&"FOR".to_string()));

        // A relocation without `FOR` overrides already parses, so the optional tail is offered by
        // the composed top-level grammar rather than derived from a failing parse.
        let input = "RELOCATE JUNCTION risk_scorer ONTO NODE node-2 FOLLOW PREFERENCES ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"FOR".to_string()));
        assert!(suggestions.contains(&";".to_string()));

        let input = "RELOCATE JUNCTION risk_scorer ONTO NODE node-2 FOLLOW PREFERENCES FOR \
                     EMITTER archive ";
        let suggestions = suggest_relocate(input, input.len());
        assert!(suggestions.contains(&"FOLLOW PREFERENCES".to_string()));
        assert!(suggestions.contains(&"IGNORE PREFERENCES".to_string()));
    }

    #[test]
    fn completes_describe_relocation_from_the_top_level_grammar() {
        let suggestions = suggest_statement("DESCRIBE RELOCA", 15);
        assert!(suggestions.contains(&"RELOCATION".to_string()));

        let input = "DESCRIBE RELOCATION ";
        let suggestions = suggest_describe_relocation(input, input.len());
        assert!(suggestions.contains(&"FROM".to_string()));
        assert!(suggestions.contains(&"JUNCTION".to_string()));
        assert!(!suggestions.contains(&"PLACEMENT".to_string()));
    }

    #[test]
    fn canonical_form_uppercases_keywords_and_keeps_written_order() {
        let statement = parse_statement(
            "relocate from junction feature_normalizer to junction risk_scorer, deduplicator \
             dedup_txns onto node node-2 follow preferences for deduplicator dedup_txns ignore \
             preferences;",
        )
        .expect("lowercase relocation must parse");
        assert_eq!(
            statement
                .to_canonical_nspl()
                .expect("relocation must render"),
            "RELOCATE FROM JUNCTION feature_normalizer TO JUNCTION risk_scorer, DEDUPLICATOR \
             dedup_txns ONTO NODE node-2 FOLLOW PREFERENCES FOR DEDUPLICATOR dedup_txns IGNORE \
             PREFERENCES;"
        );

        let statement = parse_statement(
            "describe relocation hash map zip_codes onto node nervix-1.internal ignore \
             preferences;",
        )
        .expect("describe relocation must parse");
        assert_eq!(
            statement
                .to_canonical_nspl()
                .expect("describe relocation must render"),
            "DESCRIBE RELOCATION HASH MAP zip_codes ONTO NODE nervix-1.internal IGNORE \
             PREFERENCES;"
        );
    }
}
