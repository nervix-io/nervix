use chumsky::prelude::*;
use nervix_models::{
    AlterPlacement, AlterPlacementOperation, CreatePlacement, CreateStatement, DescribePlacement,
    PlacementPolicy, ShowPlacements,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, alter_op_separator, if_not_exists_clause,
        into_parse_error, kw, kw_phrase2, lex_input, placement_name, placement_ref,
        runtime_node_ref, suggest_from, tok, u64_value,
    },
};

pub fn placement_policy_parser<'src>()
-> impl Parser<'src, &'src [Token], PlacementPolicy, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw_phrase2(Identifier::Require, Identifier::Colocation)
            .to(PlacementPolicy::RequireColocation),
        kw_phrase2(Identifier::Prefer, Identifier::Colocation)
            .to(PlacementPolicy::PreferColocation),
        kw(Identifier::Neutral).to(PlacementPolicy::Neutral),
        kw_phrase2(Identifier::Suggest, Identifier::Separation)
            .to(PlacementPolicy::SuggestSeparation),
    ))
    .boxed()
}

fn placement_members<'src>()
-> impl Parser<'src, &'src [Token], Vec<nervix_models::Identifier>, extra::Err<ParseError<'src>>> + Clone
{
    runtime_node_ref()
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
        .boxed()
}

fn alter_placement_members<'src>()
-> impl Parser<'src, &'src [Token], Vec<nervix_models::Identifier>, extra::Err<ParseError<'src>>> + Clone
{
    runtime_node_ref()
        .then(
            tok(Token::Comma)
                .and_is(alter_op_separator().not())
                .ignore_then(runtime_node_ref())
                .repeated()
                .collect::<Vec<_>>(),
        )
        .map(|(first, mut rest)| {
            rest.insert(0, first);
            rest
        })
        .boxed()
}

fn placement_rank<'src>()
-> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Rank)
        .ignore_then(u64_value())
        .try_map(|rank, span| {
            if rank == 0 {
                Err(Rich::custom(
                    span,
                    "placement RANK 0 is invalid; RANK must be greater than zero",
                ))
            } else {
                Ok(rank)
            }
        })
        .boxed()
}

pub fn create_placement_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreatePlacement>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Placement))
        .then(placement_name())
        .then_ignore(kw(Identifier::From))
        .then(placement_members())
        .then_ignore(kw(Identifier::To))
        .then(placement_members())
        .then(placement_policy_parser())
        .then(placement_rank().or_not())
        .then_ignore(tok(Token::Semicolon).or_not())
        .try_map(
            |(((((if_not_exists, name), from), to), policy), rank), span| {
                CreatePlacement::new(name, from, to, policy, rank)
                    .map(|placement| CreateStatement::new(placement, if_not_exists))
                    .map_err(|error| Rich::custom(span, error.to_string()))
            },
        )
        .boxed()
}

pub fn alter_placement_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterPlacement, extra::Err<ParseError<'src>>> + Clone {
    let set_policy = kw(Identifier::Set)
        .ignore_then(kw(Identifier::Policy))
        .ignore_then(placement_policy_parser())
        .map(|policy| AlterPlacementOperation::SetPolicy { policy });
    let set_rank = kw(Identifier::Set)
        .ignore_then(placement_rank())
        .map(|rank| AlterPlacementOperation::SetRank { rank });
    let drop_rank = kw(Identifier::Drop)
        .ignore_then(kw(Identifier::Rank))
        .to(AlterPlacementOperation::DropRank);
    let set_members = kw(Identifier::Set)
        .ignore_then(kw(Identifier::From))
        .ignore_then(alter_placement_members())
        .then_ignore(kw(Identifier::To))
        .then(alter_placement_members())
        .map(|(from, to)| AlterPlacementOperation::SetMembers { from, to });
    let rename = kw(Identifier::Rename)
        .ignore_then(kw(Identifier::To))
        .ignore_then(placement_name())
        .map(|name| AlterPlacementOperation::RenameTo { name });
    let operation = choice((set_policy, set_rank, drop_rank, set_members, rename)).boxed();

    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Placement))
        .ignore_then(placement_ref())
        .then(
            operation
                .separated_by(alter_op_separator())
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(placement, operations)| AlterPlacement {
            placement,
            operations,
        })
        .boxed()
}

pub fn describe_placement_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribePlacement, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Placement))
        .ignore_then(placement_ref())
        .map(|name| DescribePlacement { name })
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

pub fn show_placements_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowPlacements, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Placements))
        .to(ShowPlacements)
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

pub fn parse_create_placement(
    input: &str,
) -> Result<CreateStatement<CreatePlacement>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    let out = create_placement_parser()
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

pub fn parse_alter_placement(input: &str) -> Result<AlterPlacement, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    let out = alter_placement_parser()
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

pub fn suggest_create_placement(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_placement_parser())
}

pub fn suggest_alter_placement(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, alter_placement_parser())
}

#[cfg(test)]
mod tests {
    use nervix_models::{Model, Statement};

    use super::*;
    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn parses_ranked_create_and_collapses_duplicate_members() {
        let parsed = parse_create_placement(
            "CREATE IF NOT EXISTS PLACEMENT critical FROM ingest, ingest, enrich TO score REQUIRE \
             COLOCATION RANK 1;",
        )
        .expect("placement must parse");

        assert!(parsed.if_not_exists);
        assert_eq!(parsed.name.as_str(), "critical");
        assert_eq!(
            parsed
                .from
                .iter()
                .map(nervix_models::Identifier::as_str)
                .collect::<Vec<_>>(),
            vec!["ingest", "enrich"]
        );
        assert_eq!(parsed.policy, PlacementPolicy::RequireColocation);
        assert_eq!(parsed.rank, Some(1));
    }

    #[test]
    fn parses_every_policy_head() {
        for (source, expected) in [
            ("REQUIRE COLOCATION", PlacementPolicy::RequireColocation),
            ("PREFER COLOCATION", PlacementPolicy::PreferColocation),
            ("NEUTRAL", PlacementPolicy::Neutral),
            ("SUGGEST SEPARATION", PlacementPolicy::SuggestSeparation),
        ] {
            let statement =
                parse_statement(&format!("CREATE PLACEMENT p FROM source TO sink {source};"))
                    .expect("placement policy must parse");
            let Statement::Create(create) = statement else {
                panic!("expected CREATE model");
            };
            let Model::Placement(placement) = create.body.as_ref() else {
                panic!("expected placement model");
            };
            assert_eq!(placement.policy, expected);
        }
    }

    #[test]
    fn rejects_rank_zero_with_public_diagnostic() {
        let error =
            parse_create_placement("CREATE PLACEMENT p FROM source TO sink NEUTRAL RANK 0;")
                .expect_err("rank zero must fail");
        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error");
        };
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("RANK 0"))
        );
    }

    #[test]
    fn rejects_hard_separation_and_empty_sides() {
        parse_create_placement("CREATE PLACEMENT p FROM source TO sink REQUIRE SEPARATION;")
            .expect_err("hard separation is not a policy");
        parse_create_placement("CREATE PLACEMENT p FROM TO sink NEUTRAL;")
            .expect_err("FROM must not be empty");
        parse_create_placement("CREATE PLACEMENT p FROM source TO NEUTRAL;")
            .expect_err("TO must not be empty");
    }

    #[test]
    fn parses_ordered_alter_operations() {
        let alter = parse_alter_placement(
            "ALTER PLACEMENT p SET POLICY PREFER COLOCATION, SET RANK 2, DROP RANK, SET FROM a, b \
             TO c, d, RENAME TO renamed;",
        )
        .expect("ALTER PLACEMENT must parse");

        assert_eq!(alter.operations.len(), 5);
        assert!(matches!(
            alter.operations[0],
            AlterPlacementOperation::SetPolicy {
                policy: PlacementPolicy::PreferColocation
            }
        ));
        assert!(matches!(
            alter.operations[1],
            AlterPlacementOperation::SetRank { rank: 2 }
        ));
        assert!(matches!(
            alter.operations[2],
            AlterPlacementOperation::DropRank
        ));
        assert!(matches!(
            alter.operations[3],
            AlterPlacementOperation::SetMembers { .. }
        ));
        assert!(matches!(
            alter.operations[4],
            AlterPlacementOperation::RenameTo { .. }
        ));
    }

    #[test]
    fn parses_describe_and_show_lifecycle_statements() {
        assert!(matches!(
            parse_statement("DESCRIBE PLACEMENT critical;"),
            Ok(Statement::DescribePlacement(_))
        ));
        assert!(matches!(
            parse_statement("SHOW PLACEMENTS;"),
            Ok(Statement::ShowPlacements(_))
        ));
    }

    #[test]
    fn completes_policy_phrases_without_branch_leakage() {
        let input = "CREATE PLACEMENT critical FROM source TO sink ";
        let suggestions = suggest_create_placement(input, input.len());
        for expected in [
            "REQUIRE COLOCATION",
            "PREFER COLOCATION",
            "NEUTRAL",
            "SUGGEST SEPARATION",
        ] {
            assert!(suggestions.contains(&expected.to_string()));
        }
        assert!(!suggestions.contains(&"JUNCTION".to_string()));
        assert!(!suggestions.contains(&"REQUIRE SEPARATION".to_string()));
    }

    #[test]
    fn top_level_completion_exposes_placement_without_cross_family_leakage() {
        let suggestions = suggest_statement("CREATE PLA", 10);
        assert!(suggestions.contains(&"PLACEMENT".to_string()));
        assert!(!suggestions.contains(&"PLACEMENTS".to_string()));

        let input = "ALTER PLACEMENT critical ";
        let suggestions = suggest_alter_placement(input, input.len());
        assert!(suggestions.contains(&"SET".to_string()));
        assert!(suggestions.contains(&"DROP".to_string()));
        assert!(suggestions.contains(&"RENAME".to_string()));
        assert!(!suggestions.contains(&"ADD".to_string()));
    }
}
