use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    AlterBranch, AlterBranchOperation, BranchEviction, BranchPartitions, CreateBranch,
    CreateStatement,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, alter_op_separator, branch_definition_header,
        branch_name, branch_ref, if_not_exists_clause, into_parse_error, kw, lex_input,
        nonzero_u16_value, nonzero_u64_value, suggest_from, tok,
    },
};

fn max_instances<'src>()
-> impl Parser<'src, &'src [Token], BranchEviction, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Max)
        .ignore_then(kw(Identifier::Instances))
        .ignore_then(nonzero_u64_value(
            "max_instances",
            "MAX INSTANCES must be greater than 0",
        ))
        .then_ignore(kw(Identifier::Evict))
        .then_ignore(kw(Identifier::Lru))
        .map(|max_instances| BranchEviction::Lru { max_instances })
}

/// Direct counts and discovered or grouped external partitions, shared by CREATE and ALTER.
fn branch_partitions<'src>()
-> impl Parser<'src, &'src [Token], BranchPartitions, extra::Err<ParseError<'src>>> + Clone {
    let count = nonzero_u16_value("partition_count", "PARTITIONS must be greater than 0");
    kw(Identifier::Partitions).ignore_then(choice((
        kw(Identifier::Inherited).to(BranchPartitions::Inherited),
        count
            .then(kw(Identifier::Inherited).or_not())
            .map(|(count, inherited)| {
                if inherited.is_some() {
                    BranchPartitions::InheritedGrouped(count)
                } else {
                    BranchPartitions::Direct(count)
                }
            }),
    )))
}

pub fn create_branch_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateBranch>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Branch))
        .then(branch_name())
        .then(branch_definition_header())
        .then(branch_partitions().or_not())
        .then(max_instances().or_not())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |((((if_not_exists, name), (schema, ttl)), partitions), eviction)| {
                CreateStatement::new(
                    CreateBranch {
                        name,
                        schema,
                        ttl,
                        partitions: partitions.unwrap_or(BranchPartitions::ONE),
                        eviction,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn alter_branch_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterBranch, extra::Err<ParseError<'src>>> + Clone {
    let operation = kw(Identifier::Set)
        .ignore_then(branch_partitions())
        .map(|partitions| AlterBranchOperation::SetPartitions { partitions })
        .boxed();

    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Branch))
        .ignore_then(branch_ref())
        .then(
            operation
                .separated_by(alter_op_separator())
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(branch, operations)| AlterBranch { branch, operations })
}

pub fn parse_create_branch_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateBranch>, Vec<ParseError<'_>>> {
    let out = create_branch_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_create_branch(
    input: &str,
) -> error_stack::Result<CreateStatement<CreateBranch>, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_create_branch_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_alter_branch_tokens(tokens: &[Token]) -> Result<AlterBranch, Vec<ParseError<'_>>> {
    let out = alter_branch_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_alter_branch(input: &str) -> error_stack::Result<AlterBranch, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_alter_branch_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_branch(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_branch_parser())
}

pub fn suggest_alter_branch(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, alter_branch_parser())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use nonzero_ext::nonzero;

    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|token| token.token)
            .collect()
    }

    #[test]
    fn parses_create_branch_with_lru_eviction() {
        let input =
            "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m MAX INSTANCES 1000 EVICT LRU;";

        let parsed = parse_create_branch_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "by_tenant");
        assert_eq!(parsed.schema.as_str(), "tenant_branch");
        assert_eq!(parsed.ttl, "5m");
        assert_eq!(parsed.partitions, BranchPartitions::ONE);
        assert_eq!(
            parsed.eviction,
            Some(BranchEviction::Lru {
                max_instances: nonzero!(1000u64)
            })
        );
    }

    #[test]
    fn parses_create_branch_without_eviction() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;";

        let parsed = parse_create_branch_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(parsed.eviction, None);
        assert_eq!(parsed.partitions, BranchPartitions::ONE);
    }

    #[test]
    fn parses_inherited_partition_declarations_and_their_canonical_forms() {
        for (body, expected, canonical) in [
            ("inherited", BranchPartitions::Inherited, "INHERITED"),
            (
                "4 inherited",
                BranchPartitions::InheritedGrouped(nonzero!(4u16)),
                "4 INHERITED",
            ),
            (
                "65535 INHERITED",
                BranchPartitions::InheritedGrouped(NonZeroU16::MAX),
                "65535 INHERITED",
            ),
        ] {
            let text =
                format!("CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS {body};");
            let parsed = parse_create_branch(&text).expect("the inherited declaration is valid");
            assert_eq!(parsed.partitions, expected);
            assert_eq!(
                parsed
                    .to_canonical_plexus()
                    .expect("the declaration renders"),
                format!(
                    "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS {canonical};"
                )
            );
        }
    }

    #[test]
    fn rejects_malformed_inherited_partition_declarations() {
        for body in [
            "0 INHERITED",
            "65536 INHERITED",
            "INHERITED 4",
            "4 INHERITED INHERITED",
        ] {
            let text =
                format!("CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS {body};");
            assert!(parse_create_branch(&text).is_err(), "{text}");
        }
    }

    #[test]
    fn parses_inherited_partition_alterations() {
        for (body, expected) in [
            ("inherited", BranchPartitions::Inherited),
            (
                "4 inherited",
                BranchPartitions::InheritedGrouped(nonzero!(4u16)),
            ),
        ] {
            let parsed =
                parse_alter_branch(&format!("ALTER BRANCH by_tenant SET PARTITIONS {body};"))
                    .expect("ALTER shares the complete partition declaration grammar");
            assert_eq!(parsed.requested_partitions(), Some(expected));
        }
    }

    #[test]
    fn parses_create_branch_partitions_before_eviction() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 4 MAX \
                     INSTANCES 10 EVICT LRU;";

        let parsed = parse_create_branch_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(parsed.partitions, BranchPartitions::Direct(nonzero!(4u16)));
        assert_eq!(
            parsed.eviction,
            Some(BranchEviction::Lru {
                max_instances: nonzero!(10u64)
            })
        );
    }

    #[test]
    fn parses_an_explicit_single_partition_as_the_default() {
        let input = "create branch by_tenant schema tenant_branch ttl 5m partitions 1;";

        let parsed = parse_create_branch_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(parsed.partitions, BranchPartitions::ONE);
    }

    #[test]
    fn parses_the_largest_partition_count() {
        let input = format!(
            "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS {};",
            u16::MAX
        );

        let parsed = parse_create_branch_tokens(&to_tokens(&input)).expect("parse should succeed");

        assert_eq!(parsed.partitions, BranchPartitions::Direct(NonZeroU16::MAX));
    }

    #[test]
    fn rejects_zero_partitions() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 0;";

        let error = parse_create_branch(input).expect_err("a branch has at least one partition");

        assert!(
            format!("{error:#}").contains("PARTITIONS must be greater than 0"),
            "the refusal names the bound: {error:#}"
        );
    }

    #[test]
    fn rejects_a_partition_count_beyond_the_u16_range() {
        let input = format!(
            "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS {};",
            u32::from(u16::MAX) + 1
        );

        let error = parse_create_branch(&input)
            .expect_err("a partition count is bounded by its 16-bit Model");

        assert!(
            format!("{error:#}").contains("expected 0 through 65535"),
            "the refusal names the range: {error:#}"
        );
    }

    #[test]
    fn rejects_partitions_after_eviction() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m MAX INSTANCES 10 EVICT \
                     LRU PARTITIONS 4;";

        parse_create_branch_tokens(&to_tokens(input))
            .expect_err("the partition count precedes the eviction clause");
    }

    #[test]
    fn rejects_partitions_without_a_count() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS;";

        parse_create_branch_tokens(&to_tokens(input)).expect_err("PARTITIONS needs its count");
    }

    #[test]
    fn rejects_values_block() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch VALUES { tenant = \
                     notifications.tenant } TTL 5m;";

        parse_create_branch_tokens(&to_tokens(input)).expect_err("VALUES belongs to initiators");
    }

    #[test]
    fn rejects_lru_without_max_instances() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m EVICT LRU;";

        parse_create_branch_tokens(&to_tokens(input)).expect_err("MAX INSTANCES is required");
    }

    #[test]
    fn suggests_schema_after_branch_name() {
        let input = "CREATE BRANCH by_tenant ";
        let suggestions = suggest_create_branch(input, input.len());
        assert!(suggestions.contains(&"SCHEMA".to_string()));
    }

    #[test]
    fn suggests_evict_after_max_instances() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m MAX INSTANCES 1000 ";
        let suggestions = suggest_create_branch(input, input.len());
        assert!(suggestions.contains(&"EVICT".to_string()));
    }

    #[test]
    fn suggests_the_partition_count_after_partitions() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS ";
        let suggestions = suggest_create_branch(input, input.len());
        assert!(suggestions.contains(&"partition_count".to_string()));
        assert!(suggestions.contains(&"INHERITED".to_string()));
    }

    #[test]
    fn suggests_inherited_after_a_count_and_keeps_the_completed_clause_closed() {
        for prefix in [
            "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 4 I",
            "ALTER BRANCH by_tenant SET PARTITIONS 4 I",
        ] {
            let suggestions = if prefix.starts_with("CREATE") {
                suggest_create_branch(prefix, prefix.len())
            } else {
                suggest_alter_branch(prefix, prefix.len())
            };
            assert!(
                suggestions.contains(&"INHERITED".to_string()),
                "{suggestions:?}"
            );
        }
        let complete = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS INHERITED ";
        let suggestions = suggest_create_branch(complete, complete.len());
        assert!(!suggestions.contains(&"partition_count".to_string()));
        assert!(!suggestions.contains(&"INHERITED".to_string()));
    }

    #[test]
    fn parses_alter_branch_set_partitions() {
        let input = "ALTER BRANCH by_tenant SET PARTITIONS 8;";

        let parsed = parse_alter_branch_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(parsed.branch.as_str(), "by_tenant");
        assert_eq!(
            parsed.operations,
            vec![AlterBranchOperation::SetPartitions {
                partitions: BranchPartitions::Direct(nonzero!(8u16)),
            }]
        );
    }

    #[test]
    fn parses_alter_branch_back_to_one_partition() {
        let input = "alter branch by_tenant set partitions 1";

        let parsed = parse_alter_branch_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(
            parsed.operations,
            vec![AlterBranchOperation::SetPartitions {
                partitions: BranchPartitions::ONE,
            }]
        );
    }

    #[test]
    fn rejects_alter_branch_without_an_operation() {
        parse_alter_branch_tokens(&to_tokens("ALTER BRANCH by_tenant;"))
            .expect_err("an alteration names at least one operation");
    }

    #[test]
    fn rejects_alter_branch_set_zero_partitions() {
        let error = parse_alter_branch("ALTER BRANCH by_tenant SET PARTITIONS 0;")
            .expect_err("a branch has at least one partition");

        assert!(
            format!("{error:#}").contains("PARTITIONS must be greater than 0"),
            "the refusal names the bound: {error:#}"
        );
    }

    #[test]
    fn rejects_alter_branch_operations_it_does_not_offer() {
        parse_alter_branch_tokens(&to_tokens("ALTER BRANCH by_tenant SET TTL 5m;"))
            .expect_err("ALTER BRANCH changes only the partition count");
    }

    #[test]
    fn suggests_set_after_the_altered_branch() {
        let input = "ALTER BRANCH by_tenant ";
        let suggestions = suggest_alter_branch(input, input.len());
        assert_eq!(suggestions, vec!["SET".to_string()]);
    }

    #[test]
    fn suggests_partitions_after_set() {
        let input = "ALTER BRANCH by_tenant SET ";
        let suggestions = suggest_alter_branch(input, input.len());
        assert_eq!(suggestions, vec!["PARTITIONS".to_string()]);
    }

    #[test]
    fn suggests_the_partition_count_after_set_partitions() {
        let input = "ALTER BRANCH by_tenant SET PARTITIONS ";
        let suggestions = suggest_alter_branch(input, input.len());
        assert!(suggestions.contains(&"partition_count".to_string()));
        assert!(suggestions.contains(&"INHERITED".to_string()));
    }
}
