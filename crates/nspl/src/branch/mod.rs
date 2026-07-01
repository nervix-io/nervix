use chumsky::prelude::*;
use nervix_models::{BranchEviction, CreateBranch, CreateStatement};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, branch_definition_header, branch_name,
        current_word_prefix, if_not_exists_clause, into_parse_error, kw, lex_input,
        suggestions_from_errors, tok, u64_value,
    },
};

fn max_instances<'src>()
-> impl Parser<'src, &'src [Token], BranchEviction, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Max)
        .ignore_then(kw(Identifier::Instances))
        .ignore_then(u64_value().try_map(|value, span| {
            if value == 0 {
                Err(Rich::custom(span, "MAX INSTANCES must be greater than 0"))
            } else {
                Ok(value)
            }
        }))
        .then_ignore(kw(Identifier::Evict))
        .then_ignore(kw(Identifier::Lru))
        .map(|max_instances| BranchEviction::Lru { max_instances })
}

pub fn create_branch_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateBranch>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Branch))
        .then(branch_name())
        .then(branch_definition_header())
        .then(max_instances().or_not())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(((if_not_exists, name), (schema, ttl)), eviction)| {
            CreateStatement::new(
                CreateBranch {
                    name,
                    schema,
                    ttl,
                    eviction,
                },
                if_not_exists,
            )
        })
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
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_branch(
    input: &str,
) -> Result<CreateStatement<CreateBranch>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_branch_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_branch(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_branch_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
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
        assert_eq!(
            parsed.eviction,
            Some(BranchEviction::Lru {
                max_instances: 1000
            })
        );
    }

    #[test]
    fn parses_create_branch_without_eviction() {
        let input = "CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;";

        let parsed = parse_create_branch_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(parsed.eviction, None);
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
}
