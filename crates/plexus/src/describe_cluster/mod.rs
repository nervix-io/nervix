use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::DescribeCluster;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, into_parse_error, kw, lex_input,
        suggest_from, tok,
    },
};

pub fn describe_cluster_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeCluster, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Cluster))
        .to(DescribeCluster)
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_describe_cluster_tokens(
    tokens: &[Token],
) -> Result<DescribeCluster, Vec<ParseError<'_>>> {
    let out = describe_cluster_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_describe_cluster(
    input: &str,
) -> error_stack::Result<DescribeCluster, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_describe_cluster_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_cluster(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_cluster_parser())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_describe_cluster_in_any_case() {
        for input in ["DESCRIBE CLUSTER;", "describe cluster;", "DESCRIBE CLUSTER"] {
            let tokens = to_tokens(input);
            assert_eq!(
                parse_describe_cluster_tokens(&tokens),
                Ok(DescribeCluster),
                "{input}"
            );
        }
    }

    #[test]
    fn rejects_a_named_or_qualified_cluster() {
        for input in [
            "DESCRIBE CLUSTER prod;",
            "DESCRIBE CLUSTER STATUS;",
            "DESCRIBE;",
        ] {
            let tokens = to_tokens(input);
            assert!(parse_describe_cluster_tokens(&tokens).is_err(), "{input}");
        }
        assert!(parse_describe_cluster("DESCRIBE CLUSTER node-1;").is_err());
    }

    #[test]
    fn suggests_cluster_after_describe() {
        let input = "DESCRIBE ";
        let suggestions = suggest_describe_cluster(input, input.len());
        assert!(suggestions.contains(&"CLUSTER".to_string()));
        assert!(!suggestions.contains(&"STATUS".to_string()));
    }

    #[test]
    fn suggests_nothing_but_the_end_after_describe_cluster() {
        let input = "DESCRIBE CLUSTER ";
        let suggestions = suggest_describe_cluster(input, input.len());
        assert!(!suggestions.contains(&"STATUS".to_string()));
        assert!(!suggestions.contains(&"CLUSTER".to_string()));
    }
}
