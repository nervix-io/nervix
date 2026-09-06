use chumsky::prelude::*;
use nervix_models::{ClusterNodeName, CordonNode, DrainNode, UncordonNode};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, cluster_node_name, kw, tok},
};

pub fn cordon_node_parser<'src>()
-> impl Parser<'src, &'src [Token], CordonNode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Cordon)
        .ignore_then(kw(Identifier::Node))
        .ignore_then(cluster_node_name())
        .map(|node_id| CordonNode { node_id })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn uncordon_node_parser<'src>()
-> impl Parser<'src, &'src [Token], UncordonNode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Uncordon)
        .ignore_then(kw(Identifier::Node))
        .ignore_then(cluster_node_name())
        .map(|node_id| UncordonNode { node_id })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn drain_node_parser<'src>()
-> impl Parser<'src, &'src [Token], DrainNode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Drain)
        .ignore_then(kw(Identifier::Node))
        .ignore_then(cluster_node_name())
        .map(|node_id| DrainNode { node_id })
        .then_ignore(tok(Token::Semicolon).or_not())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::{Token, lex};

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_cordon_node() {
        let tokens = to_tokens("CORDON NODE node-2;");
        let parsed = cordon_node_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");
        assert_eq!(
            parsed.node_id,
            ClusterNodeName::parse("node-2").expect("valid name")
        );
    }

    #[test]
    fn parses_uncordon_node() {
        let tokens = to_tokens("UNCORDON NODE node-2;");
        let parsed = uncordon_node_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");
        assert_eq!(
            parsed.node_id,
            ClusterNodeName::parse("node-2").expect("valid name")
        );
    }

    #[test]
    fn parses_drain_node() {
        let tokens = to_tokens("DRAIN NODE node-2;");
        let parsed = drain_node_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");
        assert_eq!(
            parsed.node_id,
            ClusterNodeName::parse("node-2").expect("valid name")
        );
    }
}
