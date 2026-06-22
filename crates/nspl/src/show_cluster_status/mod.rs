use chumsky::prelude::*;
use nervix_models::ShowClusterStatus;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, into_parse_error, kw, lex_input,
        suggestions_from_errors, tok,
    },
};

pub fn show_cluster_status_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowClusterStatus, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Cluster))
        .ignore_then(kw(Identifier::Status))
        .then_ignore(tok(Token::Semicolon).or_not())
        .to(ShowClusterStatus)
}

pub fn parse_show_cluster_status_tokens(
    tokens: &[Token],
) -> Result<ShowClusterStatus, Vec<ParseError<'_>>> {
    let out = show_cluster_status_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_show_cluster_status(input: &str) -> Result<ShowClusterStatus, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_show_cluster_status_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_show_cluster_status(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = show_cluster_status_parser()
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
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_show_cluster_status() {
        let tokens = to_tokens("SHOW CLUSTER STATUS;");
        parse_show_cluster_status_tokens(&tokens).expect("parse should succeed");
    }

    #[test]
    fn suggests_cluster_after_show() {
        let suggestions = suggest_show_cluster_status("SHOW ", "SHOW ".len());
        assert!(suggestions.contains(&"CLUSTER".to_string()));
        assert!(!suggestions.contains(&"CREATE".to_string()));
    }

    #[test]
    fn suggests_status_after_show_cluster() {
        let suggestions = suggest_show_cluster_status("SHOW CLUSTER ", "SHOW CLUSTER ".len());
        assert!(suggestions.contains(&"STATUS".to_string()));
    }
}
