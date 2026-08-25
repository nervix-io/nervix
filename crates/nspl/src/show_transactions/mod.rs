use chumsky::prelude::*;
use nervix_models::ShowTransactions;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, into_parse_error, kw, lex_input, suggest_from, tok,
    },
};

pub fn show_transactions_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowTransactions, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Transactions))
        .then_ignore(tok(Token::Semicolon).or_not())
        .to(ShowTransactions)
}

pub fn parse_show_transactions_tokens(
    tokens: &[Token],
) -> Result<ShowTransactions, Vec<ParseError<'_>>> {
    let out = show_transactions_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_show_transactions(input: &str) -> Result<ShowTransactions, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_show_transactions_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_show_transactions(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, show_transactions_parser())
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
    fn parses_show_transactions() {
        parse_show_transactions_tokens(&to_tokens("SHOW TRANSACTIONS;"))
            .expect("parse should succeed");
    }

    #[test]
    fn rejects_incomplete_show_transactions() {
        assert!(parse_show_transactions_tokens(&to_tokens("SHOW")).is_err());
    }

    #[test]
    fn suggests_transactions_after_show() {
        let suggestions = suggest_show_transactions("SHOW ", "SHOW ".len());
        assert!(suggestions.contains(&"TRANSACTIONS".to_string()));
        assert!(!suggestions.contains(&"CREATE".to_string()));
    }
}
