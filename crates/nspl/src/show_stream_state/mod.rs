use chumsky::prelude::*;
use nervix_models::ShowRelayMaterializedState;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, into_parse_error, kw, lex_input, relay_ref, suggest_from,
        tok,
    },
};

pub fn show_stream_materialized_state_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowRelayMaterializedState, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Relay))
        .ignore_then(relay_ref())
        .then_ignore(kw(Identifier::Materialized))
        .then_ignore(kw(Identifier::State))
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|relay| ShowRelayMaterializedState { relay })
}

pub fn parse_show_stream_materialized_state_tokens(
    tokens: &[Token],
) -> Result<ShowRelayMaterializedState, Vec<ParseError<'_>>> {
    let out = show_stream_materialized_state_parser()
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

pub fn parse_show_stream_materialized_state(
    input: &str,
) -> Result<ShowRelayMaterializedState, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_show_stream_materialized_state_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_show_stream_materialized_state(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, show_stream_materialized_state_parser())
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
    fn parses_show_stream_materialized_state() {
        let tokens = to_tokens("SHOW RELAY notifications MATERIALIZED STATE;");
        let parsed =
            parse_show_stream_materialized_state_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.relay.as_str(), "notifications");
    }

    #[test]
    fn suggests_relay_reference_after_show_stream() {
        let input = "SHOW RELAY ";
        let suggestions = suggest_show_stream_materialized_state(input, input.len());
        assert!(suggestions.contains(&"ref:relay".to_string()));
    }

    #[test]
    fn suggests_materialized_after_relay_reference() {
        let input = "SHOW RELAY notifications ";
        let suggestions = suggest_show_stream_materialized_state(input, input.len());
        assert!(suggestions.contains(&"MATERIALIZED".to_string()));
    }
}
