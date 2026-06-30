use chumsky::prelude::*;
use nervix_models::{AckMode, CreateReorderer, CreateStatement};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, current_word_prefix,
        duration_lit, filter_where_clause, flush_each, from_relay_clauses, if_not_exists_clause,
        into_parse_error, kw, lex_input, message_error_policy, processor_outputs, reorderer_name,
        suggestions_from_errors, tok,
    },
};

fn boundary_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Semicolon
            | Token::Word(Word::KnownWord {
                iden: Identifier::Max,
                ..
            })
    )
}

fn token_to_source(token: &Token) -> String {
    match token {
        Token::Word(Word::KnownWord { raw, .. }) => raw.clone(),
        Token::Word(Word::UnknownWord(raw)) => raw.clone(),
        Token::StringLiteral(value) => {
            format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        Token::NumberLiteral(value) => value.clone(),
        Token::LBrace => "{".to_string(),
        Token::RBrace => "}".to_string(),
        Token::LBracket => "[".to_string(),
        Token::RBracket => "]".to_string(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::Comma => ",".to_string(),
        Token::Semicolon => ";".to_string(),
        Token::Colon => ":".to_string(),
        Token::Dot => ".".to_string(),
        Token::Hyphen => "-".to_string(),
        Token::Eq => "=".to_string(),
        Token::NotEq => "!=".to_string(),
        Token::Gt => ">".to_string(),
        Token::Lt => "<".to_string(),
        Token::GtEq => ">=".to_string(),
        Token::LtEq => "<=".to_string(),
        Token::Plus => "+".to_string(),
        Token::Star => "*".to_string(),
        Token::Slash => "/".to_string(),
        Token::Percent => "%".to_string(),
    }
}

fn render_tokens(tokens: &[Token]) -> String {
    let mut rendered = String::new();
    for (index, token) in tokens.iter().enumerate() {
        let needs_space = if index == 0 {
            false
        } else {
            let previous = &tokens[index - 1];
            let previous_blocks_space =
                matches!(previous, Token::Dot | Token::LParen | Token::LBracket);
            let token_blocks_space = matches!(
                token,
                Token::Dot | Token::Comma | Token::RParen | Token::RBracket
            );
            !previous_blocks_space && !token_blocks_space
        };
        if needs_space {
            rendered.push(' ');
        }
        rendered.push_str(&token_to_source(token));
    }
    rendered
}

fn by_exprs<'src>() -> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::By)
        .ignore_then(
            any()
                .filter(|token: &Token| !boundary_token(token))
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .map(|tokens| render_tokens(&tokens))
        .labelled("reorder_by")
}

pub fn create_reorderer_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateReorderer>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Reorderer))
        .then(reorderer_name())
        .then_ignore(kw(Identifier::From))
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .then(processor_outputs())
        .then(branch_parameterization())
        .then(by_exprs())
        .then_ignore(kw(Identifier::Max))
        .then_ignore(kw(Identifier::Time))
        .then(duration_lit())
        .then(flush_each())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (
                            (
                                (
                                    ((((if_not_exists, mode), name), from_input), filter_where),
                                    outputs,
                                ),
                                parameterized_by,
                            ),
                            order_by,
                        ),
                        max_time,
                    ),
                    flush_each,
                ),
                message_error_policy,
            )| {
                let (flush_each, max_batch_size) = flush_each;
                CreateStatement::new(
                    CreateReorderer {
                        name,
                        from: from_input,
                        output_routes: outputs,
                        parameterized_by,
                        order_by,
                        max_time,
                        flush_each,
                        max_batch_size,
                        message_error_policy,
                        mode: mode.unwrap_or(AckMode::Attached),
                        filter_where,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_reorderer_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateReorderer>, Vec<ParseError<'_>>> {
    let out = create_reorderer_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_reorderer(
    input: &str,
) -> Result<CreateStatement<CreateReorderer>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_reorderer_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_reorderer(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);
    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let out = create_reorderer_parser()
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
    fn parses_create_reorderer() {
        let tokens = to_tokens(
            "CREATE REORDERER order_notifications FROM s1 TO s2 SET s2.id = trim(s1.id) WHERE \
             s1.active BRANCHED BY tenant BY s1.tenant, concat(lower(s1.id), '-', trim(s1.kind)) \
             MAX TIME 10s FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        );
        let parsed = parse_create_reorderer_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "order_notifications");
        assert_eq!(parsed.from.from[0].as_str(), "s1");
        assert_eq!(parsed.output_routes.routes[0].relay.as_str(), "s2");
        assert_eq!(
            parsed.order_by,
            "s1.tenant, concat (lower (s1.id), '-', trim (s1.kind))"
        );
        assert_eq!(parsed.max_time, "10s");
        assert_eq!(parsed.flush_each, "100ms");
        let filter_map = parsed
            .output_routes
            .routes
            .first()
            .and_then(|output| output.filter_map.as_deref())
            .expect("filter-map should parse");
        assert!(filter_map.contains("WHERE"));
        assert!(filter_map.contains("SET"));
    }

    #[test]
    fn rejects_reorderer_global_error_policy() {
        let tokens = to_tokens(
            "CREATE REORDERER order_notifications FROM s1 TO s2 BRANCHED BY tenant BY s1.id MAX \
             TIME 10s FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR \
             LOG;",
        );
        assert!(parse_create_reorderer_tokens(&tokens).is_err());
    }

    #[test]
    fn suggests_flush_after_max_time_without_cross_branch_leakage() {
        let input = "CREATE REORDERER order_notifications FROM s1 TO s2 BRANCHED BY tenant BY \
                     s1.id MAX TIME 10s FL";
        let suggestions = suggest_create_reorderer(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
    }
}
