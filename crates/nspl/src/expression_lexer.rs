//! Layer: language.
//!
//! - **Owns.** The private tokens and lexical diagnostics used by semantic expression parsing.
//! - **Depends on.** Chumsky's parser primitives and the statement lexer's reading of a string
//!   literal, which both lexers share.
//! - **Must not know.** VM programs, registry state, or runtime execution.

use chumsky::prelude::*;

use crate::lexer::string_literal;

pub type Span = SimpleSpan<usize>;
pub type LexError<'src> = Rich<'src, char, Span>;

#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Where,
    Set,
    Inherit,
    All,
    Except,
    Leak,
    Sensitive,
    Invoke,
    As,
    TryCast,
    JsonValue,
    TryJsonValue,
    JsonExists,
    And,
    Or,
    Not,
    True,
    False,
    Null,
    If,
    Case,
    When,
    Then,
    Else,
    End,
    In,
    Between,
    Is,
    Distinct,
    From,
    Udf,
    Identifier(String),
    Integer(i64),
    Float(f64),
    String(String),
    LBracket,
    RBracket,
    LParen,
    RParen,
    Comma,
    DoubleColon,
    Dot,
    Semicolon,
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
}

fn classify_identifier(raw: &str) -> Token {
    match raw.to_ascii_uppercase().as_str() {
        "WHERE" => Token::Where,
        "SET" => Token::Set,
        "INHERIT" => Token::Inherit,
        "ALL" => Token::All,
        "EXCEPT" => Token::Except,
        "LEAK" => Token::Leak,
        "SENSITIVE" => Token::Sensitive,
        "INVOKE" => Token::Invoke,
        "AS" => Token::As,
        "TRY_CAST" => Token::TryCast,
        "JSON_VALUE" => Token::JsonValue,
        "TRY_JSON_VALUE" => Token::TryJsonValue,
        "JSON_EXISTS" => Token::JsonExists,
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "TRUE" => Token::True,
        "FALSE" => Token::False,
        "NULL" => Token::Null,
        "IF" => Token::If,
        "CASE" => Token::Case,
        "WHEN" => Token::When,
        "THEN" => Token::Then,
        "ELSE" => Token::Else,
        "END" => Token::End,
        "IN" => Token::In,
        "BETWEEN" => Token::Between,
        "IS" => Token::Is,
        "DISTINCT" => Token::Distinct,
        "FROM" => Token::From,
        "UDF" => Token::Udf,
        _ => Token::Identifier(raw.to_string()),
    }
}

fn whitespace<'src>() -> impl Parser<'src, &'src str, (), extra::Err<LexError<'src>>> + Clone {
    let spaces = any()
        .filter(|c: &char| c.is_whitespace())
        .repeated()
        .at_least(1)
        .ignored();

    let line_comment = just("//")
        .then(any().filter(|c: &char| *c != '\n').repeated())
        .then(just('\n').or_not())
        .ignored();

    choice((spaces, line_comment)).repeated().ignored()
}

fn token<'src>() -> impl Parser<'src, &'src str, SpannedToken, extra::Err<LexError<'src>>> + Clone {
    let identifier = text::ascii::ident()
        .map(classify_identifier)
        .map_with(|token, e| SpannedToken {
            token,
            span: e.span(),
        });

    let number = text::int(10)
        .then(just('.').then(text::digits(10)).or_not())
        .to_slice()
        .try_map(|raw: &str, span| {
            if raw.contains('.') {
                raw.parse::<f64>()
                    .map(Token::Float)
                    .map_err(|source| Rich::custom(span, source.to_string()))
            } else {
                raw.parse::<i64>()
                    .map(Token::Integer)
                    .map_err(|source| Rich::custom(span, source.to_string()))
            }
        })
        .map_with(|token, e| SpannedToken {
            token,
            span: e.span(),
        });

    let string = string_literal()
        .map(Token::String)
        .map_with(|token, e| SpannedToken {
            token,
            span: e.span(),
        });

    let punctuation = choice((
        just("::").to(Token::DoubleColon),
        just("!=").to(Token::NotEq),
        just(">=").to(Token::GtEq),
        just("<=").to(Token::LtEq),
        just('[').to(Token::LBracket),
        just(']').to(Token::RBracket),
        just('(').to(Token::LParen),
        just(')').to(Token::RParen),
        just(',').to(Token::Comma),
        just('.').to(Token::Dot),
        just(';').to(Token::Semicolon),
        just('=').to(Token::Eq),
        just('>').to(Token::Gt),
        just('<').to(Token::Lt),
        just('+').to(Token::Plus),
        just('-').to(Token::Minus),
        just('*').to(Token::Star),
        just('/').to(Token::Slash),
        just('%').to(Token::Percent),
    ))
    .map_with(|token, e| SpannedToken {
        token,
        span: e.span(),
    });

    choice((string, number, identifier, punctuation))
}

pub fn lex(input: &str) -> Result<Vec<SpannedToken>, Vec<LexError<'_>>> {
    token()
        .padded_by(whitespace())
        .repeated()
        .collect::<Vec<_>>()
        .then_ignore(whitespace())
        .then_ignore(end())
        .parse(input)
        .into_result()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexes_conditional_keywords_case_insensitively() {
        let tokens = lex("if CASE when Then ELSE end").expect("keywords must lex");
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>(),
            vec![
                Token::If,
                Token::Case,
                Token::When,
                Token::Then,
                Token::Else,
                Token::End,
            ]
        );
    }

    #[test]
    fn lexes_the_tolerant_conversion_keyword_case_insensitively() {
        let tokens = lex("TRY_CAST try_cast Try_Cast try cast").expect("keywords must lex");
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>(),
            vec![
                Token::TryCast,
                Token::TryCast,
                Token::TryCast,
                Token::Identifier("try".to_string()),
                Token::Identifier("cast".to_string()),
            ]
        );
    }

    #[test]
    fn lexes_membership_range_and_distinctness_keywords_case_insensitively() {
        let tokens = lex("in NOT Between is Distinct fRoM input from_unix")
            .expect("comparison keywords must lex");
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>(),
            vec![
                Token::In,
                Token::Not,
                Token::Between,
                Token::Is,
                Token::Distinct,
                Token::From,
                Token::Identifier("input".to_string()),
                Token::Identifier("from_unix".to_string()),
            ]
        );
    }

    #[test]
    fn lexes_the_udf_qualifier_as_shared_language_tokens() {
        let tokens = lex("UdF::mask").expect("UDF qualifier must lex");
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>(),
            vec![
                Token::Udf,
                Token::DoubleColon,
                Token::Identifier("mask".to_string())
            ]
        );
    }

    #[test]
    fn lexes_verbatim_dollar_quoted_expression_strings() {
        let tokens = lex("$value$line one\n\"line two\"$value$")
            .expect("dollar-quoted expression string should lex");
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>(),
            vec![Token::String("line one\n\"line two\"".to_string())]
        );
    }

    #[test]
    fn lexes_every_backslash_sequence_verbatim_as_the_statement_lexer_does() {
        let cases = [
            (r"'a\\b'", r"a\\b"),
            (r#"'a\"b'"#, r#"a\"b"#),
            (r#""a\'b""#, r"a\'b"),
            (r"'a\nb'", r"a\nb"),
            (r#""a\rb""#, r"a\rb"),
            (r"'a\tb'", r"a\tb"),
            (r"'a\0b'", r"a\0b"),
            (r#""a\x41b""#, r"a\x41b"),
            (r"'a\u{e9}b'", r"a\u{e9}b"),
            (r"'a\$b'", r"a\$b"),
            (r"'a\'", r"a\"),
            (r#""a\""#, r"a\"),
            (r"$$a\nb\$$", r"a\nb\"),
        ];
        for (source, value) in cases {
            let tokens =
                lex(source).unwrap_or_else(|errors| panic!("{source} must lex: {errors:?}"));
            assert_eq!(
                tokens
                    .into_iter()
                    .map(|token| token.token)
                    .collect::<Vec<_>>(),
                vec![Token::String(value.to_string())],
                "{source}"
            );
            let statement_tokens = crate::lexer::lex(source)
                .unwrap_or_else(|errors| panic!("{source} must lex in a statement: {errors:?}"));
            assert_eq!(
                statement_tokens
                    .into_iter()
                    .map(|token| token.token)
                    .collect::<Vec<_>>(),
                vec![crate::lexer::Token::StringLiteral(value.to_string())],
                "{source} in a statement"
            );
        }
    }

    #[test]
    fn a_backslash_before_the_closing_quote_leaves_it_closing() {
        for source in [r"'a\'b'", r#""a\"b""#] {
            assert!(lex(source).is_err(), "{source} must be rejected");
            assert!(
                crate::lexer::lex(source).is_err(),
                "{source} must be rejected in a statement"
            );
        }
    }
}
