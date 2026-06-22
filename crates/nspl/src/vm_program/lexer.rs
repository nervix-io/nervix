use chumsky::prelude::*;

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
    Unset,
    As,
    And,
    Or,
    Not,
    True,
    False,
    Null,
    Identifier(String),
    Integer(i64),
    Float(f64),
    String(String),
    LBracket,
    RBracket,
    LParen,
    RParen,
    Comma,
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
        "UNSET" => Token::Unset,
        "AS" => Token::As,
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "TRUE" => Token::True,
        "FALSE" => Token::False,
        "NULL" => Token::Null,
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

fn string_literal<'src>() -> impl Parser<'src, &'src str, String, extra::Err<LexError<'src>>> + Clone
{
    let escape = just('\\').ignore_then(choice((
        just('\\'),
        just('\''),
        just('"'),
        just('n').to('\n'),
        just('r').to('\r'),
        just('t').to('\t'),
    )));

    let single = just('\'')
        .ignore_then(
            choice((
                escape,
                any().filter(|c: &char| *c != '\'' && *c != '\\' && *c != '\n'),
            ))
            .repeated()
            .collect::<String>(),
        )
        .then_ignore(just('\''));

    let double = just('"')
        .ignore_then(
            choice((
                escape,
                any().filter(|c: &char| *c != '"' && *c != '\\' && *c != '\n'),
            ))
            .repeated()
            .collect::<String>(),
        )
        .then_ignore(just('"'));

    choice((single, double))
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
