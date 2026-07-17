use std::ops::Range;

use arrow_schema::{DataType, TimeUnit};
use chumsky::{
    input::{Stream, ValueInput},
    prelude::*,
};

use crate::vm_program::{
    ast::{
        BinaryOp, Expr, FieldRef, FunctionName, Invocation, Literal, Program, Span, merge_spans,
        spanned,
    },
    lexer::{SpannedToken, Token, lex},
};

pub type ParseError<'src> = Rich<'src, Token, Span>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub message: String,
    pub span: Range<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseFromSourceError {
    Lex {
        source: String,
        diagnostics: Vec<Diagnostic>,
    },
    Parse {
        source: String,
        diagnostics: Vec<Diagnostic>,
    },
}

fn parse_data_type(name: &str) -> Option<DataType> {
    match name.to_ascii_uppercase().as_str() {
        "UINT8" | "U8" => Some(DataType::UInt8),
        "INT8" | "I8" => Some(DataType::Int8),
        "UINT16" | "U16" => Some(DataType::UInt16),
        "INT16" | "I16" => Some(DataType::Int16),
        "UINT32" | "U32" => Some(DataType::UInt32),
        "INT32" | "I32" => Some(DataType::Int32),
        "UINT64" | "U64" => Some(DataType::UInt64),
        "INT64" | "I64" => Some(DataType::Int64),
        "FLOAT32" | "F32" => Some(DataType::Float32),
        "FLOAT64" | "F64" => Some(DataType::Float64),
        "BOOLEAN" | "BOOL" => Some(DataType::Boolean),
        "UTF8" | "STRING" => Some(DataType::Utf8),
        "DATETIME" => Some(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("+00:00".into()),
        )),
        _ => None,
    }
}

fn keyword<'src, I>(token: Token) -> impl Parser<'src, I, (), extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    just(token).ignored()
}

fn identifier_name<'src, I>() -> impl Parser<'src, I, String, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    select! {
        Token::Identifier(name) => name,
    }
    .labelled("identifier")
}

pub fn field_ref_parser<'src, I>()
-> impl Parser<'src, I, chumsky::span::Spanned<FieldRef, Span>, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    identifier_name()
        .then_ignore(keyword(Token::Dot))
        .then(identifier_name())
        .map_with(|(relay, field), e| spanned(FieldRef { relay, field }, e.span()))
}

fn type_name<'src, I>()
-> impl Parser<'src, I, chumsky::span::Spanned<DataType, Span>, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    select! {
        Token::Identifier(name) = e => (name, e.span()),
    }
    .try_map(|(name, span), _| {
        parse_data_type(&name)
            .map(|data_type| spanned(data_type, span))
            .ok_or_else(|| Rich::custom(span, format!("unsupported type '{name}'")))
    })
    .labelled("type_name")
}

pub fn expr_parser<'src, I>()
-> impl Parser<'src, I, chumsky::span::Spanned<Expr, Span>, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    recursive(|expr| {
        let literal = choice((
            select! {
                Token::Integer(value) = e => spanned(Expr::Literal(Literal::Int64(value)), e.span()),
            },
            select! {
                Token::Float(value) = e => spanned(Expr::Literal(Literal::Float64(value)), e.span()),
            },
            select! {
                Token::True = e => spanned(Expr::Literal(Literal::Bool(true)), e.span()),
            },
            select! {
                Token::False = e => spanned(Expr::Literal(Literal::Bool(false)), e.span()),
            },
            select! {
                Token::Null = e => spanned(Expr::Literal(Literal::Null), e.span()),
            },
            select! {
                Token::String(value) = e => spanned(Expr::Literal(Literal::String(value)), e.span()),
            },
        ));

        let function_call = identifier_name()
            .then(
                expr.clone()
                    .separated_by(keyword(Token::Comma))
                    .allow_trailing()
                    .collect::<Vec<_>>()
                    .delimited_by(keyword(Token::LParen), keyword(Token::RParen)),
            )
            .map_with(|(name, args), e| {
                spanned(
                    Expr::Call {
                        function: FunctionName::parse(&name),
                        args,
                    },
                    e.span(),
                )
            });

        let atom = choice((
            literal,
            function_call,
            field_ref_parser()
                .map(|field_ref| spanned(Expr::FieldRef(field_ref.inner), field_ref.span)),
            expr.clone()
                .delimited_by(keyword(Token::LParen), keyword(Token::RParen)),
        ));

        let cast = atom
            .then(
                keyword(Token::As)
                    .ignore_then(type_name())
                    .repeated()
                    .collect::<Vec<_>>(),
            )
            .map(|(value, casts)| {
                casts.into_iter().fold(value, |value, data_type| {
                    let span = merge_spans(&value.span, &data_type.span);
                    spanned(
                        Expr::Cast {
                            expr: Box::new(value),
                            data_type: data_type.inner,
                        },
                        span,
                    )
                })
            });

        let unary = choice((
            select! {
                Token::Minus = e => (crate::vm_program::UnaryOp::Neg, e.span()),
            },
            select! {
                Token::Not = e => (crate::vm_program::UnaryOp::Not, e.span()),
            },
        ))
        .repeated()
        .collect::<Vec<_>>()
        .then(cast)
        .map(|(ops, value)| {
            ops.into_iter().rev().fold(value, |value, (op, op_span)| {
                let span = merge_spans(&op_span, &value.span);
                spanned(
                    Expr::Unary {
                        op,
                        expr: Box::new(value),
                    },
                    span,
                )
            })
        });

        let multiplicative = unary.clone().foldl(
            choice((
                keyword(Token::Star).to(BinaryOp::Mul),
                keyword(Token::Slash).to(BinaryOp::Div),
                keyword(Token::Percent).to(BinaryOp::Rem),
            ))
            .then(unary.clone())
            .repeated(),
            |left, (op, right)| {
                let span = merge_spans(&left.span, &right.span);
                spanned(
                    Expr::Binary {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                    span,
                )
            },
        );

        let additive = multiplicative.clone().foldl(
            choice((
                keyword(Token::Plus).to(BinaryOp::Add),
                keyword(Token::Minus).to(BinaryOp::Sub),
            ))
            .then(multiplicative.clone())
            .repeated(),
            |left, (op, right)| {
                let span = merge_spans(&left.span, &right.span);
                spanned(
                    Expr::Binary {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                    span,
                )
            },
        );

        let comparison = additive.clone().foldl(
            choice((
                keyword(Token::Eq).to(BinaryOp::Eq),
                keyword(Token::NotEq).to(BinaryOp::NotEq),
                keyword(Token::GtEq).to(BinaryOp::GtEq),
                keyword(Token::LtEq).to(BinaryOp::LtEq),
                keyword(Token::Gt).to(BinaryOp::Gt),
                keyword(Token::Lt).to(BinaryOp::Lt),
            ))
            .then(additive.clone())
            .repeated(),
            |left, (op, right)| {
                let span = merge_spans(&left.span, &right.span);
                spanned(
                    Expr::Binary {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                    span,
                )
            },
        );

        let logical_and = comparison.clone().foldl(
            keyword(Token::And)
                .to(BinaryOp::And)
                .then(comparison.clone())
                .repeated(),
            |left, (op, right)| {
                let span = merge_spans(&left.span, &right.span);
                spanned(
                    Expr::Binary {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                    span,
                )
            },
        );

        logical_and.clone().foldl(
            keyword(Token::Or)
                .to(BinaryOp::Or)
                .then(logical_and)
                .repeated(),
            |left, (op, right)| {
                let span = merge_spans(&left.span, &right.span);
                spanned(
                    Expr::Binary {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                    span,
                )
            },
        )
    })
}

pub fn parser<'src, I>()
-> impl Parser<'src, I, chumsky::span::Spanned<Program, Span>, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    let expr = expr_parser();

    let set_assignment = field_ref_parser()
        .map(|field_ref| field_ref.inner)
        .then_ignore(keyword(Token::Eq))
        .then(expr.clone());
    let set_block = keyword(Token::Set).ignore_then(
        set_assignment
            .separated_by(keyword(Token::Comma))
            .at_least(1)
            .allow_trailing()
            .collect::<Vec<_>>(),
    );
    let unset_block = keyword(Token::Unset).ignore_then(
        field_ref_parser()
            .map(|field_ref| field_ref.inner)
            .separated_by(keyword(Token::Comma))
            .at_least(1)
            .allow_trailing()
            .collect::<Vec<_>>(),
    );
    let invocation = identifier_name()
        .then(
            expr.clone()
                .separated_by(keyword(Token::Comma))
                .allow_trailing()
                .collect::<Vec<_>>()
                .delimited_by(keyword(Token::LParen), keyword(Token::RParen)),
        )
        .map_with(|(name, args), e| {
            spanned(
                Invocation {
                    function: FunctionName::parse(&name),
                    args,
                },
                e.span(),
            )
        });
    let invoke_block = keyword(Token::Invoke).ignore_then(
        invocation
            .separated_by(keyword(Token::Comma))
            .at_least(1)
            .allow_trailing()
            .collect::<Vec<_>>(),
    );

    set_block
        .or_not()
        .then_ignore(keyword(Token::Semicolon).repeated())
        .then(unset_block.or_not())
        .then_ignore(keyword(Token::Semicolon).repeated())
        .then(keyword(Token::Where).ignore_then(expr).or_not())
        .then_ignore(keyword(Token::Semicolon).repeated())
        .then(invoke_block.or_not())
        .then_ignore(keyword(Token::Semicolon).repeated())
        .try_map(|(((set, unset), filter), invoke), span| {
            if set.is_none() && unset.is_none() && filter.is_none() && invoke.is_none() {
                return Err(Rich::custom(
                    span,
                    "expected SET, UNSET, WHERE, or INVOKE clause",
                ));
            }

            Ok(Program {
                filter,
                branch_filters: Vec::new(),
                set: set.unwrap_or_default(),
                unset: unset.unwrap_or_default(),
                invoke: invoke.unwrap_or_default(),
            })
        })
        .map_with(|program, e| spanned(program, e.span()))
}

pub fn parse_tokens(
    tokens: &[SpannedToken],
) -> Result<chumsky::span::Spanned<Program, Span>, Vec<ParseError<'_>>> {
    let end_span = tokens
        .last()
        .map(|token| token.span.end..token.span.end)
        .unwrap_or(0..0);
    let relay = Stream::from_iter(
        tokens
            .iter()
            .cloned()
            .map(|token| (token.token, token.span)),
    )
    .map(end_span.into(), |(token, span)| (token, span));

    let parsed = parser().then_ignore(end()).parse(relay);
    if parsed.has_errors() {
        Err(parsed.into_errors())
    } else {
        Ok(parsed
            .into_output()
            .expect("successful parse must contain a program"))
    }
}

pub fn parse_program(
    input: &str,
) -> Result<chumsky::span::Spanned<Program, Span>, ParseFromSourceError> {
    let source = input.to_string();
    let tokens = lex(input).map_err(|errors| ParseFromSourceError::Lex {
        source: source.clone(),
        diagnostics: errors
            .into_iter()
            .map(|error| Diagnostic {
                message: format!("{error:?}"),
                span: error.span().into_range(),
            })
            .collect(),
    })?;

    parse_tokens(&tokens).map_err(|errors| ParseFromSourceError::Parse {
        source,
        diagnostics: errors
            .into_iter()
            .map(|error| Diagnostic {
                message: format!("{error:?}"),
                span: error.span().into_range(),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn parses_set_unset_and_where_blocks() {
        let parsed = parse_program(
            "SET input.total = input.amount * 2, input.kind = lower(input.name) UNSET \
             input.legacy, input.old_flag WHERE input.amount > 10 AND input.active;",
        )
        .expect("program must parse");

        assert!(parsed.inner.filter.is_some());
        assert!(parsed.inner.branch_filters.is_empty());
        assert_eq!(parsed.inner.set.len(), 2);
        assert_eq!(parsed.inner.set[0].0.relay, "input");
        assert_eq!(parsed.inner.set[0].0.field, "total");
        assert_eq!(parsed.inner.set[1].0.field, "kind");
        assert_eq!(
            parsed.inner.unset,
            vec![
                FieldRef {
                    relay: "input".to_string(),
                    field: "legacy".to_string(),
                },
                FieldRef {
                    relay: "input".to_string(),
                    field: "old_flag".to_string(),
                },
            ]
        );
    }

    #[test]
    fn parses_clauses_in_canonical_order() {
        let parsed = parse_program(
            "SET input.total = input.amount UNSET input.legacy WHERE input.active = true INVOKE \
             write_header(lower(\"X-Tenant\"), input.tenant), write_header(\"x-route\", \
             input.route);",
        )
        .expect("program must parse");

        assert!(parsed.inner.filter.is_some());
        assert!(parsed.inner.branch_filters.is_empty());
        assert_eq!(parsed.inner.set.len(), 1);
        assert_eq!(parsed.inner.unset[0].field, "legacy");
        assert_eq!(parsed.inner.invoke.len(), 2);
        assert_eq!(
            parsed.inner.invoke[0].inner.function,
            FunctionName::WriteHeader
        );
        assert_eq!(parsed.inner.invoke[0].inner.args.len(), 2);
    }

    #[test]
    fn parses_invoke_only_program() {
        let parsed = parse_program("INVOKE write_header(\"route\", input.route)")
            .expect("invoke-only program must parse");

        assert!(parsed.inner.set.is_empty());
        assert!(parsed.inner.unset.is_empty());
        assert!(parsed.inner.filter.is_none());
        assert_eq!(parsed.inner.invoke.len(), 1);
    }

    #[test]
    fn rejects_noncanonical_clause_order() {
        let error = parse_program("WHERE input.active SET input.total = input.amount;")
            .expect_err("program must fail");

        assert!(matches!(error, ParseFromSourceError::Parse { .. }));

        let error = parse_program(
            "INVOKE write_header(\"route\", input.route) SET input.total = input.amount;",
        )
        .expect_err("INVOKE must be the final block");
        assert!(matches!(error, ParseFromSourceError::Parse { .. }));
    }

    #[test]
    fn rejects_double_equals_comparison() {
        let error = parse_program(concat!("WHERE input.active ", "=", "= true;"))
            .expect_err("program must fail");

        assert!(matches!(error, ParseFromSourceError::Parse { .. }));
    }

    #[test]
    fn parses_casts_and_function_calls() {
        let parsed = parse_program("SET input.value = trim(input.raw) AS STRING;")
            .expect("program must parse");
        let (_, expr) = &parsed.inner.set[0];

        match &expr.inner {
            Expr::Cast { expr, data_type } => {
                assert_eq!(*data_type, DataType::Utf8);
                assert!(matches!(
                    expr.inner,
                    Expr::Call {
                        function: FunctionName::Trim,
                        ..
                    }
                ));
            }
            other => panic!("expected cast, got {other:?}"),
        }
    }

    #[test]
    fn parses_null_literal() {
        let parsed = parse_program("SET input.optional_value = NULL;").expect("program must parse");

        assert_eq!(parsed.inner.set.len(), 1);
    }

    #[test]
    fn parses_function_names_as_enum_variants() {
        let parsed = parse_program(
            "SET input.known = lower(input.raw), input.total = sum(input.values), input.latest = \
             last(input.values), input.earliest = first(input.values), input.counted = \
             count(input.values), input.second = nth(input.values, 1), input.unknown = \
             mystery(input.raw);",
        )
        .expect("program must parse");

        let (_, known) = &parsed.inner.set[0];
        let (_, unknown) = &parsed.inner.set[6];

        assert!(matches!(
            known.inner,
            Expr::Call {
                function: FunctionName::Lower,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[1].1.inner,
            Expr::Call {
                function: FunctionName::Sum,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[2].1.inner,
            Expr::Call {
                function: FunctionName::Last,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[3].1.inner,
            Expr::Call {
                function: FunctionName::First,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[4].1.inner,
            Expr::Call {
                function: FunctionName::Count,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[5].1.inner,
            Expr::Call {
                function: FunctionName::Nth,
                ..
            }
        ));
        assert!(matches!(
            unknown.inner,
            Expr::Call {
                function: FunctionName::Unknown(ref name),
                ..
            } if name == "mystery"
        ));
    }

    #[test]
    fn parses_extended_builtin_aliases_and_contextual_functions() {
        let parsed = parse_program(
            "SET input.ceil_alias = ceiling(input.amount), input.pow_alias = power(2.0, 3.0), \
             input.substr_alias = substring(input.raw, 1, 2), input.current = now(), input.id4 = \
             uuid_v4(), input.id7 = uuid_v7(), input.matches = regexp_like(input.raw, 'a+'), \
             input.replaced = regexp_replace(input.raw, 'a+', 'b'), input.piece = \
             regexp_substr(input.raw, 'a+');",
        )
        .expect("program must parse");

        assert!(matches!(
            parsed.inner.set[0].1.inner,
            Expr::Call {
                function: FunctionName::Ceil,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[1].1.inner,
            Expr::Call {
                function: FunctionName::Pow,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[2].1.inner,
            Expr::Call {
                function: FunctionName::Substr,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[3].1.inner,
            Expr::Call {
                function: FunctionName::Now,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[4].1.inner,
            Expr::Call {
                function: FunctionName::UuidV4,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[5].1.inner,
            Expr::Call {
                function: FunctionName::UuidV7,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[6].1.inner,
            Expr::Call {
                function: FunctionName::RegexpLike,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[7].1.inner,
            Expr::Call {
                function: FunctionName::RegexpReplace,
                ..
            }
        ));
        assert!(matches!(
            parsed.inner.set[8].1.inner,
            Expr::Call {
                function: FunctionName::RegexpSubstr,
                ..
            }
        ));
    }

    #[test]
    fn parses_where_only_program() {
        let parsed = parse_program("WHERE input.active").expect("program must parse");

        assert!(parsed.inner.filter.is_some());
        assert!(parsed.inner.branch_filters.is_empty());
        assert!(parsed.inner.set.is_empty());
        assert!(parsed.inner.unset.is_empty());
    }

    #[test]
    fn rejects_empty_program() {
        let error = parse_program("").expect_err("program must fail");

        match error {
            ParseFromSourceError::Lex { diagnostics, .. }
            | ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
        }
    }

    #[test]
    fn rejects_missing_unset_identifiers() {
        let error =
            parse_program("SET input.total = input.amount UNSET").expect_err("program must fail");

        match error {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unclosed_expression() {
        let error =
            parse_program("SET input.total = (input.amount + 1").expect_err("program must fail");

        match error {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unqualified_field_references() {
        let error = parse_program("SET input.total = amount;").expect_err("program must fail");

        match error {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }
}
