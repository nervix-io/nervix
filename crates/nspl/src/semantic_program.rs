use ahash_compile_time::{HashSet, HashSetExt};
use chumsky::{
    input::{Stream, ValueInput},
    prelude::*,
};
use nervix_models::{
    Assignment, AssignmentTarget, AssignmentTargetScope, BinaryOperator, CaseBranch, Expression,
    FieldReference, FieldScope, Float64Literal, Inheritance, InheritedField, Invocation, Literal,
    ParseAsType, RouteConstruction, UnaryOperator,
};

use crate::vm_program::{Diagnostic, ParseFromSourceError, SpannedToken, Token, lex};

type Span = chumsky::span::SimpleSpan<usize>;
type ParseError<'src> = Rich<'src, Token, Span>;

fn keyword<'src, I>(token: Token) -> impl Parser<'src, I, (), extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    just(token).ignored()
}

fn raw_identifier<'src, I>() -> impl Parser<'src, I, String, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    select! { Token::Identifier(name) => name }.labelled("identifier")
}

fn identifier<'src, I>()
-> impl Parser<'src, I, nervix_models::Identifier, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    raw_identifier().try_map(|name, span| {
        nervix_models::Identifier::parse(&name)
            .map_err(|error| Rich::custom(span, error.to_string()))
    })
}

fn parse_scope<'src>(name: &str, span: Span) -> Result<FieldScope, Rich<'src, Token>> {
    match name.to_ascii_lowercase().as_str() {
        "message" => Ok(FieldScope::Message),
        "input" => Ok(FieldScope::Input),
        "output" => Ok(FieldScope::Output),
        "branch" => Ok(FieldScope::Branch),
        "left" => Ok(FieldScope::Left),
        "right" => Ok(FieldScope::Right),
        "metadata" => Ok(FieldScope::Metadata),
        "partial_output" => Ok(FieldScope::PartialOutput),
        "error" => Ok(FieldScope::Error),
        _ => Err(Rich::custom(
            span,
            format!("'{name}' is not an expression scope; relay names cannot qualify fields"),
        )),
    }
}

fn field_reference<'src, I>()
-> impl Parser<'src, I, FieldReference, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    let scoped = raw_identifier()
        .then_ignore(keyword(Token::Dot))
        .then(identifier())
        .then(keyword(Token::Dot).ignore_then(identifier()).or_not())
        .try_map(|((scope, second), third), span| match third {
            Some(field) if scope.eq_ignore_ascii_case("relay_state") => Ok(FieldReference::scoped(
                FieldScope::RelayState { relay: second },
                field,
            )),
            Some(_) => Err(Rich::custom(
                span,
                "only relay_state.<relay>.<field> may contain three field path segments",
            )),
            None if scope.eq_ignore_ascii_case("relay_state") => Err(Rich::custom(
                span,
                "relay_state references require relay_state.<relay>.<field>",
            )),
            None => parse_scope(&scope, span).map(|scope| FieldReference::scoped(scope, second)),
        });
    let bare = identifier().map(FieldReference::bare);

    choice((scoped, bare)).boxed()
}

fn cast_type<'src, I>() -> impl Parser<'src, I, ParseAsType, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    raw_identifier().try_map(|name, span| {
        let ty = match name.to_ascii_uppercase().as_str() {
            "UINT8" | "U8" => ParseAsType::U8,
            "INT8" | "I8" => ParseAsType::I8,
            "UINT16" | "U16" => ParseAsType::U16,
            "INT16" | "I16" => ParseAsType::I16,
            "UINT32" | "U32" => ParseAsType::U32,
            "INT32" | "I32" => ParseAsType::I32,
            "UINT64" | "U64" => ParseAsType::U64,
            "INT64" | "I64" => ParseAsType::I64,
            "BOOLEAN" | "BOOL" => ParseAsType::Bool,
            "UTF8" | "STRING" => ParseAsType::String,
            "DATETIME" => ParseAsType::Datetime,
            "FLOAT32" | "F32" => ParseAsType::F32,
            "FLOAT64" | "F64" => ParseAsType::F64,
            _ => return Err(Rich::custom(span, format!("unsupported type '{name}'"))),
        };
        Ok(ty)
    })
}

fn expression<'src, I>() -> impl Parser<'src, I, Expression, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    recursive(|expression| {
        let literal = choice((
            select! { Token::Integer(value) => Expression::Literal(Literal::I64(value)) },
            select! { Token::Float(value) => Expression::Literal(Literal::F64(Float64Literal::new(value))) },
            keyword(Token::True).to(Expression::Literal(Literal::Bool(true))),
            keyword(Token::False).to(Expression::Literal(Literal::Bool(false))),
            keyword(Token::Null).to(Expression::Literal(Literal::Null)),
            select! { Token::String(value) => Expression::Literal(Literal::String(value)) },
        ));
        let arguments = expression
            .clone()
            .separated_by(keyword(Token::Comma))
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(keyword(Token::LParen), keyword(Token::RParen));
        let udf_call = keyword(Token::Udf)
            .then_ignore(keyword(Token::DoubleColon))
            .then(identifier())
            .then(arguments.clone())
            .map(|(((), function), arguments)| Expression::UdfCall {
                function,
                arguments,
            });
        let function_call =
            identifier()
                .then(arguments)
                .map(|(function, arguments)| Expression::Call {
                    function,
                    arguments,
                });
        let array = expression
            .clone()
            .separated_by(keyword(Token::Comma))
            .at_least(1)
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(keyword(Token::LBracket), keyword(Token::RBracket))
            .map(Expression::Array);
        let when_clause = keyword(Token::When)
            .ignore_then(expression.clone())
            .then_ignore(keyword(Token::Then))
            .then(expression.clone())
            .map(|(when, result)| CaseBranch { when, result });
        let case_expression = keyword(Token::Case)
            .ignore_then(expression.clone().or_not())
            .then(when_clause.repeated().at_least(1).collect::<Vec<_>>())
            .then(
                keyword(Token::Else)
                    .ignore_then(expression.clone())
                    .or_not(),
            )
            .then_ignore(keyword(Token::End))
            .map(|((operand, branches), else_result)| Expression::Case {
                operand: operand.map(Box::new),
                branches,
                else_result: else_result.map(Box::new),
            });
        let if_expression = keyword(Token::If)
            .ignore_then(expression.clone())
            .then_ignore(keyword(Token::Then))
            .then(expression.clone())
            .then_ignore(keyword(Token::Else))
            .then(expression.clone())
            .then_ignore(keyword(Token::End))
            .map(|((condition, then_result), else_result)| Expression::If {
                condition: Box::new(condition),
                then_result: Box::new(then_result),
                else_result: Box::new(else_result),
            });
        let atom = choice((
            literal,
            udf_call,
            function_call,
            array,
            if_expression,
            case_expression,
            field_reference().map(Expression::Field),
            expression
                .clone()
                .delimited_by(keyword(Token::LParen), keyword(Token::RParen)),
        ))
        .boxed();
        let cast = atom
            .then(
                keyword(Token::As)
                    .ignore_then(cast_type())
                    .repeated()
                    .collect::<Vec<_>>(),
            )
            .map(|(value, casts)| {
                casts
                    .into_iter()
                    .fold(value, |expression, target| Expression::Cast {
                        expression: Box::new(expression),
                        target,
                    })
            })
            .boxed();
        let unary = choice((
            keyword(Token::Minus).to(UnaryOperator::Negate),
            keyword(Token::Not).to(UnaryOperator::Not),
        ))
        .repeated()
        .collect::<Vec<_>>()
        .then(cast)
        .map(|(operators, value)| {
            operators
                .into_iter()
                .rev()
                .fold(value, |expression, operator| Expression::Unary {
                    operator,
                    expression: Box::new(expression),
                })
        })
        .boxed();
        let multiplicative = unary
            .clone()
            .foldl(
                choice((
                    keyword(Token::Star).to(BinaryOperator::Multiply),
                    keyword(Token::Slash).to(BinaryOperator::Divide),
                    keyword(Token::Percent).to(BinaryOperator::Remainder),
                ))
                .then(unary.clone())
                .repeated(),
                |left, (operator, right)| Expression::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            )
            .boxed();
        let additive = multiplicative
            .clone()
            .foldl(
                choice((
                    keyword(Token::Plus).to(BinaryOperator::Add),
                    keyword(Token::Minus).to(BinaryOperator::Subtract),
                ))
                .then(multiplicative.clone())
                .repeated(),
                |left, (operator, right)| Expression::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            )
            .boxed();
        let comparison = additive
            .clone()
            .foldl(
                choice((
                    keyword(Token::Eq).to(BinaryOperator::Equal),
                    keyword(Token::NotEq).to(BinaryOperator::NotEqual),
                    keyword(Token::GtEq).to(BinaryOperator::GreaterThanOrEqual),
                    keyword(Token::LtEq).to(BinaryOperator::LessThanOrEqual),
                    keyword(Token::Gt).to(BinaryOperator::GreaterThan),
                    keyword(Token::Lt).to(BinaryOperator::LessThan),
                ))
                .then(additive.clone())
                .repeated(),
                |left, (operator, right)| Expression::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            )
            .boxed();
        let and = comparison
            .clone()
            .foldl(
                keyword(Token::And)
                    .to(BinaryOperator::And)
                    .then(comparison.clone())
                    .repeated(),
                |left, (operator, right)| Expression::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            )
            .boxed();
        and.clone()
            .foldl(
                keyword(Token::Or)
                    .to(BinaryOperator::Or)
                    .then(and)
                    .repeated(),
                |left, (operator, right)| Expression::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            )
            .boxed()
    })
}

fn assignment_target<'src, I>()
-> impl Parser<'src, I, AssignmentTarget, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    raw_identifier()
        .then(keyword(Token::Dot).ignore_then(identifier()).or_not())
        .try_map(|(first, field), span| match field {
            None => nervix_models::Identifier::parse(&first)
                .map(AssignmentTarget::bare)
                .map_err(|error| Rich::custom(span, error.to_string())),
            Some(field) => {
                let scope = match first.to_ascii_lowercase().as_str() {
                    "message" => AssignmentTargetScope::Message,
                    "output" => AssignmentTargetScope::Output,
                    "branch" => AssignmentTargetScope::Branch,
                    _ => {
                        return Err(Rich::custom(
                            span,
                            "SET targets must be bare, message.<field>, output.<field>, or \
                             branch.<field>",
                        ));
                    }
                };
                Ok(AssignmentTarget { scope, field })
            }
        })
}

fn inheritance<'src, I>() -> impl Parser<'src, I, Inheritance, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    let all = keyword(Token::All)
        .ignore_then(
            keyword(Token::Except)
                .ignore_then(
                    identifier()
                        .separated_by(keyword(Token::Comma))
                        .at_least(1)
                        .collect::<Vec<_>>(),
                )
                .or_not(),
        )
        .try_map(|except, span| {
            if let Some(fields) = except {
                reject_duplicate_identifiers(&fields, span)?;
                Ok(Inheritance::AllExcept(fields.to_vec()))
            } else {
                Ok(Inheritance::All)
            }
        });
    let explicit = identifier()
        .then(
            keyword(Token::Leak)
                .ignore_then(keyword(Token::Sensitive))
                .or_not(),
        )
        .map(|(field, leak_sensitive)| InheritedField {
            field,
            leak_sensitive: leak_sensitive.is_some(),
        })
        .separated_by(keyword(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
        .try_map(|fields, span| {
            reject_duplicate_identifiers(
                &fields
                    .iter()
                    .map(|field| field.field.clone())
                    .collect::<Vec<_>>(),
                span,
            )?;
            Ok(Inheritance::Fields(fields))
        });
    keyword(Token::Inherit).ignore_then(choice((all, explicit)))
}

fn reject_duplicate_identifiers<'src>(
    fields: &[nervix_models::Identifier],
    span: Span,
) -> Result<(), Rich<'src, Token>> {
    let mut seen = HashSet::new();
    for field in fields {
        if !seen.insert(field.as_str()) {
            return Err(Rich::custom(
                span,
                format!("duplicate field '{}'", field.as_str()),
            ));
        }
    }
    Ok(())
}

fn parser<'src, I>() -> impl Parser<'src, I, RouteConstruction, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    let assignment = assignment_target()
        .then_ignore(keyword(Token::Eq))
        .then(expression())
        .map(|(target, value)| Assignment { target, value });
    let assignments = keyword(Token::Set).ignore_then(
        assignment
            .separated_by(keyword(Token::Comma))
            .at_least(1)
            .collect::<Vec<_>>(),
    );
    let invocation = identifier()
        .then(
            expression()
                .separated_by(keyword(Token::Comma))
                .allow_trailing()
                .collect::<Vec<_>>()
                .delimited_by(keyword(Token::LParen), keyword(Token::RParen)),
        )
        .map(|(function, arguments)| Invocation {
            function,
            arguments,
        });
    let invocations = keyword(Token::Invoke).ignore_then(
        invocation
            .separated_by(keyword(Token::Comma))
            .at_least(1)
            .collect::<Vec<_>>(),
    );

    inheritance()
        .or_not()
        .then(assignments.or_not())
        .then(keyword(Token::Where).ignore_then(expression()).or_not())
        .then(invocations.or_not())
        .try_map(
            |(((inherit, assignments), where_clause), invocations), span| {
                if inherit.is_none()
                    && assignments.is_none()
                    && where_clause.is_none()
                    && invocations.is_none()
                {
                    return Err(Rich::custom(
                        span,
                        "expected INHERIT, SET, WHERE, or INVOKE clause",
                    ));
                }
                Ok(RouteConstruction {
                    inherit,
                    assignments: assignments.unwrap_or_default(),
                    where_clause,
                    invocations: invocations.unwrap_or_default(),
                })
            },
        )
        .boxed()
}

fn parse_tokens(tokens: &[SpannedToken]) -> Result<RouteConstruction, Vec<ParseError<'_>>> {
    let end_span = tokens
        .last()
        .map(|token| token.span.end..token.span.end)
        .unwrap_or(0..0);
    let input = Stream::from_iter(
        tokens
            .iter()
            .cloned()
            .map(|token| (token.token, token.span)),
    )
    .map(end_span.into(), |(token, span)| (token, span));
    parser().then_ignore(end()).parse(input).into_result()
}

pub fn parse_route_construction(input: &str) -> Result<RouteConstruction, ParseFromSourceError> {
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

pub fn parse_expression(input: &str) -> Result<Expression, ParseFromSourceError> {
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
    let end_span = tokens
        .last()
        .map(|token| token.span.end..token.span.end)
        .unwrap_or(0..0);
    let input = Stream::from_iter(
        tokens
            .iter()
            .cloned()
            .map(|token| (token.token, token.span)),
    )
    .map(end_span.into(), |(token, span)| (token, span));
    expression()
        .then_ignore(end())
        .parse(input)
        .into_result()
        .map_err(|errors| ParseFromSourceError::Parse {
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

pub fn parse_expression_list(input: &str) -> Result<Vec<Expression>, ParseFromSourceError> {
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
    let end_span = tokens
        .last()
        .map(|token| token.span.end..token.span.end)
        .unwrap_or(0..0);
    let input = Stream::from_iter(
        tokens
            .iter()
            .cloned()
            .map(|token| (token.token, token.span)),
    )
    .map(end_span.into(), |(token, span)| (token, span));
    expression()
        .separated_by(keyword(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
        .then_ignore(end())
        .parse(input)
        .into_result()
        .map_err(|errors| ParseFromSourceError::Parse {
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
    use super::*;

    #[test]
    fn preserves_udf_namespace_in_the_public_expression_model() {
        let expression =
            parse_expression("udf::add_one(input.value)").expect("qualified UDF call must parse");
        assert!(matches!(
            expression,
            Expression::UdfCall {
                ref function,
                ref arguments,
            } if function.as_str() == "add_one" && arguments.len() == 1
        ));

        assert!(matches!(
            parse_expression("add_one(input.value)").expect("bare call remains valid syntax"),
            Expression::Call { ref function, .. } if function.as_str() == "add_one"
        ));
        assert!(parse_expression("builtin::add_one(input.value)").is_err());
    }
}
