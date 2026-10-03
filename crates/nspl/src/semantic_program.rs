//! The expression grammar: expressions, expression lists and route constructions, read from the
//! tokens of the one NSPL lexer.
//!
//! Layer: language.
//!
//! - **Owns.** The grammar of an expression and of the route construction around it, the readers a
//!   statement applies to the tokens of an expression it embeds, and the standalone readers the
//!   web console's forms, `nervix-cli subscribe --where` and the client library call.
//! - **Depends on.** The shared lexer, its tokens and the shared grammar primitives, and the
//!   vocabulary's expression Models.
//! - **Must not know.** Statement grammars, VM programs, registry state or runtime execution.

use std::num::NonZeroU32;

use ahash_compile_time::{HashSet, HashSetExt};
use chumsky::{Boxed, prelude::*};
use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{
    Assignment, AssignmentTarget, AssignmentTargetScope, BinaryOperator, BuiltinFunctionName,
    CaseBranch, Expression, FieldName, FieldReference, FieldScope, Float64Literal, Inheritance,
    InheritedField, Invocation, JsonPath, Literal, MembershipOperator, NameError, ParseAsType,
    RangeOperator, RelayName, RouteConstruction, UdfName, UnaryOperator,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, into_parse_error, kw, kw_phrase, lex_input,
        tok,
    },
};

type Span = SimpleSpan<usize>;

/// A plain word an expression reads where a keyword may also stand: any word but a keyword the
/// expression grammar reserves. It names a scope, a type, a call or a bare field.
fn raw_identifier<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::Word(Word::UnknownWord(raw)) => raw,
        Token::Word(Word::KnownWord { iden, raw }) if !iden.is_expression_keyword() => raw,
    }
    .labelled("identifier")
}

/// A name written between backticks, which reads as a name wherever it stands, even when it spells
/// a reserved word or holds a character a plain word cannot.
fn quoted_name<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::Word(Word::Quoted(raw)) => raw,
    }
    .labelled("identifier")
}

/// The text of a name written where a keyword may also stand, as a call or a bare field is: a plain
/// word the expression grammar does not reserve, or any name between backticks.
fn name_word<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((raw_identifier(), quoted_name()))
}

/// The text of a name written after a scope's `.` or after `udf::`, where no keyword can stand: any
/// word, a reserved one too, or a name between backticks.
fn qualified_name_word<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    select! {
        Token::Word(Word::UnknownWord(raw)) => raw,
        Token::Word(Word::KnownWord { raw, .. }) => raw,
        Token::Word(Word::Quoted(raw)) => raw,
    }
    .labelled("identifier")
}

/// Parse a name written where a keyword may also stand as the named concept `N`, validated by the
/// name type's own constructor.
fn name<'src, N: Clone + 'static>(
    parse: fn(&str) -> Result<N, Report<NameError>>,
) -> impl Parser<'src, &'src [Token], N, extra::Err<ParseError<'src>>> + Clone {
    name_word().try_map(move |raw: String, span| {
        parse(&raw).map_err(|error| Rich::custom(span, error.to_string()))
    })
}

/// Parse a name written after a scope's `.` or after `udf::` as the named concept `N`, validated
/// by the name type's own constructor.
fn qualified_name<'src, N: Clone + 'static>(
    parse: fn(&str) -> Result<N, Report<NameError>>,
) -> impl Parser<'src, &'src [Token], N, extra::Err<ParseError<'src>>> + Clone {
    qualified_name_word().try_map(move |raw: String, span| {
        parse(&raw).map_err(|error| Rich::custom(span, error.to_string()))
    })
}

fn parse_scope<'src>(name: &str, span: Span) -> Result<FieldScope, ParseError<'src>> {
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

fn field_reference<'src>()
-> impl Parser<'src, &'src [Token], FieldReference, extra::Err<ParseError<'src>>> + Clone {
    let scoped = raw_identifier()
        .then_ignore(tok(Token::Dot))
        .then(qualified_name_word())
        .then(
            tok(Token::Dot)
                .ignore_then(qualified_name(FieldName::parse))
                .or_not(),
        )
        .try_map(|((scope, second), third), span| match third {
            Some(field) if scope.eq_ignore_ascii_case("relay_state") => {
                let relay = RelayName::try_from(second.as_str())
                    .map_err(|error| Rich::custom(span, error.to_string()))?;
                Ok(FieldReference::scoped(
                    FieldScope::RelayState { relay },
                    field,
                ))
            }
            Some(_) => Err(Rich::custom(
                span,
                "only relay_state.<relay>.<field> may contain three field path segments",
            )),
            None if scope.eq_ignore_ascii_case("relay_state") => Err(Rich::custom(
                span,
                "relay_state references require relay_state.<relay>.<field>",
            )),
            None => {
                let field = FieldName::try_from(second.as_str())
                    .map_err(|error| Rich::custom(span, error.to_string()))?;
                parse_scope(&scope, span).map(|scope| FieldReference::scoped(scope, field))
            }
        });
    let bare = name(FieldName::parse).map(FieldReference::bare);

    choice((scoped, bare)).boxed()
}

fn cast_type<'src>()
-> impl Parser<'src, &'src [Token], ParseAsType, extra::Err<ParseError<'src>>> + Clone {
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
            "BYTES" => ParseAsType::Bytes,
            "DATETIME" => ParseAsType::Datetime,
            "FLOAT32" | "F32" => ParseAsType::F32,
            "FLOAT64" | "F64" => ParseAsType::F64,
            _ => return Err(Rich::custom(span, format!("unsupported type '{name}'"))),
        };
        Ok(ty)
    })
}

/// A declared result type: a scalar type as `cast_type` reads it, or a `VEC<...>` or
/// `ARRAY<..., n>` of declared types written as a schema field declares them.
fn declared_type<'src>()
-> impl Parser<'src, &'src [Token], ParseAsType, extra::Err<ParseError<'src>>> + Clone {
    recursive(|declared| {
        let collection = |spelling: &'static str| {
            raw_identifier()
                .try_map(move |name, span| {
                    if name.eq_ignore_ascii_case(spelling) {
                        Ok(())
                    } else {
                        Err(Rich::custom(span, format!("expected {spelling}")))
                    }
                })
                .labelled(spelling)
        };
        // The label goes on the number itself, as the schema grammar does, so the checks below
        // keep their own explanation.
        let array_len = select! { Token::NumberLiteral(raw) => raw }
            .labelled("array_length")
            .try_map(|raw: String, span| {
                let not_positive =
                    || Rich::custom(span, "array length must be a positive unsigned integer");
                let Ok(len) = raw.parse::<u32>() else {
                    return Err(not_positive());
                };
                let Some(len) = NonZeroU32::new(len) else {
                    return Err(not_positive());
                };
                // An array becomes an Arrow fixed-size list, whose length is an i32.
                if i32::try_from(len.get()).is_err() {
                    return Err(Rich::custom(
                        span,
                        "array length must not exceed 2147483647",
                    ));
                }
                Ok(len)
            });
        let array = collection("ARRAY").ignore_then(
            declared
                .clone()
                .then_ignore(tok(Token::Comma))
                .then(
                    array_len
                        .separated_by(tok(Token::Comma))
                        .at_least(1)
                        .collect::<Vec<_>>(),
                )
                .delimited_by(tok(Token::Lt), tok(Token::Gt))
                .map(|(element, lengths)| {
                    // `ARRAY<F32, 2, 3>` is two arrays of three, so the last length is innermost.
                    let mut declared = element;
                    for len in lengths.into_iter().rev() {
                        declared = ParseAsType::Array {
                            element: Box::new(declared),
                            len,
                        };
                    }
                    declared
                }),
        );
        let vector = collection("VEC").ignore_then(
            declared
                .delimited_by(tok(Token::Lt), tok(Token::Gt))
                .map(|element| ParseAsType::Vec {
                    element: Box::new(element),
                }),
        );
        choice((array, vector, cast_type())).boxed()
    })
}

/// The string literal naming a JSON path, parsed into the steps it takes.
fn json_path<'src>()
-> impl Parser<'src, &'src [Token], JsonPath, extra::Err<ParseError<'src>>> + Clone {
    select! { Token::StringLiteral(text) => text }
        .labelled("json_path")
        .try_map(|text: String, span| {
            JsonPath::parse(&text).map_err(|error| {
                Rich::custom(
                    span,
                    format!("invalid JSON path '{text}': {}", error.current_context()),
                )
            })
        })
}

/// A number literal as an expression reads it: decimal digits are an `I64`, and digits with a
/// fraction an `F64`.
///
/// The lexer reads a number with its fraction and its exponent as one token, so the `.` of a float
/// is part of the literal only when no space separates it from the digits around it. A number with
/// an exponent is one token too, and an expression rejects it rather than reading it as another
/// number.
fn number_literal<'src>()
-> impl Parser<'src, &'src [Token], Literal, extra::Err<ParseError<'src>>> + Clone {
    select! { Token::NumberLiteral(raw) => raw }
        .labelled("number_literal")
        .try_map(|raw: String, span| {
            if raw.contains(['e', 'E']) {
                return Err(Rich::custom(
                    span,
                    format!(
                        "number literal '{raw}' has an exponent, which an expression does not \
                         read; write '{raw}' AS F64"
                    ),
                ));
            }
            if raw.contains('.') {
                let value = raw.parse::<f64>().assured(
                    "the lexer writes a fraction as digits, a dot and digits, which always read \
                     as an f64",
                );
                return Ok(Literal::F64(Float64Literal::new(value)));
            }
            let value = raw.parse::<i64>().map_err(|error| {
                Rich::custom(span, format!("invalid integer literal '{raw}': {error}"))
            })?;
            Ok(Literal::I64(value))
        })
}

/// One comparison-level operation, applied to the expression parsed before it.
///
/// Comparisons fold left, so each one takes the whole comparison to its left as its operand:
/// `a = b IN (TRUE)` tests whether `a = b` is an element of the set.
enum ComparisonSuffix {
    Binary {
        operator: BinaryOperator,
        right: Expression,
    },
    Membership {
        operator: MembershipOperator,
        set: Vec<Expression>,
    },
    Range {
        operator: RangeOperator,
        low: Expression,
        high: Expression,
    },
}

impl ComparisonSuffix {
    fn apply(self, operand: Expression) -> Expression {
        match self {
            Self::Binary { operator, right } => Expression::Binary {
                operator,
                left: Box::new(operand),
                right: Box::new(right),
            },
            Self::Membership { operator, set } => Expression::Membership {
                operator,
                operand: Box::new(operand),
                set,
            },
            Self::Range {
                operator,
                low,
                high,
            } => Expression::Range {
                operator,
                operand: Box::new(operand),
                low: Box::new(low),
                high: Box::new(high),
            },
        }
    }
}

/// A postfix `AS <type>`, which converts the operand before it.
fn cast_suffix<'src>()
-> impl Parser<'src, &'src [Token], ParseAsType, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::As).ignore_then(cast_type())
}

/// An operand followed by the postfix casts `suffix` reads, each converting everything before it.
fn cast_chain<'src>(
    operand: impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone + 'src,
    suffix: impl Parser<'src, &'src [Token], ParseAsType, extra::Err<ParseError<'src>>> + Clone + 'src,
) -> impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone {
    operand
        .then(suffix.repeated().collect::<Vec<_>>())
        .map(|(value, casts)| {
            casts
                .into_iter()
                .fold(value, |expression, target| Expression::Cast {
                    expression: Box::new(expression),
                    target,
                })
        })
        .boxed()
}

/// The prefix, arithmetic, comparison and logical operators, from the tightest binding to the
/// loosest, over the cast chains `cast_chain` reads. `set` reads the set of an `IN` test.
fn operator_ladder<'src>(
    cast_chain: impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>>
    + Clone
    + 'src,
    set: impl Parser<'src, &'src [Token], Vec<Expression>, extra::Err<ParseError<'src>>> + Clone + 'src,
) -> impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone {
    let unary = choice((
        tok(Token::Hyphen).to(UnaryOperator::Negate),
        kw(Identifier::Not).to(UnaryOperator::Not),
    ))
    .repeated()
    .collect::<Vec<_>>()
    .then(cast_chain)
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
                tok(Token::Star).to(BinaryOperator::Multiply),
                tok(Token::Slash).to(BinaryOperator::Divide),
                tok(Token::Percent).to(BinaryOperator::Remainder),
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
                tok(Token::Plus).to(BinaryOperator::Add),
                tok(Token::Hyphen).to(BinaryOperator::Subtract),
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
    let comparison_operator = choice((
        tok(Token::Eq).to(BinaryOperator::Equal),
        tok(Token::NotEq).to(BinaryOperator::NotEqual),
        tok(Token::GtEq).to(BinaryOperator::GreaterThanOrEqual),
        tok(Token::LtEq).to(BinaryOperator::LessThanOrEqual),
        tok(Token::Gt).to(BinaryOperator::GreaterThan),
        tok(Token::Lt).to(BinaryOperator::LessThan),
        kw_phrase([Identifier::Is, Identifier::Distinct, Identifier::From])
            .to(BinaryOperator::IsDistinctFrom),
        kw_phrase([
            Identifier::Is,
            Identifier::Not,
            Identifier::Distinct,
            Identifier::From,
        ])
        .to(BinaryOperator::IsNotDistinctFrom),
    ));
    let membership_operator = choice((
        kw(Identifier::In).to(MembershipOperator::In),
        kw_phrase([Identifier::Not, Identifier::In]).to(MembershipOperator::NotIn),
    ));
    let range_operator = choice((
        kw(Identifier::Between).to(RangeOperator::Between),
        kw_phrase([Identifier::Not, Identifier::Between]).to(RangeOperator::NotBetween),
    ));
    // Both bounds are read one level tighter than a comparison, so the `AND` that closes the low
    // bound belongs to the range and a later `AND` combines the whole range test.
    let comparison_suffix = choice((
        comparison_operator
            .then(additive.clone())
            .map(|(operator, right)| ComparisonSuffix::Binary { operator, right }),
        membership_operator
            .then(set)
            .map(|(operator, set)| ComparisonSuffix::Membership { operator, set }),
        range_operator
            .then(additive.clone())
            .then_ignore(kw(Identifier::And))
            .then(additive.clone())
            .map(|((operator, low), high)| ComparisonSuffix::Range {
                operator,
                low,
                high,
            }),
    ));
    let comparison = additive
        .clone()
        .foldl(comparison_suffix.repeated(), |operand, suffix| {
            suffix.apply(operand)
        })
        .boxed();
    let and = comparison
        .clone()
        .foldl(
            kw(Identifier::And)
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
            kw(Identifier::Or)
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
}

fn expression<'src>()
-> impl Parser<'src, &'src [Token], Expression, extra::Err<ParseError<'src>>> + Clone {
    recursive(|expression| {
        // A call's arguments and the set of an `IN` test share one written form, so a set may be
        // empty and may end with a comma.
        let arguments = expression
            .clone()
            .separated_by(tok(Token::Comma))
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(tok(Token::LParen), tok(Token::RParen))
            .boxed();
        let set = arguments.clone();
        // A tolerant conversion is an atom whose operand is itself built from atoms, so atoms are
        // recursive in their own right.
        let atom = recursive(|atom| {
            let literal = choice((
                number_literal().map(Expression::Literal),
                kw(Identifier::True).to(Expression::Literal(Literal::Bool(true))),
                kw(Identifier::False).to(Expression::Literal(Literal::Bool(false))),
                kw(Identifier::Null).to(Expression::Literal(Literal::Null)),
                select! { Token::StringLiteral(value) => Expression::Literal(Literal::String(value)) }
                    .labelled("string_literal"),
            ));
            let udf_call = kw(Identifier::Udf)
                .then_ignore(tok(Token::DoubleColon))
                .then(qualified_name(UdfName::parse))
                .then(arguments.clone())
                .map(|(((), function), arguments)| Expression::UdfCall {
                    function,
                    arguments,
                });
            let function_call = name(BuiltinFunctionName::parse)
                .then(arguments.clone())
                .map(|(function, arguments)| Expression::Call {
                    function,
                    arguments,
                });
            let array = expression
                .clone()
                .separated_by(tok(Token::Comma))
                .at_least(1)
                .allow_trailing()
                .collect::<Vec<_>>()
                .delimited_by(tok(Token::LBracket), tok(Token::RBracket))
                .map(Expression::Array);
            let when_clause = kw(Identifier::When)
                .ignore_then(expression.clone())
                .then_ignore(kw(Identifier::Then))
                .then(expression.clone())
                .map(|(when, result)| CaseBranch { when, result });
            let case_expression = kw(Identifier::Case)
                .ignore_then(expression.clone().or_not())
                .then(when_clause.repeated().at_least(1).collect::<Vec<_>>())
                .then(
                    kw(Identifier::Else)
                        .ignore_then(expression.clone())
                        .or_not(),
                )
                .then_ignore(kw(Identifier::End))
                .map(|((operand, branches), else_result)| Expression::Case {
                    operand: operand.map(Box::new),
                    branches,
                    else_result: else_result.map(Box::new),
                });
            let if_expression = kw(Identifier::If)
                .ignore_then(expression.clone())
                .then_ignore(kw(Identifier::Then))
                .then(expression.clone())
                .then_ignore(kw(Identifier::Else))
                .then(expression.clone())
                .then_ignore(kw(Identifier::End))
                .map(|((condition, then_result), else_result)| Expression::If {
                    condition: Box::new(condition),
                    then_result: Box::new(then_result),
                    else_result: Box::new(else_result),
                });
            // `TRY_CAST(<operand> AS <type>)` converts the whole expression before its final
            // `AS`. The operand reads every other postfix cast as its own and leaves the one the
            // closing parenthesis follows to the conversion.
            let conversion = kw(Identifier::As)
                .ignore_then(cast_type())
                .then_ignore(tok(Token::RParen));
            let operand_cast_suffix = cast_suffix().and_is(conversion.clone().not());
            let try_cast = kw(Identifier::TryCast)
                .ignore_then(tok(Token::LParen))
                .ignore_then(operator_ladder(
                    cast_chain(atom, operand_cast_suffix),
                    arguments.clone(),
                ))
                .then(conversion)
                .map(|(expression, target)| Expression::TryCast {
                    expression: Box::new(expression),
                    target,
                });
            // `JSON_VALUE(<document>, '<path>' AS <type>)` and its tolerant form read one value
            // of a declared type, and `JSON_EXISTS(<document>, '<path>')` tests for one.
            let json_source = tok(Token::LParen)
                .ignore_then(expression.clone())
                .then_ignore(tok(Token::Comma))
                .then(json_path());
            let json_read = json_source
                .clone()
                .then_ignore(kw(Identifier::As))
                .then(declared_type())
                .then_ignore(tok(Token::RParen));
            let json_value = kw(Identifier::JsonValue)
                .ignore_then(json_read.clone())
                .map(|((document, path), target)| Expression::JsonValue {
                    document: Box::new(document),
                    path,
                    target,
                });
            let try_json_value = kw(Identifier::TryJsonValue).ignore_then(json_read).map(
                |((document, path), target)| Expression::TryJsonValue {
                    document: Box::new(document),
                    path,
                    target,
                },
            );
            let json_exists = kw(Identifier::JsonExists)
                .ignore_then(json_source)
                .then_ignore(tok(Token::RParen))
                .map(|(document, path)| Expression::JsonExists {
                    document: Box::new(document),
                    path,
                });
            choice((
                literal,
                udf_call,
                function_call,
                array,
                if_expression,
                case_expression,
                try_cast,
                json_value,
                try_json_value,
                json_exists,
                field_reference().map(Expression::Field),
                expression
                    .clone()
                    .delimited_by(tok(Token::LParen), tok(Token::RParen)),
            ))
        });
        operator_ladder(cast_chain(atom, cast_suffix()), set)
    })
}

/// The field a `SET` or `DEFAULT` assignment writes: a bare field, or `message.`, `output.` or
/// `branch.` followed by the field. A scope is a plain word, so a name between backticks is always
/// a bare target.
fn assignment_target<'src>()
-> impl Parser<'src, &'src [Token], AssignmentTarget, extra::Err<ParseError<'src>>> + Clone {
    let quoted = quoted_name().try_map(|raw: String, span| {
        FieldName::try_from(raw.as_str())
            .map(AssignmentTarget::bare)
            .map_err(|error| Rich::custom(span, error.to_string()))
    });
    let plain = raw_identifier()
        .then(
            tok(Token::Dot)
                .ignore_then(qualified_name(FieldName::parse))
                .or_not(),
        )
        .try_map(|(first, field), span| match field {
            None => FieldName::try_from(first.as_str())
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
        });
    choice((plain, quoted))
}

/// The `<target> = <expression>` assignments that `SET` and a materialized-state `DEFAULT` write,
/// each after the `separator` before it.
fn assignments<'src, S>(
    separator: S,
) -> impl Parser<'src, &'src [Token], Vec<Assignment>, extra::Err<ParseError<'src>>> + Clone
where
    S: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    assignment_target()
        .then_ignore(tok(Token::Eq))
        .then(expression())
        .map(|(target, value)| Assignment { target, value })
        .separated_by(separator)
        .at_least(1)
        .collect::<Vec<_>>()
        .boxed()
}

/// What `INHERIT` inherits, read after the keyword: `ALL`, `ALL EXCEPT` fields, or fields each
/// optionally leaking their sensitivity. A listed field follows the `separator` before it.
fn inheritance<'src, S>(
    separator: S,
) -> impl Parser<'src, &'src [Token], Inheritance, extra::Err<ParseError<'src>>> + Clone
where
    S: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let all = kw(Identifier::All)
        .ignore_then(
            kw(Identifier::Except)
                .ignore_then(
                    name(FieldName::parse)
                        .separated_by(separator.clone())
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
    let explicit = name(FieldName::parse)
        .then(
            kw(Identifier::Leak)
                .ignore_then(kw(Identifier::Sensitive))
                .or_not(),
        )
        .map(|(field, leak_sensitive)| InheritedField {
            field,
            leak_sensitive: leak_sensitive.is_some(),
        })
        .separated_by(separator)
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
    choice((all, explicit)).boxed()
}

fn reject_duplicate_identifiers<'src>(
    fields: &[FieldName],
    span: Span,
) -> Result<(), ParseError<'src>> {
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

/// A clause of a route construction. A route writes its clauses in this order, each at most once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteClause {
    Inherit,
    Set,
    Where,
    Invoke,
}

impl RouteClause {
    /// Every clause, in the order a route writes them.
    const ALL: [Self; 4] = [Self::Inherit, Self::Set, Self::Where, Self::Invoke];

    /// The keyword that begins the clause.
    pub(crate) fn keyword(self) -> Identifier {
        match self {
            Self::Inherit => Identifier::Inherit,
            Self::Set => Identifier::Set,
            Self::Where => Identifier::Where,
            Self::Invoke => Identifier::Invoke,
        }
    }

    /// The clauses a route may write after this one, in the order it writes them.
    fn later(self) -> &'static [Self] {
        match self {
            Self::Inherit => &[Self::Set, Self::Where, Self::Invoke],
            Self::Set => &[Self::Where, Self::Invoke],
            Self::Where => &[Self::Invoke],
            Self::Invoke => &[],
        }
    }

    /// The label completion offers where the clause's body begins, right after its keyword.
    pub(crate) fn body_label(self) -> &'static str {
        match self {
            Self::Inherit => "inherit_targets",
            Self::Set => "set_assignments",
            Self::Where => "where_expression",
            Self::Invoke => "invocations",
        }
    }

    /// The clause's body, read after its keyword. A list the body holds continues past a comma only
    /// where `separator` accepts it.
    fn body<'src, S>(
        self,
        separator: S,
    ) -> Boxed<'src, 'src, &'src [Token], RoutePart, extra::Err<ParseError<'src>>>
    where
        S: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
    {
        match self {
            Self::Inherit => inheritance(separator).map(RoutePart::Inherit).boxed(),
            Self::Set => assignments(separator).map(RoutePart::Set).boxed(),
            Self::Where => expression().map(RoutePart::Where).boxed(),
            Self::Invoke => invocations(separator).map(RoutePart::Invoke).boxed(),
        }
    }
}

/// One clause of a route construction, as its body reads.
enum RoutePart {
    Inherit(Inheritance),
    Set(Vec<Assignment>),
    Where(Expression),
    Invoke(Vec<Invocation>),
}

impl RoutePart {
    /// Records the clause in the construction it belongs to.
    fn apply_to(self, construction: &mut RouteConstruction) {
        match self {
            Self::Inherit(inherit) => construction.inherit = Some(inherit),
            Self::Set(assignments) => construction.assignments = assignments,
            Self::Where(where_clause) => construction.where_clause = Some(where_clause),
            Self::Invoke(invocations) => construction.invocations = invocations,
        }
    }
}

/// The calls `INVOKE` makes, read after the keyword, each after the `separator` before it.
fn invocations<'src, S>(
    separator: S,
) -> impl Parser<'src, &'src [Token], Vec<Invocation>, extra::Err<ParseError<'src>>> + Clone
where
    S: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    name(BuiltinFunctionName::parse)
        .then(
            expression()
                .separated_by(tok(Token::Comma))
                .allow_trailing()
                .collect::<Vec<_>>()
                .delimited_by(tok(Token::LParen), tok(Token::RParen)),
        )
        .map(|(function, arguments)| Invocation {
            function,
            arguments,
        })
        .separated_by(separator)
        .at_least(1)
        .collect::<Vec<_>>()
        .boxed()
}

/// A route construction whose first clause is `first`, read from just after that clause's keyword:
/// its body, then each clause the route may write after it, in order.
fn route_construction_after<'src, S>(
    first: RouteClause,
    separator: S,
) -> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone
where
    S: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let mut parts = first.body(separator.clone()).map(|part| vec![part]).boxed();
    for later in first.later() {
        let clause = kw(later.keyword())
            .ignore_then(later.body(separator.clone()))
            .or_not();
        parts = parts
            .then(clause)
            .map(|(mut parts, part)| {
                if let Some(part) = part {
                    parts.push(part);
                }
                parts
            })
            .boxed();
    }
    parts.map(|parts| {
        let mut construction = RouteConstruction::default();
        for part in parts {
            part.apply_to(&mut construction);
        }
        construction
    })
}

/// A route construction: `INHERIT`, `SET`, `WHERE` and `INVOKE` clauses in that order, each
/// optional but at least one of them written.
fn route_construction<'src>()
-> impl Parser<'src, &'src [Token], RouteConstruction, extra::Err<ParseError<'src>>> + Clone {
    let starts = RouteClause::ALL.map(|first| {
        kw(first.keyword())
            .ignore_then(route_construction_after(first, tok(Token::Comma)))
            .boxed()
    });
    choice(starts).boxed()
}

/// What a reader took from the front of the tokens a statement handed it: what it read, and how
/// many tokens that was. The statement goes on right after them.
pub(crate) struct Prefix<O> {
    pub(crate) output: O,
    pub(crate) length: usize,
}

/// Reads the longest run at the front of `tokens` that `grammar` accepts.
///
/// A statement hands an embedded part every token after the keyword that introduces it, so the part
/// ends where its own grammar cannot go on, and the statement's next clause begins there. A word
/// the expression grammar does not reserve is a name wherever a name may stand, so the next clause
/// begins at its keyword only once the expression before it is complete.
///
/// The part ends before a token only when its grammar cannot go on with that token at all. Where
/// the grammar does go on, as it does into the `(` of an `IN` set that is never closed, and then
/// fails further along, the tokens after the run continue the part and the part is malformed: the
/// rejection is where that continuation fails, not at the token after the run.
fn read_prefix<'src, O>(
    grammar: impl Parser<'src, &'src [Token], O, extra::Err<ParseError<'src>>> + Clone,
    tokens: &'src [Token],
) -> Result<Prefix<O>, Vec<ParseError<'src>>> {
    let prefix = grammar
        .clone()
        .map_with(|output, extra| Prefix {
            output,
            length: extra.span().end,
        })
        .lazy()
        .parse(tokens)
        .into_result()?;
    let Err(errors) = grammar.then_ignore(end()).parse(tokens).into_result() else {
        return Ok(prefix);
    };
    let mut continued = false;
    for error in &errors {
        if error.span().start > prefix.length {
            continued = true;
        }
    }
    if continued {
        return Err(errors);
    }
    Ok(prefix)
}

/// Reads `tokens` as exactly one expression.
pub(crate) fn read_expression(tokens: &[Token]) -> Result<Expression, Vec<ParseError<'_>>> {
    expression().then_ignore(end()).parse(tokens).into_result()
}

/// Reads one or more comma-separated expressions from `tokens`, all of them.
pub(crate) fn read_expression_list(
    tokens: &[Token],
) -> Result<Vec<Expression>, Vec<ParseError<'_>>> {
    expression()
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
        .then_ignore(end())
        .parse(tokens)
        .into_result()
}

/// Reads `tokens` as a route construction: `INHERIT`, `SET`, `WHERE` and `INVOKE` clauses in that
/// order, each optional but at least one of them written.
pub(crate) fn read_route_construction(
    tokens: &[Token],
) -> Result<RouteConstruction, Vec<ParseError<'_>>> {
    route_construction()
        .then_ignore(end())
        .parse(tokens)
        .into_result()
}

/// Reads the expression at the front of `tokens`.
pub(crate) fn read_expression_prefix(
    tokens: &[Token],
) -> Result<Prefix<Expression>, Vec<ParseError<'_>>> {
    read_prefix(expression(), tokens)
}

/// Reads the comma-separated expressions at the front of `tokens`, going on past a comma only where
/// `separator` accepts it.
pub(crate) fn read_expression_list_prefix<'src, S>(
    tokens: &'src [Token],
    separator: S,
) -> Result<Prefix<Vec<Expression>>, Vec<ParseError<'src>>>
where
    S: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    read_prefix(
        expression()
            .separated_by(separator)
            .at_least(1)
            .collect::<Vec<_>>(),
        tokens,
    )
}

/// Reads the route construction at the front of `tokens`, which begin just after the keyword of its
/// first clause, `first`. A list it holds goes on past a comma only where `separator` accepts it.
pub(crate) fn read_route_construction_prefix<'src, S>(
    tokens: &'src [Token],
    first: RouteClause,
    separator: S,
) -> Result<Prefix<RouteConstruction>, Vec<ParseError<'src>>>
where
    S: Parser<'src, &'src [Token], (), extra::Err<ParseError<'src>>> + Clone + 'src,
{
    read_prefix(route_construction_after(first, separator), tokens)
}

/// Reads the assignments of a materialized-state `DEFAULT` at the front of `tokens`.
pub(crate) fn read_assignments_prefix(
    tokens: &[Token],
) -> Result<Prefix<Vec<Assignment>>, Vec<ParseError<'_>>> {
    read_prefix(assignments(tok(Token::Comma)), tokens)
}

/// Lexes `input` on its own and reads its tokens with `read`, reporting a rejection at its place in
/// `input`: the lexer and diagnostics of a statement, applied to text that is not one.
fn read_standalone<O>(
    input: &str,
    read: for<'tokens> fn(&'tokens [Token]) -> Result<O, Vec<ParseError<'tokens>>>,
) -> error_stack::Result<O, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    read(&tokens).map_err(|errors| into_parse_error(source, &spanned_tokens, input.len(), errors))
}

/// Reads `input` as one expression, lexed and parsed exactly as a statement reads an expression it
/// embeds.
pub fn parse_expression(input: &str) -> error_stack::Result<Expression, ParseFromSourceError> {
    read_standalone(input, read_expression)
}

/// Reads `input` as one or more comma-separated expressions, as a statement reads an expression
/// list it embeds.
pub fn parse_expression_list(
    input: &str,
) -> error_stack::Result<Vec<Expression>, ParseFromSourceError> {
    read_standalone(input, read_expression_list)
}

/// Reads `input` as a route construction, as a statement reads the construction of a route.
pub fn parse_route_construction(
    input: &str,
) -> error_stack::Result<RouteConstruction, ParseFromSourceError> {
    read_standalone(input, read_route_construction)
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

    #[test]
    fn parses_collection_expressions_and_rejects_incomplete_elements() {
        assert_eq!(
            parse_expression("[input.first, input.second]").expect("ARRAY elements parse"),
            Expression::Array(vec![field("first"), field("second")]),
        );
        for source in [
            "array(input.first, input.second)",
            "vec(input.first, input.second)",
            "vec()",
            "slice(input.items, 0, 2)",
        ] {
            assert!(matches!(
                parse_expression(source).expect("collection call parses"),
                Expression::Call { .. }
            ));
        }
        for source in [
            "[input.first,,input.second]",
            "[input.first",
            "vec(,input.first)",
        ] {
            assert!(
                parse_expression(source).is_err(),
                "{source} must be rejected"
            );
        }
    }

    fn field(name: &str) -> Expression {
        Expression::Field(FieldReference::scoped(
            FieldScope::Input,
            FieldName::try_from(name).expect("test field names are valid"),
        ))
    }

    fn bare_field(name: &str) -> Expression {
        Expression::Field(FieldReference::bare(
            FieldName::try_from(name).expect("test field names are valid"),
        ))
    }

    fn integer(value: i64) -> Expression {
        Expression::Literal(Literal::I64(value))
    }

    fn string(value: &str) -> Expression {
        Expression::Literal(Literal::String(value.to_string()))
    }

    fn binary(operator: BinaryOperator, left: Expression, right: Expression) -> Expression {
        Expression::Binary {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    #[test]
    fn parses_membership_tests_over_written_sets() {
        assert_eq!(
            parse_expression("input.status in ('open', 'held',)").expect("IN must parse"),
            Expression::Membership {
                operator: MembershipOperator::In,
                operand: Box::new(field("status")),
                set: vec![string("open"), string("held")],
            }
        );
        assert_eq!(
            parse_expression("input.code NOT IN ()").expect("an empty set must parse"),
            Expression::Membership {
                operator: MembershipOperator::NotIn,
                operand: Box::new(field("code")),
                set: Vec::new(),
            }
        );
        let Expression::Membership { set, .. } =
            parse_expression("input.code IN (-1 AS I32, (2) AS I32)").expect("casts must parse")
        else {
            panic!("IN must parse as a membership test");
        };
        assert_eq!(
            set[0],
            Expression::Unary {
                operator: UnaryOperator::Negate,
                expression: Box::new(Expression::Cast {
                    expression: Box::new(integer(1)),
                    target: ParseAsType::I32,
                }),
            }
        );
    }

    #[test]
    fn parses_ranges_whose_and_closes_the_low_bound() {
        assert_eq!(
            parse_expression("input.weight BETWEEN 1 AND 2 + 3 AND input.active")
                .expect("BETWEEN must parse"),
            binary(
                BinaryOperator::And,
                Expression::Range {
                    operator: RangeOperator::Between,
                    operand: Box::new(field("weight")),
                    low: Box::new(integer(1)),
                    high: Box::new(binary(BinaryOperator::Add, integer(2), integer(3))),
                },
                field("active"),
            )
        );
        assert_eq!(
            parse_expression("input.weight not between -1 and 1").expect("NOT BETWEEN must parse"),
            Expression::Range {
                operator: RangeOperator::NotBetween,
                operand: Box::new(field("weight")),
                low: Box::new(Expression::Unary {
                    operator: UnaryOperator::Negate,
                    expression: Box::new(integer(1)),
                }),
                high: Box::new(integer(1)),
            }
        );
    }

    #[test]
    fn parses_null_safe_equality_as_a_comparison() {
        assert_eq!(
            parse_expression(
                "input.region IS DISTINCT FROM input.home OR input.region is not distinct from \
                 'eu'"
            )
            .expect("distinctness must parse"),
            binary(
                BinaryOperator::Or,
                binary(
                    BinaryOperator::IsDistinctFrom,
                    field("region"),
                    field("home"),
                ),
                binary(
                    BinaryOperator::IsNotDistinctFrom,
                    field("region"),
                    string("eu"),
                ),
            )
        );
    }

    #[test]
    fn comparisons_fold_left_across_every_comparison_form() {
        assert_eq!(
            parse_expression("input.a = input.b IN (TRUE) IS DISTINCT FROM FALSE")
                .expect("chained comparisons must parse"),
            binary(
                BinaryOperator::IsDistinctFrom,
                Expression::Membership {
                    operator: MembershipOperator::In,
                    operand: Box::new(binary(BinaryOperator::Equal, field("a"), field("b"))),
                    set: vec![Expression::Literal(Literal::Bool(true))],
                },
                Expression::Literal(Literal::Bool(false)),
            )
        );
    }

    #[test]
    fn rejects_incomplete_comparison_phrases() {
        for source in [
            "input.region IS DISTINCT input.home",
            "input.region IS NOT input.home",
            "input.region IS NULL",
            "input.region DISTINCT FROM input.home",
            "input.region NOT 'eu'",
            "input.region NOT",
            "input.code IN 1",
            "input.code IN (1",
            "input.code IN [1]",
            "IN (1)",
            "input.weight BETWEEN 1",
            "input.weight BETWEEN 1 OR 2",
            "input.weight BETWEEN AND 2",
        ] {
            assert!(parse_expression(source).is_err(), "{source} must not parse");
        }
    }

    #[test]
    fn comparison_keywords_name_fields_only_after_a_scope_or_between_backticks() {
        for keyword in ["in", "between", "is", "distinct", "from"] {
            assert_eq!(
                parsed(&format!("input.{keyword} = 1")),
                binary(BinaryOperator::Equal, field(keyword), integer(1)),
                "input.{keyword} names a field"
            );
            assert!(
                parse_expression(&format!("{keyword} = 1")).is_err(),
                "{keyword} must not parse as a bare field"
            );
            assert_eq!(
                parsed(&format!("`{keyword}` = 1")),
                binary(BinaryOperator::Equal, bare_field(keyword), integer(1)),
                "`{keyword}` names a bare field"
            );
        }
        assert!(parse_expression("input.inbound IN (input.from_unix)").is_ok());
    }

    fn cast(expression: Expression, target: ParseAsType) -> Expression {
        Expression::Cast {
            expression: Box::new(expression),
            target,
        }
    }

    fn try_cast(expression: Expression, target: ParseAsType) -> Expression {
        Expression::TryCast {
            expression: Box::new(expression),
            target,
        }
    }

    fn parsed(source: &str) -> Expression {
        parse_expression(source).unwrap_or_else(|error| panic!("`{source}` must parse: {error}"))
    }

    #[test]
    fn parses_a_tolerant_conversion_in_any_letter_case() {
        let expected = try_cast(field("raw"), ParseAsType::I64);
        assert_eq!(parsed("TRY_CAST(input.raw AS I64)"), expected);
        assert_eq!(parsed("try_cast(input.raw as int64)"), expected);
        assert_eq!(
            parsed("TRY_CAST(input.raw AS DATETIME)"),
            try_cast(field("raw"), ParseAsType::Datetime)
        );
    }

    #[test]
    fn a_tolerant_conversion_converts_the_whole_expression_before_its_final_as() {
        let sum = Expression::Binary {
            operator: BinaryOperator::Add,
            left: Box::new(field("low")),
            right: Box::new(field("high")),
        };
        assert_eq!(
            parsed("TRY_CAST(input.low + input.high AS STRING)"),
            try_cast(sum, ParseAsType::String)
        );
        assert_eq!(
            parsed("TRY_CAST(-input.value AS I32)"),
            try_cast(
                Expression::Unary {
                    operator: UnaryOperator::Negate,
                    expression: Box::new(field("value")),
                },
                ParseAsType::I32,
            )
        );
        // Every postfix cast but the last belongs to the operand.
        assert_eq!(
            parsed("TRY_CAST(input.raw AS I64 AS STRING)"),
            try_cast(cast(field("raw"), ParseAsType::I64), ParseAsType::String)
        );
        // A cast the operand closes in its own parentheses or call stays with it.
        assert_eq!(
            parsed("TRY_CAST((input.raw AS I64) AS STRING)"),
            try_cast(cast(field("raw"), ParseAsType::I64), ParseAsType::String)
        );
        assert_eq!(
            parsed("TRY_CAST(abs(input.raw AS I64) AS U8)"),
            try_cast(
                Expression::Call {
                    function: BuiltinFunctionName::parse("abs").expect("valid function name"),
                    arguments: vec![cast(field("raw"), ParseAsType::I64)],
                },
                ParseAsType::U8,
            )
        );
    }

    #[test]
    fn a_tolerant_conversion_composes_as_an_operand() {
        assert_eq!(
            parsed("TRY_CAST(input.raw AS I64) AS U8"),
            cast(try_cast(field("raw"), ParseAsType::I64), ParseAsType::U8)
        );
        assert_eq!(
            parsed("TRY_CAST(TRY_CAST(input.raw AS F64) AS I64)"),
            try_cast(try_cast(field("raw"), ParseAsType::F64), ParseAsType::I64)
        );
        assert_eq!(
            parsed("coalesce(TRY_CAST(input.raw AS I64), 0) + 1"),
            Expression::Binary {
                operator: BinaryOperator::Add,
                left: Box::new(Expression::Call {
                    function: BuiltinFunctionName::parse("coalesce").expect("valid function name"),
                    arguments: vec![
                        try_cast(field("raw"), ParseAsType::I64),
                        Expression::Literal(Literal::I64(0)),
                    ],
                }),
                right: Box::new(Expression::Literal(Literal::I64(1))),
            }
        );
    }

    #[test]
    fn rejects_a_tolerant_conversion_without_its_target_type() {
        for source in [
            "TRY_CAST(input.raw)",
            "TRY_CAST(input.raw, I64)",
            "TRY_CAST input.raw AS I64",
            "TRY_CAST(input.raw AS I64",
            "TRY_CAST((input.raw AS I64))",
            "TRY_CAST(AS I64)",
            "TRY_CAST()",
        ] {
            assert!(
                parse_expression(source).is_err(),
                "`{source}` must be rejected"
            );
        }
        let error = parse_expression("TRY_CAST(input.raw AS NUMBER)")
            .expect_err("an unknown target type must be rejected");
        assert!(
            error.to_string().contains("unsupported type 'NUMBER'"),
            "{error}"
        );
    }

    #[test]
    fn the_tolerant_conversion_keyword_names_a_field_or_a_call_only_after_a_scope_or_quoted() {
        assert_eq!(parsed("input.try_cast"), field("try_cast"));
        assert!(parse_expression("try_cast").is_err());
        assert!(parse_expression("try_cast(input.raw)").is_err());
        assert_eq!(parsed("`try_cast`"), bare_field("try_cast"));
        assert_eq!(
            parsed("`try_cast`(input.raw)"),
            Expression::Call {
                function: BuiltinFunctionName::parse("try_cast").expect("valid function name"),
                arguments: vec![field("raw")],
            }
        );
    }

    fn json_path(text: &str) -> JsonPath {
        JsonPath::parse(text).expect("test paths are valid")
    }

    fn json_value(document: Expression, path: &str, target: ParseAsType) -> Expression {
        Expression::JsonValue {
            document: Box::new(document),
            path: json_path(path),
            target,
        }
    }

    #[test]
    fn parses_json_extractions_of_declared_scalar_and_collection_types() {
        assert_eq!(
            parsed("JSON_VALUE(input.doc, '$.count' AS I64)"),
            json_value(field("doc"), "$.count", ParseAsType::I64)
        );
        assert_eq!(
            parsed("json_value(input.doc, '$.tags' as vec<string>)"),
            json_value(
                field("doc"),
                "$.tags",
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::String),
                },
            )
        );
        assert_eq!(
            parsed(r#"TRY_JSON_VALUE(input.doc AS STRING, '$["odd key"][0]' AS VEC<VEC<INT32>>)"#),
            Expression::TryJsonValue {
                document: Box::new(cast(field("doc"), ParseAsType::String)),
                path: json_path(r#"$["odd key"][0]"#),
                target: ParseAsType::Vec {
                    element: Box::new(ParseAsType::Vec {
                        element: Box::new(ParseAsType::I32),
                    }),
                },
            }
        );
        assert_eq!(
            parsed("JSON_VALUE(input.doc, '$.m' AS ARRAY<F32, 2, 3>)"),
            json_value(
                field("doc"),
                "$.m",
                ParseAsType::Array {
                    element: Box::new(ParseAsType::Array {
                        element: Box::new(ParseAsType::F32),
                        len: NonZeroU32::new(3).expect("three is positive"),
                    }),
                    len: NonZeroU32::new(2).expect("two is positive"),
                },
            )
        );
        assert_eq!(
            parsed("JSON_EXISTS(input.doc, '$') AND NOT Json_Exists(input.doc, '$.a')"),
            Expression::Binary {
                operator: BinaryOperator::And,
                left: Box::new(Expression::JsonExists {
                    document: Box::new(field("doc")),
                    path: json_path("$"),
                }),
                right: Box::new(Expression::Unary {
                    operator: UnaryOperator::Not,
                    expression: Box::new(Expression::JsonExists {
                        document: Box::new(field("doc")),
                        path: json_path("$.a"),
                    }),
                }),
            }
        );
        assert_eq!(
            parsed("JSON_VALUE(input.doc, '$.n' AS I64) AS STRING"),
            cast(
                json_value(field("doc"), "$.n", ParseAsType::I64),
                ParseAsType::String
            )
        );
    }

    #[test]
    fn rejects_json_extractions_without_their_path_or_declared_type() {
        for source in [
            "JSON_VALUE(input.doc)",
            "JSON_VALUE(input.doc, '$.a')",
            "JSON_VALUE(input.doc, input.path AS I64)",
            "JSON_VALUE(input.doc '$.a' AS I64)",
            "JSON_VALUE(input.doc, '$.a' AS VEC<I64)",
            "JSON_VALUE(input.doc, '$.a' AS VEC)",
            "JSON_VALUE(input.doc, '$.a' AS ARRAY<I64>)",
            "JSON_VALUE(input.doc, '$.a' AS ARRAY<I64, 0>)",
            "JSON_VALUE(input.doc, '$.a' AS ARRAY<I64, 2147483648>)",
            "JSON_VALUE(input.doc, '$.a' AS I64",
            "TRY_JSON_VALUE(input.doc, '$.a')",
            "JSON_EXISTS(input.doc)",
            "JSON_EXISTS(input.doc, '$.a' AS BOOL)",
            "JSON_EXISTS input.doc, '$.a'",
        ] {
            assert!(
                parse_expression(source).is_err(),
                "`{source}` must be rejected"
            );
        }
        let error = parse_expression("JSON_VALUE(input.doc, '$.a[-1]' AS I64)")
            .expect_err("a malformed path must be rejected");
        assert!(
            error
                .to_string()
                .contains("invalid JSON path '$.a[-1]': expected an array index"),
            "{error}"
        );
        let error = parse_expression("JSON_VALUE(input.doc, '$.a' AS ARRAY<I64, 0>)")
            .expect_err("a zero-length array must be rejected");
        assert!(
            error
                .to_string()
                .contains("array length must be a positive unsigned integer"),
            "{error}"
        );
    }

    #[test]
    fn the_json_extraction_keywords_name_fields_only_after_a_scope_or_quoted() {
        for source in ["json_exists", "try_json_value(input.doc)"] {
            assert!(
                parse_expression(source).is_err(),
                "`{source}` must be rejected"
            );
        }
        assert_eq!(parsed("input.json_value"), field("json_value"));
        assert_eq!(parsed("input.JSON_EXISTS"), field("json_exists"));
        assert_eq!(parsed("`json_exists`"), bare_field("json_exists"));
    }

    #[test]
    fn a_rendered_json_extraction_reparses_to_the_same_expression() {
        for source in [
            "JSON_VALUE(input.doc, '$.count' AS I64)",
            r#"TRY_JSON_VALUE(input.doc, $path$$["it's"]$path$ AS VEC<ARRAY<F64, 2>>)"#,
            r#"JSON_VALUE(input.doc, '$["odd key"].y[3]' AS ARRAY<U8, 2, 2>) AS STRING"#,
            "JSON_EXISTS(coalesce(input.doc, '{}'), '$.a.b')",
            "NOT JSON_EXISTS(input.doc, '$')",
        ] {
            let expression = parsed(source);
            let rendered = nervix_models::expression_to_nspl(&expression)
                .unwrap_or_else(|error| panic!("`{source}` must render: {error}"));
            assert_eq!(parsed(&rendered), expression, "`{rendered}` changed");
        }
    }

    #[test]
    fn a_rendered_tolerant_conversion_reparses_to_the_same_expression() {
        for source in [
            "TRY_CAST(input.low + input.high AS STRING)",
            "TRY_CAST(input.raw AS I64 AS STRING)",
            "TRY_CAST(input.raw AS I64) AS U8",
            "TRY_CAST(NOT input.flag AS BOOL AS I64)",
            "-TRY_CAST(input.raw AS I64) * 2",
            "TRY_CAST(CASE WHEN input.flag THEN input.raw AS I64 END AS STRING)",
        ] {
            let expression = parsed(source);
            let rendered = nervix_models::expression_to_nspl(&expression)
                .unwrap_or_else(|error| panic!("`{source}` must render: {error}"));
            assert_eq!(parsed(&rendered), expression, "`{rendered}` regrouped");
        }
    }

    /// Backslash sequences that other languages read as escapes, a lone backslash before the
    /// closing delimiter among them.
    const ESCAPE_LOOKALIKES: [&str; 11] = [
        r"a\\b",
        r"a\'b",
        r#"a\"b"#,
        r"a\nb",
        r"a\rb",
        r"a\tb",
        r"a\0b",
        r"a\x41b",
        r"a\u{e9}b",
        r"a\$b",
        r"a\",
    ];

    /// Every way to write `value` as a string literal that reads back verbatim: dollar-quoted, and
    /// in each quote style the value does not hold.
    fn verbatim_spellings(value: &str) -> Vec<String> {
        let mut spellings = vec![format!("$q${value}$q$")];
        if !value.contains('\'') {
            spellings.push(format!("'{value}'"));
        }
        if !value.contains('"') {
            spellings.push(format!("\"{value}\""));
        }
        spellings
    }

    /// Reads `predicate` as a statement reads an expression it embeds, here the `WHERE` clause of a
    /// subscription. The line break before the `;` ends a comment the predicate closes with.
    fn read_in_statement(
        predicate: &str,
    ) -> error_stack::Result<Option<Expression>, ParseFromSourceError> {
        let statement = format!("CREATE SUBSCRIPTION literal TO events WHERE {predicate}\n;");
        let parsed = crate::client_statement::parse_client_statement(&statement)?;
        let crate::client_statement::ClientStatement::CreateSubscription(subscription) = parsed
        else {
            panic!("`{statement}` must read as a subscription");
        };
        Ok(subscription.where_clause)
    }

    #[test]
    fn every_entry_point_reads_a_backslash_in_a_string_literal_verbatim() {
        for value in ESCAPE_LOOKALIKES {
            let expected = binary(BinaryOperator::Equal, field("tenant"), string(value));
            for literal in verbatim_spellings(value) {
                let predicate = format!("input.tenant = {literal}");
                let standalone = parse_expression(&predicate)
                    .unwrap_or_else(|error| panic!("`{predicate}` must parse: {error:?}"));
                assert_eq!(
                    standalone, expected,
                    "`{predicate}` read another value alone"
                );
                let embedded = read_in_statement(&predicate).unwrap_or_else(|error| {
                    panic!("`{predicate}` must parse in a statement: {error:?}")
                });
                assert_eq!(
                    embedded,
                    Some(expected.clone()),
                    "`{predicate}` read another value in a statement"
                );
            }
        }
    }

    /// Reads `predicate` alone and as a statement's `WHERE` clause, asserts that both read it the
    /// same way, as the same expression or not at all, and returns that reading.
    fn read_at_every_entry_point(predicate: &str) -> Option<Expression> {
        let alone = parse_expression(predicate).ok();
        let embedded = read_in_statement(predicate).ok().flatten();
        assert_eq!(
            alone, embedded,
            "`{predicate}` read differently alone and in a statement"
        );
        alone
    }

    #[test]
    fn whitespace_and_comments_between_tokens_read_alike_at_every_entry_point() {
        let expected = binary(BinaryOperator::Equal, field("a"), integer(1));
        for predicate in [
            "input.a = 1",
            "input.a=1",
            "input . a = 1",
            "input.a // a comment ends at its line\n = 1",
            "input.a\n\t=\r\n1 // a comment may end the text",
        ] {
            assert_eq!(
                read_at_every_entry_point(predicate),
                Some(expected.clone()),
                "{predicate:?}"
            );
        }
    }

    #[test]
    fn keywords_of_expression_forms_read_alike_at_every_entry_point() {
        assert_eq!(
            read_at_every_entry_point(
                "input.a is not distinct from 1 OR input.b IS DISTINCT FROM 2"
            ),
            Some(binary(
                BinaryOperator::Or,
                binary(BinaryOperator::IsNotDistinctFrom, field("a"), integer(1)),
                binary(BinaryOperator::IsDistinctFrom, field("b"), integer(2)),
            ))
        );
        for predicate in [
            "input.a BETWEEN 1 AND 2 AND input.b not between -1 and 1",
            "CASE input.a WHEN 1 THEN 'one' ELSE 'other' END = 'one'",
            "case when input.a then 1 end > 0",
            "if input.flag then 1 else 2 end > 0",
            "TRY_CAST(input.raw AS I64) IN (1, 2) AND input.c NOT IN ()",
            "JSON_EXISTS(input.doc, '$.a') AND json_value(input.doc, '$.n' AS I64) > \
             Try_Json_Value(input.doc, '$.m' AS VEC<I64>) IS NOT DISTINCT FROM NULL",
            "udf::mask(input.a) = input.b",
        ] {
            assert!(
                read_at_every_entry_point(predicate).is_some(),
                "`{predicate}` must parse"
            );
        }
    }

    #[test]
    fn statement_keywords_name_calls_scopes_and_fields_at_every_entry_point() {
        let call = |function: &str, arguments: Vec<Expression>| Expression::Call {
            function: BuiltinFunctionName::parse(function).expect("test functions are valid names"),
            arguments,
        };
        assert_eq!(
            read_at_every_entry_point("first(input.sum) > min(input.last, input.max)"),
            Some(binary(
                BinaryOperator::GreaterThan,
                call("first", vec![field("sum")]),
                call("min", vec![field("last"), field("max")]),
            ))
        );
        for predicate in [
            "output.total > 0",
            "left.status = right.key",
            "message.time = branch.timestamp",
            "status = 1",
        ] {
            assert!(
                read_at_every_entry_point(predicate).is_some(),
                "`{predicate}` must parse"
            );
        }
    }

    #[test]
    fn reserved_expression_keywords_name_fields_only_after_a_scope_or_quoted_at_every_entry_point()
    {
        let reserved = [
            "where",
            "set",
            "inherit",
            "all",
            "except",
            "leak",
            "sensitive",
            "invoke",
            "as",
            "try_cast",
            "json_value",
            "try_json_value",
            "json_exists",
            "and",
            "or",
            "not",
            "true",
            "false",
            "null",
            "if",
            "case",
            "when",
            "then",
            "else",
            "end",
            "in",
            "between",
            "is",
            "distinct",
            "from",
            "udf",
        ];
        for keyword in reserved {
            let scoped = format!("input.{keyword} = 1");
            assert_eq!(
                read_at_every_entry_point(&scoped),
                Some(binary(BinaryOperator::Equal, field(keyword), integer(1))),
                "`{scoped}` names a field after the scope"
            );
            let quoted = format!("`{keyword}` = 1");
            assert_eq!(
                read_at_every_entry_point(&quoted),
                Some(binary(
                    BinaryOperator::Equal,
                    bare_field(keyword),
                    integer(1)
                )),
                "`{quoted}` names a bare field"
            );
            // A literal keyword is a value of its own rather than a name.
            if !matches!(keyword, "true" | "false" | "null") {
                let bare = format!("{keyword} = 1");
                assert_eq!(
                    read_at_every_entry_point(&bare),
                    None,
                    "`{bare}` must be rejected"
                );
            }
        }
    }

    #[test]
    fn number_literals_read_alike_at_every_entry_point() {
        assert_eq!(
            read_at_every_entry_point("input.x = 1.5"),
            Some(binary(
                BinaryOperator::Equal,
                field("x"),
                Expression::Literal(Literal::F64(Float64Literal::new(1.5))),
            ))
        );
        assert_eq!(
            read_at_every_entry_point("input.x = 9223372036854775807"),
            Some(binary(BinaryOperator::Equal, field("x"), integer(i64::MAX)))
        );
        assert_eq!(
            read_at_every_entry_point("input.x = 9223372036854775808"),
            None
        );
        for predicate in ["input.x = 1e5", "input.x = 1.5E+3", "input.x = 2e-1"] {
            assert_eq!(read_at_every_entry_point(predicate), None, "{predicate}");
            let error = parse_expression(predicate).expect_err("an exponent is rejected");
            assert!(error.to_string().contains("has an exponent"), "{error}");
        }
    }

    #[test]
    fn a_minus_reads_alike_at_every_entry_point() {
        let negated = |expression: Expression| Expression::Unary {
            operator: UnaryOperator::Negate,
            expression: Box::new(expression),
        };
        assert_eq!(
            read_at_every_entry_point("input.a-1"),
            Some(binary(BinaryOperator::Subtract, field("a"), integer(1)))
        );
        assert_eq!(
            read_at_every_entry_point("-input.a - -1"),
            Some(binary(
                BinaryOperator::Subtract,
                negated(field("a")),
                negated(integer(1)),
            ))
        );
    }

    #[test]
    fn punctuation_no_expression_reads_is_rejected_at_every_entry_point() {
        for predicate in [
            "input.a = {1}",
            "input.a : 1",
            "input.a = 1 }",
            "input.a :: b",
        ] {
            assert_eq!(
                read_at_every_entry_point(predicate),
                None,
                "`{predicate}` must be rejected"
            );
        }
    }

    #[test]
    fn a_number_split_at_its_dot_is_rejected_at_every_entry_point() {
        for predicate in ["input.x = 1 .5", "input.x = 1. 5", "input.x = 1 . 5"] {
            assert!(
                parse_expression(predicate).is_err(),
                "`{predicate}` must be rejected alone"
            );
            assert!(
                read_in_statement(predicate).is_err(),
                "`{predicate}` must be rejected in a statement"
            );
        }
    }

    #[test]
    fn a_backslash_leaves_the_closing_quote_closing_at_every_entry_point() {
        for predicate in [r"input.tenant = 'a\'b'", r#"input.tenant = "a\"b""#] {
            assert!(
                parse_expression(predicate).is_err(),
                "`{predicate}` must be rejected alone"
            );
            assert!(
                read_in_statement(predicate).is_err(),
                "`{predicate}` must be rejected in a statement"
            );
        }
    }
}
