use chumsky::prelude::*;
use nervix_models::{
    AlterSchema, AlterSchemaOperation, AlterWireSchema, AlterWireSchemaOperation, AvroType,
    CreateAvroWireSchema, CreateCborWireSchema, CreateJsonWireSchema, CreateSchema,
    CreateStatement, CreateWireSchema, JsonType, Model, ParseAsType, SchemaField, Statement,
    WireSchemaField, WireSchemaStrictness,
};

pub use crate::parser_support::{Diagnostic, ParseFromSourceError};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, boxed_choice, field_ref, if_not_exists_clause, into_parse_error, kw,
        kw_phrase2, lex_input, schema_name, suggest_from, tok, wire_schema_name,
    },
};

fn json_type<'src>()
-> impl Parser<'src, &'src [Token], JsonType, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        kw(Identifier::String).to(JsonType::String),
        kw(Identifier::Number).to(JsonType::Number),
        kw(Identifier::Integer).to(JsonType::Integer),
        kw(Identifier::Object).to(JsonType::Object),
        kw(Identifier::Array).to(JsonType::Array),
        kw(Identifier::Boolean).to(JsonType::Boolean),
        kw(Identifier::Null).to(JsonType::Null),
    )
}

fn cbor_type<'src>()
-> impl Parser<'src, &'src [Token], JsonType, extra::Err<ParseError<'src>>> + Clone {
    json_type()
}

fn avro_type<'src>()
-> impl Parser<'src, &'src [Token], AvroType, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        kw(Identifier::Null).to(AvroType::Null),
        kw(Identifier::Boolean).to(AvroType::Boolean),
        kw(Identifier::Int).to(AvroType::Int),
        kw(Identifier::Long).to(AvroType::Long),
        kw(Identifier::Float).to(AvroType::Float),
        kw(Identifier::Double).to(AvroType::Double),
        kw(Identifier::Bytes).to(AvroType::Bytes),
        kw(Identifier::String).to(AvroType::String),
        kw(Identifier::Record).to(AvroType::Record),
        kw(Identifier::Enum).to(AvroType::Enum),
        kw(Identifier::Array).to(AvroType::Array),
        kw(Identifier::Map).to(AvroType::Map),
        kw(Identifier::Fixed).to(AvroType::Fixed),
    )
}

pub(crate) fn nervix_type<'src>()
-> impl Parser<'src, &'src [Token], ParseAsType, extra::Err<ParseError<'src>>> + Clone {
    recursive(|ty| {
        let scalar = boxed_choice!(
            kw(Identifier::U8).to(ParseAsType::U8),
            kw(Identifier::I8).to(ParseAsType::I8),
            kw(Identifier::U16).to(ParseAsType::U16),
            kw(Identifier::I16).to(ParseAsType::I16),
            kw(Identifier::U32).to(ParseAsType::U32),
            kw(Identifier::I32).to(ParseAsType::I32),
            kw(Identifier::U64).to(ParseAsType::U64),
            kw(Identifier::I64).to(ParseAsType::I64),
            kw(Identifier::Bool).to(ParseAsType::Bool),
            kw(Identifier::String).to(ParseAsType::String),
            kw(Identifier::Datetime).to(ParseAsType::Datetime),
            kw(Identifier::F32).to(ParseAsType::F32),
            kw(Identifier::F64).to(ParseAsType::F64),
        );

        let array_len = select! { Token::NumberLiteral(raw) => raw }
            .try_map(|raw, span| {
                let len = raw
                    .parse::<u32>()
                    .map_err(|_| Rich::custom(span, "array length must be an unsigned integer"))?;
                if len == 0 {
                    return Err(Rich::custom(span, "array length must be greater than zero"));
                }
                Ok(len)
            })
            .labelled("array_length");

        let array = kw(Identifier::Array).ignore_then(
            ty.clone()
                .then_ignore(tok(Token::Comma))
                .then(array_len)
                .then(
                    tok(Token::Comma)
                        .ignore_then(array_len)
                        .repeated()
                        .collect::<Vec<_>>(),
                )
                .map(|((element, first_len), remaining_lengths)| {
                    let mut lengths = Vec::with_capacity(remaining_lengths.len() + 1);
                    lengths.push(first_len);
                    lengths.extend(remaining_lengths);
                    lengths
                        .into_iter()
                        .rev()
                        .fold(element, |element, len| ParseAsType::Array {
                            element: Box::new(element),
                            len,
                        })
                })
                .delimited_by(tok(Token::Lt), tok(Token::Gt)),
        );

        let vec_ty = kw(Identifier::Vec).ignore_then(
            ty.clone()
                .map(|element| ParseAsType::Vec {
                    element: Box::new(element),
                })
                .delimited_by(tok(Token::Lt), tok(Token::Gt)),
        );

        choice((array, vec_ty, scalar)).boxed()
    })
}

fn wire_schema_field<'src, T, P>(
    native_type: P,
) -> impl Parser<'src, &'src [Token], WireSchemaField<T>, extra::Err<ParseError<'src>>> + Clone
where
    T: Clone + 'src,
    P: Parser<'src, &'src [Token], T, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    field_ref()
        .then(native_type)
        .then(kw(Identifier::Optional).or_not())
        .map(|((name, ty), optional)| WireSchemaField {
            name,
            ty,
            optional: optional.is_some(),
        })
}

#[derive(Default)]
struct InternalSchemaFieldModifiers {
    optional: bool,
    sensitive: bool,
}

/// Each modifier may appear at most once, in either order.
///
/// This is expressed in the grammar rather than validated after a `repeated()`, because completion
/// is derived from what the grammar still expects: a `repeated()` keeps offering a modifier that is
/// already present, and accepting it only to reject the statement afterwards.
fn internal_schema_field_modifiers<'src>()
-> impl Parser<'src, &'src [Token], InternalSchemaFieldModifiers, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        kw(Identifier::Optional)
            .ignore_then(kw(Identifier::Sensitive).or_not())
            .map(|sensitive| InternalSchemaFieldModifiers {
                optional: true,
                sensitive: sensitive.is_some(),
            }),
        kw(Identifier::Sensitive)
            .ignore_then(kw(Identifier::Optional).or_not())
            .map(|optional| InternalSchemaFieldModifiers {
                optional: optional.is_some(),
                sensitive: true,
            }),
    ))
    .or_not()
    .map(Option::unwrap_or_default)
}

fn internal_schema_field<'src>()
-> impl Parser<'src, &'src [Token], SchemaField, extra::Err<ParseError<'src>>> + Clone {
    field_ref()
        .then(nervix_type())
        .then(internal_schema_field_modifiers())
        .map(|((name, ty), modifiers)| SchemaField {
            name,
            ty,
            optional: modifiers.optional,
            sensitive: modifiers.sensitive,
        })
}

#[derive(Clone)]
enum InternalFieldAlter {
    Type(ParseAsType),
    Optional(bool),
    Sensitive(bool),
}

fn alter_schema_operation<'src>()
-> impl Parser<'src, &'src [Token], AlterSchemaOperation, extra::Err<ParseError<'src>>> + Clone {
    let add = kw_phrase2(Identifier::Add, Identifier::Field)
        .ignore_then(internal_schema_field())
        .map(|field| AlterSchemaOperation::AddField { field });
    let drop = kw_phrase2(Identifier::Drop, Identifier::Field)
        .ignore_then(field_ref())
        .map(|field| AlterSchemaOperation::DropField { field });
    let rename = kw_phrase2(Identifier::Rename, Identifier::Field)
        .ignore_then(field_ref())
        .then_ignore(kw(Identifier::To))
        .then(field_ref())
        .map(|(field, to)| AlterSchemaOperation::RenameField { field, to });
    let field_alter = boxed_choice!(
        kw_phrase2(Identifier::Set, Identifier::Type)
            .ignore_then(nervix_type())
            .map(InternalFieldAlter::Type),
        kw_phrase2(Identifier::Set, Identifier::Optional).to(InternalFieldAlter::Optional(true)),
        kw_phrase2(Identifier::Drop, Identifier::Optional).to(InternalFieldAlter::Optional(false)),
        kw_phrase2(Identifier::Set, Identifier::Sensitive).to(InternalFieldAlter::Sensitive(true)),
        kw_phrase2(Identifier::Drop, Identifier::Sensitive)
            .to(InternalFieldAlter::Sensitive(false)),
    );
    let alter = kw_phrase2(Identifier::Alter, Identifier::Field)
        .ignore_then(field_ref())
        .then(field_alter)
        .map(|(field, alter)| match alter {
            InternalFieldAlter::Type(ty) => AlterSchemaOperation::SetFieldType { field, ty },
            InternalFieldAlter::Optional(optional) => {
                AlterSchemaOperation::SetFieldOptional { field, optional }
            }
            InternalFieldAlter::Sensitive(sensitive) => {
                AlterSchemaOperation::SetFieldSensitive { field, sensitive }
            }
        });

    boxed_choice!(add, drop, rename, alter)
}

pub fn alter_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterSchema, extra::Err<ParseError<'src>>> + Clone {
    let operations = alter_schema_operation()
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .at_least(1)
        .collect::<Vec<_>>();

    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(schema_name())
        .then(operations)
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(schema, operations)| AlterSchema { schema, operations })
        .boxed()
}

#[derive(Clone)]
enum WireFieldAlter<T> {
    Type(T),
    Optional(bool),
}

fn alter_wire_schema_operation<'src, T, P>(
    native_type: P,
) -> impl Parser<'src, &'src [Token], AlterWireSchemaOperation<T>, extra::Err<ParseError<'src>>> + Clone
where
    T: Clone + 'src,
    P: Parser<'src, &'src [Token], T, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let mode = kw(Identifier::Mode)
        .ignore_then(wire_schema_mode())
        .map(|mode| AlterWireSchemaOperation::SetMode { mode });
    let add = kw_phrase2(Identifier::Add, Identifier::Field)
        .ignore_then(wire_schema_field(native_type.clone()))
        .map(|field| AlterWireSchemaOperation::AddField { field });
    let drop = kw_phrase2(Identifier::Drop, Identifier::Field)
        .ignore_then(field_ref())
        .map(|field| AlterWireSchemaOperation::DropField { field });
    let rename = kw_phrase2(Identifier::Rename, Identifier::Field)
        .ignore_then(field_ref())
        .then_ignore(kw(Identifier::To))
        .then(field_ref())
        .map(|(field, to)| AlterWireSchemaOperation::RenameField { field, to });
    let field_alter = boxed_choice!(
        kw_phrase2(Identifier::Set, Identifier::Type)
            .ignore_then(native_type)
            .map(WireFieldAlter::Type),
        kw_phrase2(Identifier::Set, Identifier::Optional).to(WireFieldAlter::Optional(true)),
        kw_phrase2(Identifier::Drop, Identifier::Optional).to(WireFieldAlter::Optional(false)),
    );
    let alter = kw_phrase2(Identifier::Alter, Identifier::Field)
        .ignore_then(field_ref())
        .then(field_alter)
        .map(|(field, alter)| match alter {
            WireFieldAlter::Type(ty) => AlterWireSchemaOperation::SetFieldType { field, ty },
            WireFieldAlter::Optional(optional) => {
                AlterWireSchemaOperation::SetFieldOptional { field, optional }
            }
        });
    boxed_choice!(mode, add, drop, rename, alter)
}

fn alter_wire_schema_parser<'src, T, P>(
    format_kw: Identifier,
    native_type: P,
) -> impl Parser<'src, &'src [Token], AlterWireSchema<T>, extra::Err<ParseError<'src>>> + Clone
where
    T: Clone + 'src,
    P: Parser<'src, &'src [Token], T, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let operations = alter_wire_schema_operation(native_type)
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .at_least(1)
        .collect::<Vec<_>>();

    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Wire))
        .ignore_then(kw(format_kw))
        .ignore_then(kw(Identifier::Schema))
        .ignore_then(wire_schema_name())
        .then(operations)
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(schema, operations)| AlterWireSchema { schema, operations })
        .boxed()
}

pub fn alter_wire_schema_parser_any<'src>()
-> impl Parser<'src, &'src [Token], Statement, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        alter_wire_schema_parser(Identifier::Json, json_type()).map(Statement::AlterWireJsonSchema),
        alter_wire_schema_parser(Identifier::Cbor, cbor_type()).map(Statement::AlterWireCborSchema),
        alter_wire_schema_parser(Identifier::Avro, avro_type()).map(Statement::AlterWireAvroSchema),
    )
}

fn wire_schema_mode<'src>()
-> impl Parser<'src, &'src [Token], WireSchemaStrictness, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        kw(Identifier::Strict).to(WireSchemaStrictness::Strict),
        kw(Identifier::Loose).to(WireSchemaStrictness::Loose),
    )
}

fn create_wire_schema_parser<'src, T, P>(
    format_kw: Identifier,
    native_type: P,
) -> impl Parser<'src, &'src [Token], CreateStatement<CreateWireSchema<T>>, extra::Err<ParseError<'src>>>
+ Clone
where
    T: Clone + 'src,
    P: Parser<'src, &'src [Token], T, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let fields = wire_schema_field(native_type)
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .at_least(1)
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LParen), tok(Token::RParen));

    let wire_schema_header = kw(Identifier::Wire)
        .then_ignore(kw(format_kw))
        .then_ignore(kw(Identifier::Schema));

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(wire_schema_header)
        .then(wire_schema_name())
        .then_ignore(kw(Identifier::Mode))
        .then(wire_schema_mode())
        .boxed()
        .then(fields)
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(((if_not_exists, name), strictness), fields)| {
            CreateStatement::new(
                CreateWireSchema {
                    name,
                    strictness,
                    fields,
                },
                if_not_exists,
            )
        })
        .boxed()
}

pub fn create_json_wire_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateJsonWireSchema>, extra::Err<ParseError<'src>>>
+ Clone {
    create_wire_schema_parser(Identifier::Json, json_type())
}

pub fn create_cbor_wire_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateCborWireSchema>, extra::Err<ParseError<'src>>>
+ Clone {
    create_wire_schema_parser(Identifier::Cbor, cbor_type())
}

pub fn create_avro_wire_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateAvroWireSchema>, extra::Err<ParseError<'src>>>
+ Clone {
    create_wire_schema_parser(Identifier::Avro, avro_type())
}

pub fn create_wire_schema_parser_any<'src>()
-> impl Parser<'src, &'src [Token], Statement, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        create_json_wire_schema_parser().map(|create| {
            Statement::Create(create.map_body(Model::WireJsonSchema).map_body(Box::new))
        }),
        create_cbor_wire_schema_parser().map(|create| {
            Statement::Create(create.map_body(Model::WireCborSchema).map_body(Box::new))
        }),
        create_avro_wire_schema_parser().map(|create| {
            Statement::Create(create.map_body(Model::WireAvroSchema).map_body(Box::new))
        }),
    )
}

pub fn create_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateSchema>, extra::Err<ParseError<'src>>> + Clone
{
    let fields = internal_schema_field()
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .at_least(1)
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LParen), tok(Token::RParen));

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Schema))
        .then(schema_name())
        .boxed()
        .then(fields)
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|((if_not_exists, name), fields)| {
            CreateStatement::new(CreateSchema { name, fields }, if_not_exists)
        })
        .boxed()
}

pub fn parse_create_wire_schema_tokens(tokens: &[Token]) -> Result<Statement, Vec<ParseError<'_>>> {
    let out = create_wire_schema_parser_any()
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

pub fn parse_alter_schema_tokens(tokens: &[Token]) -> Result<AlterSchema, Vec<ParseError<'_>>> {
    let out = alter_schema_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_alter_wire_schema_tokens(tokens: &[Token]) -> Result<Statement, Vec<ParseError<'_>>> {
    let out = alter_wire_schema_parser_any()
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

pub fn parse_create_schema_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateSchema>, Vec<ParseError<'_>>> {
    let out = create_schema_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_wire_schema(input: &str) -> Result<Statement, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_wire_schema_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_create_schema(
    input: &str,
) -> Result<CreateStatement<CreateSchema>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_schema_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_alter_schema(input: &str) -> Result<AlterSchema, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_alter_schema_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_alter_wire_schema(input: &str) -> Result<Statement, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_alter_wire_schema_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_wire_schema(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_wire_schema_parser_any())
}

pub fn suggest_create_schema(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_schema_parser())
}

pub fn suggest_alter_schema(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, alter_schema_parser())
}

pub fn suggest_alter_wire_schema(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, alter_wire_schema_parser_any())
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
    fn parses_internal_schema_definition() {
        let input = r#"
            CREATE SCHEMA notification (
                user_id U32,
                created_at DATETIME,
                payload STRING
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "notification");
        assert_eq!(parsed.fields.len(), 3);
        assert_eq!(parsed.fields[0].ty, ParseAsType::U32);
    }

    #[test]
    fn parses_internal_schema_definition_with_if_not_exists() {
        let input = r#"
            CREATE IF NOT EXISTS SCHEMA notification (
                user_id U32
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert!(parsed.if_not_exists);
        assert_eq!(parsed.name.as_str(), "notification");
        assert_eq!(parsed.fields.len(), 1);
    }

    #[test]
    fn parses_optional_internal_schema_fields() {
        let input = r#"
            CREATE SCHEMA notification (
                user_id U32,
                nickname STRING OPTIONAL
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert!(!parsed.fields[0].optional);
        assert!(parsed.fields[1].optional);
    }

    #[test]
    fn parses_sensitive_internal_schema_fields() {
        let input = r#"
            CREATE SCHEMA notification (
                user_id U32,
                secret STRING SENSITIVE,
                token STRING OPTIONAL SENSITIVE
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert!(!parsed.fields[0].sensitive);
        assert!(parsed.fields[1].sensitive);
        assert!(!parsed.fields[1].optional);
        assert!(parsed.fields[2].sensitive);
        assert!(parsed.fields[2].optional);
    }

    #[test]
    fn rejects_duplicate_internal_schema_field_modifiers() {
        let input = "CREATE SCHEMA notification ( secret STRING SENSITIVE SENSITIVE );";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn parses_internal_schema_array_and_vector_fields() {
        let input = r#"
            CREATE SCHEMA metrics (
                cpu_last_64 ARRAY<F32, 64>,
                labels VEC<STRING> OPTIONAL
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.fields[0].ty,
            ParseAsType::Array {
                element: Box::new(ParseAsType::F32),
                len: 64
            }
        );
        assert_eq!(
            parsed.fields[1].ty,
            ParseAsType::Vec {
                element: Box::new(ParseAsType::String)
            }
        );
        assert!(parsed.fields[1].optional);
    }

    #[test]
    fn parses_multidimensional_and_mixed_internal_array_shapes() {
        let input = r#"
            CREATE SCHEMA tensors (
                matrix ARRAY<F32, 2, 3>,
                samples VEC<ARRAY<F32, 4>>,
                buckets ARRAY<VEC<I64>, 2>
            );
        "#;

        let parsed = parse_create_schema_tokens(&to_tokens(input)).expect("parse should succeed");
        assert_eq!(
            parsed.fields[0].ty,
            ParseAsType::Array {
                len: 2,
                element: Box::new(ParseAsType::Array {
                    len: 3,
                    element: Box::new(ParseAsType::F32),
                }),
            }
        );
        assert_eq!(
            parsed.fields[1].ty,
            ParseAsType::Vec {
                element: Box::new(ParseAsType::Array {
                    len: 4,
                    element: Box::new(ParseAsType::F32),
                }),
            }
        );
        assert_eq!(
            parsed.fields[2].ty,
            ParseAsType::Array {
                len: 2,
                element: Box::new(ParseAsType::Vec {
                    element: Box::new(ParseAsType::I64),
                }),
            }
        );
    }

    #[test]
    fn parses_array_and_vector_elements_for_all_internal_primitive_types() {
        let input = r#"
            CREATE SCHEMA metrics (
                u8_array ARRAY<U8, 2>,
                u8_vec VEC<U8>,
                i8_array ARRAY<I8, 2>,
                i8_vec VEC<I8>,
                u16_array ARRAY<U16, 2>,
                u16_vec VEC<U16>,
                i16_array ARRAY<I16, 2>,
                i16_vec VEC<I16>,
                u32_array ARRAY<U32, 2>,
                u32_vec VEC<U32>,
                i32_array ARRAY<I32, 2>,
                i32_vec VEC<I32>,
                u64_array ARRAY<U64, 2>,
                u64_vec VEC<U64>,
                i64_array ARRAY<I64, 2>,
                i64_vec VEC<I64>,
                bool_array ARRAY<BOOL, 2>,
                bool_vec VEC<BOOL>,
                string_array ARRAY<STRING, 2>,
                string_vec VEC<STRING>,
                datetime_array ARRAY<DATETIME, 2>,
                datetime_vec VEC<DATETIME>,
                f32_array ARRAY<F32, 2>,
                f32_vec VEC<F32>,
                f64_array ARRAY<F64, 2>,
                f64_vec VEC<F64>
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        let element_types = [
            ParseAsType::U8,
            ParseAsType::I8,
            ParseAsType::U16,
            ParseAsType::I16,
            ParseAsType::U32,
            ParseAsType::I32,
            ParseAsType::U64,
            ParseAsType::I64,
            ParseAsType::Bool,
            ParseAsType::String,
            ParseAsType::Datetime,
            ParseAsType::F32,
            ParseAsType::F64,
        ];

        for (index, element) in element_types.into_iter().enumerate() {
            assert_eq!(
                parsed.fields[index * 2].ty,
                ParseAsType::Array {
                    element: Box::new(element.clone()),
                    len: 2
                }
            );
            assert_eq!(
                parsed.fields[index * 2 + 1].ty,
                ParseAsType::Vec {
                    element: Box::new(element)
                }
            );
        }
    }

    #[test]
    fn rejects_internal_schema_zero_length_array() {
        let input = "CREATE SCHEMA metrics (cpu ARRAY<F32, 0>);";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn rejects_zero_length_in_any_multidimensional_array_axis() {
        let input = "CREATE SCHEMA metrics (cpu ARRAY<F32, 2, 0>);";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn rejects_empty_internal_schema_definition() {
        let input = "CREATE SCHEMA root_branch ();";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn parses_json_wire_schema_definition() {
        let input = r#"
            CREATE WIRE JSON SCHEMA notification MODE STRICT (
                user_id integer,
                created_at string,
                payload object
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_wire_schema_tokens(&tokens).expect("parse should succeed");
        let Statement::Create(create) = parsed else {
            panic!("expected create statement");
        };
        let Model::WireJsonSchema(schema) = *create.body else {
            panic!("expected JSON wire schema");
        };
        assert_eq!(schema.name.as_str(), "notification");
        assert_eq!(schema.strictness, WireSchemaStrictness::Strict);
        assert_eq!(schema.fields[0].ty, JsonType::Integer);
    }

    #[test]
    fn parses_strict_json_wire_schema_definition() {
        let input = r#"
            CREATE WIRE JSON SCHEMA notification MODE STRICT (
                user_id integer,
                payload object
            );
        "#;

        let parsed = parse_create_wire_schema(input).expect("parse should succeed");
        let Statement::Create(create) = parsed else {
            panic!("expected create statement");
        };
        let Model::WireJsonSchema(schema) = *create.body else {
            panic!("expected JSON wire schema");
        };
        assert_eq!(schema.strictness, WireSchemaStrictness::Strict);
        assert_eq!(schema.fields[1].ty, JsonType::Object);
    }

    #[test]
    fn parses_loose_cbor_wire_schema_definition() {
        let input = r#"
            CREATE WIRE CBOR SCHEMA notification MODE LOOSE (
                user_id integer,
                payload object OPTIONAL
            );
        "#;

        let parsed = parse_create_wire_schema(input).expect("parse should succeed");
        let Statement::Create(create) = parsed else {
            panic!("expected create statement");
        };
        let Model::WireCborSchema(schema) = *create.body else {
            panic!("expected CBOR wire schema");
        };
        assert_eq!(schema.strictness, WireSchemaStrictness::Loose);
        assert_eq!(schema.fields[0].ty, JsonType::Integer);
        assert!(schema.fields[1].optional);
    }

    #[test]
    fn rejects_internal_types_in_json_wire_schema_definition() {
        let input = "CREATE WIRE JSON SCHEMA notification MODE STRICT ( user_id U32 );";

        assert!(parse_create_wire_schema(input).is_err());
    }

    #[test]
    fn rejects_strictness_before_legacy_wire_schema_order() {
        let input = "CREATE STRICT WIRE JSON SCHEMA notification ( user_id integer );";

        assert!(parse_create_wire_schema(input).is_err());
    }

    #[test]
    fn rejects_legacy_wire_schema_order_without_strictness() {
        let input = "CREATE JSON WIRE SCHEMA notification ( user_id integer );";

        assert!(parse_create_wire_schema(input).is_err());
    }

    #[test]
    fn rejects_empty_wire_schema_definition() {
        let input = "CREATE WIRE JSON SCHEMA notification MODE STRICT ();";
        assert!(parse_create_wire_schema(input).is_err());
    }

    #[test]
    fn parses_avro_wire_schema_definition() {
        let input = r#"
            CREATE WIRE AVRO SCHEMA latency_report MODE STRICT (
                user_id long,
                created_at string,
                payload bytes
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_wire_schema_tokens(&tokens).expect("parse should succeed");
        let Statement::Create(create) = parsed else {
            panic!("expected create statement");
        };
        let Model::WireAvroSchema(schema) = *create.body else {
            panic!("expected AVRO wire schema");
        };
        assert_eq!(schema.fields[0].ty, AvroType::Long);
    }

    #[test]
    fn parses_optional_wire_schema_fields() {
        let input = r#"
            CREATE WIRE JSON SCHEMA notification MODE STRICT (
                user_id integer,
                nickname string OPTIONAL
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_wire_schema_tokens(&tokens).expect("parse should succeed");
        let Statement::Create(create) = parsed else {
            panic!("expected create statement");
        };
        let Model::WireJsonSchema(schema) = *create.body else {
            panic!("expected JSON wire schema");
        };
        assert!(!schema.fields[0].optional);
        assert!(schema.fields[1].optional);
    }

    #[test]
    fn rejects_invalid_if_exists_clause_for_schema() {
        let input = "CREATE IF EXISTS SCHEMA notification ( user_id U32 );";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn suggests_types_from_internal_schema_grammar() {
        let input = "CREATE SCHEMA s (id ";
        let suggestions = suggest_create_schema(input, input.len());
        assert!(suggestions.contains(&"U32".to_string()));
        assert!(suggestions.contains(&"STRING".to_string()));
        assert!(suggestions.contains(&"ARRAY".to_string()));
        assert!(suggestions.contains(&"VEC".to_string()));
    }

    #[test]
    fn suggests_internal_element_types_inside_array_without_wire_type_leakage() {
        let input = "CREATE SCHEMA s (matrix ARRAY<";
        let suggestions = suggest_create_schema(input, input.len());
        assert!(suggestions.contains(&"F32".to_string()));
        assert!(suggestions.contains(&"ARRAY".to_string()));
        assert!(suggestions.contains(&"VEC".to_string()));
        assert!(!suggestions.contains(&"NUMBER".to_string()));
        assert!(!suggestions.contains(&"OBJECT".to_string()));
    }

    #[test]
    fn suggests_optional_after_internal_field_type_without_cross_leakage() {
        let input = "CREATE SCHEMA s (id U32 ";
        let suggestions = suggest_create_schema(input, input.len());
        assert!(suggestions.contains(&"OPTIONAL".to_string()));
        assert!(suggestions.contains(&"SENSITIVE".to_string()));
        assert!(!suggestions.contains(&"NUMBER".to_string()));
    }

    #[test]
    fn suggests_sensitive_after_internal_optional_modifier_without_cross_leakage() {
        let input = "CREATE SCHEMA s (id U32 OPTIONAL ";
        let suggestions = suggest_create_schema(input, input.len());
        assert!(suggestions.contains(&"SENSITIVE".to_string()));
        assert!(!suggestions.contains(&"NUMBER".to_string()));
    }

    #[test]
    fn suggests_types_from_json_wire_schema_grammar() {
        let input = "CREATE WIRE JSON SCHEMA s MODE STRICT (id ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"STRING".to_string()));
        assert!(suggestions.contains(&"NUMBER".to_string()));
        assert!(!suggestions.contains(&"U32".to_string()));
        assert!(!suggestions.contains(&"DATETIME".to_string()));
    }

    #[test]
    fn suggests_wire_after_create_without_mode_leakage() {
        let input = "CREATE ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"WIRE".to_string()));
        assert!(!suggestions.contains(&"STRICT".to_string()));
        assert!(!suggestions.contains(&"LOOSE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"CBOR".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
        assert!(!suggestions.contains(&"U32".to_string()));
    }

    #[test]
    fn suggests_wire_formats_after_wire() {
        let input = "CREATE WIRE ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"JSON".to_string()));
        assert!(suggestions.contains(&"CBOR".to_string()));
        assert!(suggestions.contains(&"AVRO".to_string()));
        assert!(!suggestions.contains(&"U32".to_string()));
    }

    #[test]
    fn suggests_mode_then_strictness_after_wire_schema_name() {
        let input = "CREATE WIRE JSON SCHEMA payload ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert_eq!(suggestions, vec!["MODE"]);

        let input = "CREATE WIRE JSON SCHEMA payload MODE ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"STRICT".to_string()));
        assert!(suggestions.contains(&"LOOSE".to_string()));
        assert!(!suggestions.contains(&"STRING".to_string()));
    }

    #[test]
    fn suggests_types_from_cbor_wire_schema_grammar_without_internal_leakage() {
        let input = "CREATE WIRE CBOR SCHEMA s MODE LOOSE (id ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"STRING".to_string()));
        assert!(suggestions.contains(&"INTEGER".to_string()));
        assert!(suggestions.contains(&"OBJECT".to_string()));
        assert!(!suggestions.contains(&"U32".to_string()));
        assert!(!suggestions.contains(&"DATETIME".to_string()));
    }

    #[test]
    fn suggests_optional_after_wire_field_type_without_cross_leakage() {
        let input = "CREATE WIRE JSON SCHEMA s MODE STRICT (id STRING ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"OPTIONAL".to_string()));
        assert!(!suggestions.contains(&"DATETIME".to_string()));
    }

    #[test]
    fn parses_all_internal_schema_alter_operations() {
        let parsed = parse_alter_schema(
            "ALTER SCHEMA user_events ADD FIELD note STRING OPTIONAL SENSITIVE, DROP FIELD \
             legacy_id, RENAME FIELD ts TO event_time, ALTER FIELD amount SET TYPE F64, ALTER \
             FIELD email SET OPTIONAL, ALTER FIELD phone DROP OPTIONAL, ALTER FIELD token SET \
             SENSITIVE, ALTER FIELD public_id DROP SENSITIVE;",
        )
        .expect("parse should succeed");

        assert_eq!(parsed.schema.as_str(), "user_events");
        assert_eq!(parsed.operations.len(), 8);
        assert!(matches!(
            &parsed.operations[0],
            AlterSchemaOperation::AddField { field }
                if field.name.as_str() == "note"
                    && field.ty == ParseAsType::String
                    && field.optional
                    && field.sensitive
        ));
        assert!(matches!(
            &parsed.operations[3],
            AlterSchemaOperation::SetFieldType { field, ty }
                if field.as_str() == "amount" && *ty == ParseAsType::F64
        ));
        assert!(matches!(
            &parsed.operations[7],
            AlterSchemaOperation::SetFieldSensitive { field, sensitive: false }
                if field.as_str() == "public_id"
        ));
    }

    #[test]
    fn parses_all_wire_schema_alter_operations_for_each_format() {
        let cases = [("JSON", "STRING"), ("CBOR", "OBJECT"), ("AVRO", "LONG")];
        for (format, ty) in cases {
            let input = format!(
                "ALTER WIRE {format} SCHEMA payload ADD FIELD added {ty} OPTIONAL, DROP FIELD \
                 removed, RENAME FIELD old TO renamed, ALTER FIELD renamed SET TYPE {ty}, ALTER \
                 FIELD renamed SET OPTIONAL, ALTER FIELD renamed DROP OPTIONAL;"
            );

            let parsed = parse_alter_wire_schema(&input).expect("parse should succeed");
            let (schema, operations_len) = match parsed {
                Statement::AlterWireJsonSchema(alter) | Statement::AlterWireCborSchema(alter) => {
                    (alter.schema.to_string(), alter.operations.len())
                }
                Statement::AlterWireAvroSchema(alter) => {
                    (alter.schema.to_string(), alter.operations.len())
                }
                _ => panic!("expected exact-format wire schema ALTER"),
            };
            assert_eq!(schema, "payload");
            assert_eq!(operations_len, 6);
        }
    }

    #[test]
    fn rejects_empty_and_cross_format_schema_alters() {
        assert!(parse_alter_schema("ALTER SCHEMA events;").is_err());
        assert!(
            parse_alter_schema("ALTER SCHEMA events ADD FIELD payload OBJECT;").is_err(),
            "wire-only type must not leak into internal ALTER"
        );
        assert!(
            parse_alter_wire_schema(
                "ALTER WIRE AVRO SCHEMA payload ALTER FIELD id SET TYPE INTEGER;"
            )
            .is_err(),
            "JSON-only type must not leak into AVRO ALTER"
        );
        assert!(
            parse_alter_wire_schema(
                "ALTER WIRE JSON SCHEMA payload ADD FIELD secret STRING SENSITIVE;"
            )
            .is_err(),
            "wire fields do not support sensitivity"
        );
        assert!(
            parse_alter_wire_schema("ALTER WIRE JSON SCHEMA payload SET LOOSE;").is_err(),
            "wire mode changes use ALTER WIRE <format> SCHEMA <name> MODE <mode>"
        );
        assert!(
            crate::statement::parse_statement("ALTER SCHEMA payload MODE LOOSE;").is_err(),
            "native-schema ALTER must not resolve a wire schema"
        );
        assert!(
            crate::statement::parse_statement("ALTER WIRE SCHEMA payload MODE LOOSE;").is_err(),
            "wire-schema ALTER must identify the exact wire format"
        );
    }

    #[test]
    fn suggests_composed_internal_alter_operations_without_wire_leakage() {
        let input = "ALTER SCHEMA events ";
        let suggestions = suggest_alter_schema(input, input.len());
        for expected in ["ADD FIELD", "DROP FIELD", "RENAME FIELD", "ALTER FIELD"] {
            assert!(suggestions.contains(&expected.to_string()));
        }
        assert!(!suggestions.contains(&"MODE".to_string()));

        let input = "ALTER SCHEMA events ALTER FIELD id ";
        let suggestions = suggest_alter_schema(input, input.len());
        for expected in [
            "SET TYPE",
            "SET OPTIONAL",
            "DROP OPTIONAL",
            "SET SENSITIVE",
            "DROP SENSITIVE",
        ] {
            assert!(suggestions.contains(&expected.to_string()));
        }
    }

    #[test]
    fn suggests_exact_wire_alter_operations_and_types() {
        let input = "ALTER WIRE JSON SCHEMA payload ";
        let suggestions = suggest_alter_wire_schema(input, input.len());
        for expected in [
            "MODE",
            "ADD FIELD",
            "DROP FIELD",
            "RENAME FIELD",
            "ALTER FIELD",
        ] {
            assert!(suggestions.contains(&expected.to_string()));
        }
        assert!(!suggestions.contains(&"SET SENSITIVE".to_string()));

        let input = "ALTER WIRE AVRO SCHEMA payload ALTER FIELD id SET TYPE ";
        let suggestions = suggest_alter_wire_schema(input, input.len());
        assert!(suggestions.contains(&"LONG".to_string()));
        assert!(!suggestions.contains(&"INTEGER".to_string()));
        assert!(!suggestions.contains(&"U64".to_string()));
    }

    #[test]
    fn parses_and_completes_exact_wire_schema_mode_alter() {
        let parsed =
            crate::statement::parse_statement("ALTER WIRE JSON SCHEMA payload MODE LOOSE;")
                .expect("parse should succeed");
        assert!(matches!(
            parsed,
            nervix_models::Statement::AlterWireJsonSchema(_)
        ));

        let input = "ALTER WIRE ";
        let suggestions = crate::statement::suggest_statement(input, input.len());
        for format in ["JSON", "CBOR", "AVRO"] {
            assert!(suggestions.contains(&format.to_string()));
        }
        assert!(!suggestions.contains(&"SCHEMA".to_string()));

        let input = "ALTER WIRE JSON SCHEMA payload ";
        let suggestions = crate::statement::suggest_statement(input, input.len());
        assert!(suggestions.contains(&"MODE".to_string()));
        assert!(suggestions.contains(&"ADD FIELD".to_string()));

        let input = "ALTER WIRE JSON SCHEMA payload MODE ";
        let suggestions = crate::statement::suggest_statement(input, input.len());
        assert!(suggestions.contains(&"STRICT".to_string()));
        assert!(suggestions.contains(&"LOOSE".to_string()));
        assert!(!suggestions.contains(&"ADD FIELD".to_string()));
        assert!(!suggestions.contains(&"STRING".to_string()));
    }
}
