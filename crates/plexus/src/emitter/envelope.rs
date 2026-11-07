//! Shared grammar of the envelope clauses an emitter writes after its payload.
//!
//! Layer: language.
//! - **Owns.** The envelope section: each clause an entity declares, written at most once in the
//!   order clause kinds are listed, and the `PROPERTIES` block over the entity's closed key set;
//!   and the `SET` and `DROP` of each declared clause and of `PROPERTIES` an `ALTER` writes.
//! - **Depends on.** Shared keyword, duration, string-literal and expression clauses, and the
//!   envelope clauses and properties an entity declares.
//! - **Must not know.** Which connector declares a clause, how the registry checks its types, or
//!   how a host evaluates and carries an envelope.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use strum::IntoEnumIterator as _;

use super::*;

/// A parser of the envelope section written so far.
type Section<'src> =
    Boxed<'src, 'src, &'src [Token], EmitterEnvelope, extra::Err<ParseError<'src>>>;

/// The envelope section of an emitter whose entity declares `clauses` and `properties`.
///
/// Each declared clause may be written once, in the order [`EnvelopeClauseKind`] lists the kinds,
/// and `PROPERTIES` follows them where the entity declares a property. Completion offers exactly
/// these continuations, and an entity that declares nothing reads no token here, so a clause it
/// does not declare is refused where it is written.
pub(super) fn envelope_section<'src>(
    clauses: &'static [EnvelopeClauseKind],
    properties: &'static [PropertyDeclaration],
) -> Section<'src> {
    let mut section = empty().to(EmitterEnvelope::default()).boxed();
    for kind in EnvelopeClauseKind::iter() {
        if !clauses.contains(&kind) {
            continue;
        }
        section = section
            .then(clause_value(kind).or_not())
            .map(|(mut envelope, written)| {
                if let Some(value) = written {
                    envelope.set_clause(&value);
                }
                envelope
            })
            .boxed();
    }
    if properties.is_empty() {
        return section;
    }
    section
        .then(
            kw(Identifier::Properties)
                .ignore_then(properties_values(properties))
                .or_not(),
        )
        .map(|(mut envelope, written)| {
            if let Some(set) = written {
                envelope.properties = set;
            }
            envelope
        })
        .boxed()
}

/// The words that open an envelope clause.
#[derive(Clone, Copy)]
enum ClauseWords {
    One(Identifier),
    Two(Identifier, Identifier),
    Three(Identifier, Identifier, Identifier),
}

/// The words that open the clause `kind`.
fn clause_words(kind: EnvelopeClauseKind) -> ClauseWords {
    match kind {
        EnvelopeClauseKind::Key => ClauseWords::One(Identifier::Key),
        EnvelopeClauseKind::PartitionFromBranch => {
            ClauseWords::Three(Identifier::Partition, Identifier::From, Identifier::Branch)
        }
        EnvelopeClauseKind::Persistent => ClauseWords::One(Identifier::Persistent),
        EnvelopeClauseKind::Mandatory => ClauseWords::One(Identifier::Mandatory),
        EnvelopeClauseKind::Retain => ClauseWords::One(Identifier::Retain),
        EnvelopeClauseKind::Timestamp => ClauseWords::One(Identifier::Timestamp),
        EnvelopeClauseKind::DeliverAfter => {
            ClauseWords::Two(Identifier::Deliver, Identifier::After)
        }
        EnvelopeClauseKind::Delay => ClauseWords::One(Identifier::Delay),
        EnvelopeClauseKind::ReplyTo => ClauseWords::Two(Identifier::Reply, Identifier::To),
        EnvelopeClauseKind::MessageId => ClauseWords::Two(Identifier::Message, Identifier::Id),
        EnvelopeClauseKind::DeduplicationId => {
            ClauseWords::Two(Identifier::Deduplication, Identifier::Id)
        }
    }
}

/// The keywords that open the clause `kind`.
fn clause_keywords<'src>(
    kind: EnvelopeClauseKind,
) -> Boxed<'src, 'src, &'src [Token], (), extra::Err<ParseError<'src>>> {
    match clause_words(kind) {
        ClauseWords::One(word) => kw(word).boxed(),
        ClauseWords::Two(first, second) => kw_phrase2(first, second).boxed(),
        ClauseWords::Three(first, second, third) => kw_phrase3(first, second, third).boxed(),
    }
}

/// `head` and the keywords of the clause `kind` as one grammar unit, so completion offers an
/// operation such as `SET DELIVER AFTER` or `DROP PERSISTENT` as one item.
fn operation_keywords<'src>(
    head: Identifier,
    kind: EnvelopeClauseKind,
) -> Boxed<'src, 'src, &'src [Token], (), extra::Err<ParseError<'src>>> {
    match clause_words(kind) {
        ClauseWords::One(word) => kw_phrase2(head, word).boxed(),
        ClauseWords::Two(first, second) => kw_phrase3(head, first, second).boxed(),
        ClauseWords::Three(first, second, third) => kw_phrase([head, first, second, third]).boxed(),
    }
}

/// What the clause `kind` is written with after its keywords, read into the clause it writes.
fn clause_tail<'src>(
    kind: EnvelopeClauseKind,
) -> Boxed<'src, 'src, &'src [Token], EnvelopeClauseValue, extra::Err<ParseError<'src>>> {
    match kind {
        EnvelopeClauseKind::Key => embedded_expression("key_expression")
            .map(EnvelopeClauseValue::Key)
            .boxed(),
        EnvelopeClauseKind::PartitionFromBranch => {
            empty().to(EnvelopeClauseValue::PartitionFromBranch).boxed()
        }
        EnvelopeClauseKind::Persistent => empty().to(EnvelopeClauseValue::Persistent).boxed(),
        EnvelopeClauseKind::Mandatory => empty().to(EnvelopeClauseValue::Mandatory).boxed(),
        EnvelopeClauseKind::Retain => empty().to(EnvelopeClauseValue::Retain).boxed(),
        EnvelopeClauseKind::Timestamp => embedded_expression("timestamp_expression")
            .map(EnvelopeClauseValue::Timestamp)
            .boxed(),
        EnvelopeClauseKind::DeliverAfter => duration_lit()
            .map(EnvelopeClauseValue::DeliverAfter)
            .boxed(),
        EnvelopeClauseKind::Delay => duration_lit().map(EnvelopeClauseValue::Delay).boxed(),
        EnvelopeClauseKind::ReplyTo => embedded_expression("string_expression")
            .map(EnvelopeClauseValue::ReplyTo)
            .boxed(),
        EnvelopeClauseKind::MessageId => embedded_expression("string_expression")
            .map(EnvelopeClauseValue::MessageId)
            .boxed(),
        EnvelopeClauseKind::DeduplicationId => embedded_expression("string_expression")
            .map(EnvelopeClauseValue::DeduplicationId)
            .boxed(),
    }
}

/// The clause `kind` written in full: its keywords, then what it is written with.
fn clause_value<'src>(
    kind: EnvelopeClauseKind,
) -> Boxed<'src, 'src, &'src [Token], EnvelopeClauseValue, extra::Err<ParseError<'src>>> {
    clause_keywords(kind).ignore_then(clause_tail(kind)).boxed()
}

/// `PROPERTIES { '<key>' = <expression>, … }`, setting each of the `declared` keys at most once.
///
/// The keys are a closed set. Each remaining key is offered as its exact literal, so completing
/// another assignment cannot repeat a key already written. An unknown or repeated key is refused
/// where it is written.
fn properties_values<'src>(
    declared: &'static [PropertyDeclaration],
) -> impl Parser<'src, &'src [Token], Vec<EnvelopeProperty>, extra::Err<ParseError<'src>>> + Clone {
    let value = embedded_expression("value_expression");
    custom(move |input| {
        let mut properties: Vec<EnvelopeProperty> = Vec::with_capacity(declared.len());
        input.parse(tok(Token::LBrace))?;
        // Every assignment consumes one distinct declared key, bounding the number of iterations.
        loop {
            if let Some(Token::StringLiteral(name)) = input.peek_ref()
                && properties.iter().any(|property| property.name == *name)
            {
                let name = name.clone();
                let token = Token::StringLiteral(name.clone());
                let duplicate = EnvelopeDeclarationError::DuplicateProperty { name };
                return input.parse(
                    just(token)
                        .try_map(move |_, span| Err(Rich::custom(span, duplicate.to_string()))),
                );
            }
            let mut next: Vec<_> = declared
                .iter()
                .filter(|declaration| {
                    !properties
                        .iter()
                        .any(|property| property.name == declaration.name)
                })
                .map(|declaration| {
                    let name = declaration.name.to_string();
                    just(Token::StringLiteral(name.clone()))
                        .to(Some(name))
                        .boxed()
                })
                .collect();
            if !properties.is_empty() {
                next.push(tok(Token::RBrace).to(None).boxed());
            }
            let Some(name) = input.parse(choice(next))? else {
                return Ok(properties);
            };
            input.parse(tok(Token::Eq))?;
            let expression = input.parse(value.clone())?;
            properties.push(EnvelopeProperty { name, expression });
            let more = input.parse(choice((
                tok(Token::Comma).to(true),
                tok(Token::RBrace).to(false),
            )))?;
            if !more {
                return Ok(properties);
            }
        }
    })
}

/// The changes an `ALTER` makes to the envelope of an emitter whose entity declares `declared`:
/// `SET` and `DROP` of each clause it declares, and of `PROPERTIES` where it declares properties.
/// Each operation's keywords are one grammar unit, so completion offers exactly these.
pub(super) fn alterations<'src>(
    declared: &EmitterEntityCapabilities,
) -> Vec<Boxed<'src, 'src, &'src [Token], AlterEmitterOperation, extra::Err<ParseError<'src>>>> {
    let mut choices = Vec::new();
    for kind in declared.envelope {
        let kind = *kind;
        choices.push(
            operation_keywords(Identifier::Set, kind)
                .ignore_then(clause_tail(kind))
                .map(|clause| EmitterEnvelopeAlteration::SetClause { clause })
                .map(operation)
                .boxed(),
        );
        choices.push(
            operation_keywords(Identifier::Drop, kind)
                .to(EmitterEnvelopeAlteration::DropClause { clause: kind })
                .map(operation)
                .boxed(),
        );
    }
    if !declared.properties.is_empty() {
        choices.push(
            kw_phrase2(Identifier::Set, Identifier::Properties)
                .ignore_then(properties_values(declared.properties))
                .map(|properties| EmitterEnvelopeAlteration::SetProperties { properties })
                .map(operation)
                .boxed(),
        );
        choices.push(
            kw_phrase2(Identifier::Drop, Identifier::Properties)
                .to(EmitterEnvelopeAlteration::DropProperties)
                .map(operation)
                .boxed(),
        );
    }
    choices
}

/// `alteration` as the `ALTER` operation that carries it.
fn operation(alteration: EmitterEnvelopeAlteration) -> AlterEmitterOperation {
    AlterEmitterOperation::AlterEnvelope {
        alteration: Box::new(alteration),
    }
}

/// The envelope section of an emitter of the entity `S`, as its declaration states it.
pub(super) fn entity_envelope<'src, S: EmitterEntityModel>() -> Section<'src> {
    let declared = S::KIND.capabilities();
    envelope_section(declared.envelope, declared.properties)
}

#[cfg(test)]
#[path = "envelope_tests.rs"]
mod tests;
