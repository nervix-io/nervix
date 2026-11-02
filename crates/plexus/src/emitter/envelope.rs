//! Shared grammar of the envelope clauses an emitter writes after its payload.
//!
//! Layer: language.
//! - **Owns.** The envelope section: each clause an entity declares, written at most once in the
//!   order clause kinds are listed, and the `PROPERTIES` block over the entity's closed key set.
//! - **Depends on.** Shared keyword, duration, string-literal and expression clauses, and the
//!   envelope clauses and properties an entity declares.
//! - **Must not know.** Which connector declares a clause, how the registry checks its types, or
//!   how a host evaluates and carries an envelope.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use std::collections::BTreeSet;

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
        section = match kind {
            EnvelopeClauseKind::Persistent => {
                then_optional(section, kw(Identifier::Persistent), |envelope, ()| {
                    envelope.persistent = true;
                })
            }
            EnvelopeClauseKind::Mandatory => {
                then_optional(section, kw(Identifier::Mandatory), |envelope, ()| {
                    envelope.mandatory = true;
                })
            }
            EnvelopeClauseKind::Retain => {
                then_optional(section, kw(Identifier::Retain), |envelope, ()| {
                    envelope.retain = true;
                })
            }
            EnvelopeClauseKind::Timestamp => then_optional(
                section,
                kw(Identifier::Timestamp).ignore_then(embedded_expression("timestamp_expression")),
                |envelope, expression| envelope.timestamp = Some(expression),
            ),
            EnvelopeClauseKind::DeliverAfter => then_optional(
                section,
                kw_phrase2(Identifier::Deliver, Identifier::After).ignore_then(duration_lit()),
                |envelope, duration| envelope.deliver_after = Some(duration),
            ),
            EnvelopeClauseKind::Delay => then_optional(
                section,
                kw(Identifier::Delay).ignore_then(duration_lit()),
                |envelope, duration| envelope.delay = Some(duration),
            ),
            EnvelopeClauseKind::ReplyTo => then_optional(
                section,
                kw_phrase2(Identifier::Reply, Identifier::To)
                    .ignore_then(embedded_expression("string_expression")),
                |envelope, expression| envelope.reply_to = Some(expression),
            ),
            EnvelopeClauseKind::MessageId => then_optional(
                section,
                kw_phrase2(Identifier::Message, Identifier::Id)
                    .ignore_then(embedded_expression("string_expression")),
                |envelope, expression| envelope.message_id = Some(expression),
            ),
            EnvelopeClauseKind::DeduplicationId => then_optional(
                section,
                kw_phrase2(Identifier::Deduplication, Identifier::Id)
                    .ignore_then(embedded_expression("string_expression")),
                |envelope, expression| envelope.deduplication_id = Some(expression),
            ),
        };
    }
    if properties.is_empty() {
        return section;
    }
    then_optional(
        section,
        kw(Identifier::Properties).ignore_then(properties_values(properties)),
        |envelope, set| {
            envelope.properties = set;
        },
    )
}

/// `section` followed by an optional `clause`, whose value `write` records in the envelope.
fn then_optional<'src, T: 'src>(
    section: Section<'src>,
    clause: impl Parser<'src, &'src [Token], T, extra::Err<ParseError<'src>>> + Clone + 'src,
    write: fn(&mut EmitterEnvelope, T),
) -> Section<'src> {
    section
        .then(clause.or_not())
        .map(move |(mut envelope, written)| {
            if let Some(value) = written {
                write(&mut envelope, value);
            }
            envelope
        })
        .boxed()
}

/// `PROPERTIES { '<key>' = <expression>, … }`, setting each of the `declared` keys at most once.
///
/// The keys are a closed set, so each is offered as the exact literal that names it, and any other
/// key is refused where it is written.
fn properties_values<'src>(
    declared: &'static [PropertyDeclaration],
) -> impl Parser<'src, &'src [Token], Vec<EnvelopeProperty>, extra::Err<ParseError<'src>>> + Clone {
    let mut keys = Vec::with_capacity(declared.len());
    for property in declared {
        let name = property.name.to_string();
        keys.push(just(Token::StringLiteral(name.clone())).to(name).boxed());
    }
    let property = choice(keys)
        .then_ignore(tok(Token::Eq))
        .then(embedded_expression("value_expression"))
        .map(|(name, expression)| EnvelopeProperty { name, expression });
    property
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LBrace), tok(Token::RBrace))
        .try_map(|properties: Vec<EnvelopeProperty>, span| {
            let mut set = BTreeSet::new();
            for property in &properties {
                if !set.insert(property.name.as_str()) {
                    let duplicate = EnvelopeDeclarationError::DuplicateProperty {
                        name: property.name.clone(),
                    };
                    return Err(Rich::custom(span, duplicate.to_string()));
                }
            }
            Ok(properties)
        })
        .boxed()
}

/// Targeted edits offered only where the entity declares their envelope surface.
pub(super) fn alterations<'src>(
    declared: &EmitterEntityCapabilities,
) -> Vec<Boxed<'src, 'src, &'src [Token], AlterEmitterOperation, extra::Err<ParseError<'src>>>> {
    let mut choices = Vec::new();
    if declared.declares_envelope_clause(EnvelopeClauseKind::Persistent) {
        choices.push(
            kw_phrase2(Identifier::Set, Identifier::Persistent)
                .to(EmitterEnvelopeAlteration::SetPersistent)
                .map(operation)
                .boxed(),
        );
        choices.push(
            kw_phrase2(Identifier::Drop, Identifier::Persistent)
                .to(EmitterEnvelopeAlteration::DropPersistent)
                .map(operation)
                .boxed(),
        );
    }
    if declared.declares_envelope_clause(EnvelopeClauseKind::Timestamp) {
        choices.push(
            kw_phrase2(Identifier::Set, Identifier::Timestamp)
                .ignore_then(embedded_expression("timestamp_expression"))
                .map(|expression| EmitterEnvelopeAlteration::SetTimestamp { expression })
                .map(operation)
                .boxed(),
        );
        choices.push(
            kw_phrase2(Identifier::Drop, Identifier::Timestamp)
                .to(EmitterEnvelopeAlteration::DropTimestamp)
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
