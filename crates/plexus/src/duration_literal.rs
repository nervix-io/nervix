//! The `DURATION` literal an expression, a relay binding and a branch selection write.
//!
//! Layer: language.
//! - **Owns.** The literal's grammar: its parts, each a number and the unit written right after
//!   it, where the literal ends, and the refusal of a unit it does not write, as it is found.
//! - **Depends on.** Shared Plexus tokens and the vocabulary's `DurationValue`, which alone decides
//!   the units, their spellings and the value the parts add up to.
//! - **Must not know.** Clause durations, which keep their own grammar, or the VM and runtime that
//!   use the value.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::{DurationLiteralPart, DurationLiteralUnit, DurationLiteralWord, DurationValue};

use crate::{
    lexer::{Token, Word},
    parser_support::ParseError,
};

/// A `DURATION` literal, as an expression or a relay binding writes it: one or more parts, each a
/// number and its unit written right after it, such as `30s`, `250ms` or `1h 30m`, read as the
/// value its parts add up to.
///
/// A literal ends at the first word that names no unit, so `5 FLUSH` reads as the number `5`
/// before the clause that follows it, and a number with no unit stays a number. A word that names
/// a unit a literal does not write, a month, a year, weeks or another spelling such as `min`, is
/// refused with that unit's name rather than read as whatever follows. Each part keeps its own
/// unit, so `1h30m`, whose second word `h30m` names no unit, is not a literal.
pub(crate) fn duration_value_literal<'src>()
-> impl Parser<'src, &'src [Token], DurationValue, extra::Err<ParseError<'src>>> + Clone {
    // Once a number is followed by a word that names a unit, the words belong to a duration
    // literal, so what is wrong with it is reported as it is found, through `validate`, rather than
    // failing the literal: a failed literal would be read again as the number alone, and the
    // refusal would be lost behind an error about the unit that follows the number.
    let unit = select! {
        Token::Word(Word::KnownWord { raw, .. }) if DurationLiteralUnit::of_word(&raw).names_a_unit() => {
            DurationLiteralUnit::of_word(&raw)
        },
        Token::Word(Word::UnknownWord(raw)) if DurationLiteralUnit::of_word(&raw).names_a_unit() => {
            DurationLiteralUnit::of_word(&raw)
        },
    }
    .labelled("duration_unit")
    .validate(|word, extra, emitter| match word {
        DurationLiteralWord::Unit(unit) => unit,
        DurationLiteralWord::Refused(error) => {
            emitter.emit(Rich::custom(
                extra.span(),
                format!("invalid duration literal: {error}"),
            ));
            DurationLiteralUnit::Second
        }
        DurationLiteralWord::NotAUnit => {
            emitter.emit(Rich::custom(
                extra.span(),
                "expected the unit of a duration literal part",
            ));
            DurationLiteralUnit::Second
        }
    });
    let part = select! { Token::NumberLiteral(number) => number }
        .then(unit)
        .validate(|(number, unit), extra, emitter| {
            if number.contains(['e', 'E']) {
                emitter.emit(Rich::custom(
                    extra.span(),
                    format!(
                        "duration part '{number}{}' has an exponent, which a duration literal \
                         does not read",
                        unit.spelling()
                    ),
                ));
            }
            (number, unit)
        });
    part.repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .validate(
            |parts: Vec<(String, DurationLiteralUnit)>, extra, emitter| {
                let mut written = Vec::with_capacity(parts.len());
                for (number, unit) in &parts {
                    written.push(DurationLiteralPart {
                        number,
                        unit: *unit,
                    });
                }
                match DurationValue::from_literal_parts(&written) {
                    Ok(value) => value,
                    Err(report) => {
                        let text = DurationLiteralPart::text_of(&written);
                        emitter.emit(Rich::custom(
                            extra.span(),
                            format!("invalid duration literal '{text}': {report:#}"),
                        ));
                        DurationValue::ZERO
                    }
                }
            },
        )
        .boxed()
}
