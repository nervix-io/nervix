//! Byte class qualification against the scalar class definition at every SIMD level.
//!
//! Layer: test harness.
//! - **Owns.** Block, register and tail boundaries, every byte value in every lane, and the
//!   differential properties of first members and forward scans.
//! - **Depends on.** The production byte classes and the supported SIMD levels.
//! - **Must not know.** The protocols whose bytes a class names.

use super::*;
use crate::supported_levels;

/// Every byte outside printable US-ASCII: a range and nothing named beside it.
struct OutsidePrintable;

impl ByteClass for OutsidePrintable {
    const ORDINARY: RangeInclusive<u8> = b'!'..=b'~';
}

/// Listed bytes only, inside an ordinary range that covers every byte value.
struct Delimiters;

impl ByteClass for Delimiters {
    const ORDINARY: RangeInclusive<u8> = u8::MIN..=u8::MAX;
    const LISTED: &'static [u8] = b"\"\\]";
}

/// Every part a class can have: both range edges, excluded bytes below and above the range,
/// and listed bytes at its edges and inside it.
struct Mixed;

impl ByteClass for Mixed {
    const ORDINARY: RangeInclusive<u8> = 0x20..=0x7E;
    const EXCLUDED: &'static [u8] = &[b'\t', b'\n', 0x7F, 0xEF];
    const LISTED: &'static [u8] = b" =\"~";
}

fn scalar_first<C: ByteClass>(bytes: &[u8]) -> Option<usize> {
    bytes.iter().position(|byte| C::contains(*byte))
}

/// Buffers longer than three blocks whose members sit on every lane of the first block, and on
/// the register, block and tail boundaries after it, for each class.
fn boundary_buffers() -> Vec<Vec<u8>> {
    let length = 3 * WORD_LANES + 17;
    let ordinary = vec![b'a'; length];
    let mut buffers = vec![Vec::new(), ordinary.clone()];
    let members = [
        b' ', 0x00, 0xFF, b'"', b'\\', b']', b'=', b'~', 0x7F, 0xEF, b'\t',
    ];
    let positions = (0..WORD_LANES).chain([64, 79, 80, 95, 96, 127, 128, 191, 192, 208]);
    for position in positions {
        for member in members {
            let mut buffer = ordinary.clone();
            buffer[position] = member;
            buffers.push(buffer);
        }
    }
    let mut random = fastrand::Rng::with_seed(0x0009_B17E);
    for _ in 0..64 {
        let sparse = (0..length)
            .map(|_| {
                if random.u8(..16) == 0 {
                    random.u8(..)
                } else {
                    b'a'
                }
            })
            .collect();
        buffers.push(sparse);
    }
    buffers
}

fn check_first_members<C: ByteClass>(level: Level, buffer: &[u8]) {
    for start in 0..=buffer.len() {
        let tail = &buffer[start..];
        assert_eq!(
            first_member::<C>(level, tail),
            scalar_first::<C>(tail),
            "level={level:?} start={start} buffer={buffer:?}"
        );
    }
}

/// Walks `buffer` with one scanner the way a parser does, asking for the member after each
/// member it found, and after each jump in `jumps`, and compares every answer with the scalar
/// definition.
fn check_scan<C: ByteClass>(level: Level, buffer: &[u8], jumps: &[usize]) {
    let mut scanner = ByteScanner::<C>::with_level(level, buffer);
    let mut from = 0;
    let mut jumps = jumps.iter();
    loop {
        let expected = match buffer.get(from..) {
            Some(tail) => scalar_first::<C>(tail).map(|offset| from + offset),
            None => None,
        };
        let found = scanner.next(from);
        assert_eq!(
            found, expected,
            "level={level:?} from={from} buffer={buffer:?}"
        );
        let Some(found) = found else {
            break;
        };
        let jump = match jumps.next() {
            Some(jump) => *jump,
            None => 0,
        };
        from = found + 1 + jump;
    }
    let past_end = buffer.len() + 1;
    assert_eq!(scanner.next(past_end), None, "level={level:?}");
}

#[test]
fn first_members_match_the_scalar_definition_at_every_level() {
    for level in supported_levels() {
        for buffer in boundary_buffers() {
            check_first_members::<OutsidePrintable>(level, &buffer);
            check_first_members::<Delimiters>(level, &buffer);
            check_first_members::<Mixed>(level, &buffer);
        }
    }
}

#[test]
fn every_byte_value_in_every_lane_is_classified_by_the_scalar_definition() {
    for level in supported_levels() {
        for lane in 0..WORD_LANES {
            for value in u8::MIN..=u8::MAX {
                let mut buffer = [b'a'; WORD_LANES];
                buffer[lane] = value;
                let expected = |member: bool| if member { Some(lane) } else { None };
                assert_eq!(
                    first_member::<OutsidePrintable>(level, &buffer),
                    expected(OutsidePrintable::contains(value)),
                    "level={level:?} lane={lane} value={value}"
                );
                assert_eq!(
                    first_member::<Delimiters>(level, &buffer),
                    expected(Delimiters::contains(value)),
                    "level={level:?} lane={lane} value={value}"
                );
                assert_eq!(
                    first_member::<Mixed>(level, &buffer),
                    expected(Mixed::contains(value)),
                    "level={level:?} lane={lane} value={value}"
                );
            }
        }
    }
}

#[test]
fn class_definitions_name_their_range_excluded_and_listed_bytes() {
    assert!(OutsidePrintable::contains(b' '));
    assert!(OutsidePrintable::contains(0x7F));
    assert!(!OutsidePrintable::contains(b'!'));
    assert!(!OutsidePrintable::contains(b'~'));
    assert!(Delimiters::contains(b']'));
    assert!(!Delimiters::contains(0x00));
    assert!(!Delimiters::contains(0xFF));
    assert!(Mixed::contains(0x1F));
    assert!(!Mixed::contains(b'\t'));
    assert!(!Mixed::contains(0xEF));
    assert!(Mixed::contains(0xF0));
    assert!(Mixed::contains(b' '));
    assert!(Mixed::contains(b'~'));
    assert!(!Mixed::contains(b'}'));
}

#[test]
fn first_in_and_new_scanners_select_the_process_level() {
    assert_eq!(OutsidePrintable::first_in(b"edge-1 orders"), Some(6));
    assert_eq!(Delimiters::first_in(b"plain"), None);
    assert_eq!(Delimiters::first_in(b""), None);
    let long_token = b"a-hostname-longer-than-one-narrow-register.example";
    assert_eq!(OutsidePrintable::first_in(long_token), None);
    assert_eq!(
        Delimiters::first_in(b"name=\"value with a ] inside\""),
        Some(5)
    );

    let structured = b"[id key=\"escaped \\\" quote\" other=\"]\"]";
    let mut scanner = ByteScanner::<Delimiters>::new(structured);
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(position) = scanner.next(from) {
        found.push(position);
        from = position + 1;
    }
    let expected = (0..structured.len())
        .filter(|position| Delimiters::contains(structured[*position]))
        .collect::<Vec<_>>();
    assert_eq!(found, expected);
}

#[test]
fn forward_scans_find_every_member_once_at_every_level() {
    let mut random = fastrand::Rng::with_seed(0x0009_5CA9);
    for level in supported_levels() {
        for buffer in boundary_buffers() {
            check_scan::<OutsidePrintable>(level, &buffer, &[]);
            check_scan::<Delimiters>(level, &buffer, &[]);
            check_scan::<Mixed>(level, &buffer, &[]);
            let jumps = (0..16).map(|_| random.usize(..80)).collect::<Vec<_>>();
            check_scan::<Mixed>(level, &buffer, &jumps);
        }
    }
}

#[test]
fn a_scanner_answers_an_earlier_position_from_a_fresh_classification() {
    let mut buffer = vec![b'a'; 3 * WORD_LANES];
    buffer[10] = b'"';
    buffer[150] = b']';
    for level in supported_levels() {
        let mut scanner = ByteScanner::<Delimiters>::with_level(level, &buffer);
        assert_eq!(scanner.next(11), Some(150));
        assert_eq!(scanner.next(151), None);
        assert_eq!(scanner.next(0), Some(10));
        assert_eq!(scanner.next(151), None);
    }
}

#[test]
fn bolero_byte_classes_match_the_scalar_definition_at_every_level() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .with_type::<(Vec<u8>, Vec<u8>, Vec<u8>)>()
        .for_each(|(bytes, noise, jumps)| {
            // Mostly ordinary bytes with generated members between them, so members land on
            // every lane and boundary rather than on every byte.
            let mut buffer = Vec::with_capacity(bytes.len() * 4);
            let mut noise = noise.iter().cycle();
            for byte in bytes {
                let run = match noise.next() {
                    Some(run) => usize::from(*run % 8),
                    None => 0,
                };
                buffer.extend(std::iter::repeat_n(b'a', run));
                buffer.push(*byte);
            }
            let jumps = jumps
                .iter()
                .map(|jump| usize::from(*jump % 96))
                .collect::<Vec<_>>();
            for level in supported_levels() {
                assert_eq!(
                    first_member::<OutsidePrintable>(level, &buffer),
                    scalar_first::<OutsidePrintable>(&buffer)
                );
                assert_eq!(
                    first_member::<Delimiters>(level, &buffer),
                    scalar_first::<Delimiters>(&buffer)
                );
                assert_eq!(
                    first_member::<Mixed>(level, &buffer),
                    scalar_first::<Mixed>(&buffer)
                );
                check_scan::<OutsidePrintable>(level, &buffer, &jumps);
                check_scan::<Delimiters>(level, &buffer, &jumps);
                check_scan::<Mixed>(level, &buffer, &jumps);
            }
        });
}
