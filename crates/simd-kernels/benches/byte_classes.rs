//! Criterion throughput of byte classes and the XML character check against the loops they
//! replaced.
//!
//! Each case runs the kernel beside the loop its caller ran before: a scalar `position` over an
//! RFC 5424 header, a per-byte walk over structured-data parameter values, and standard UTF-8
//! validation followed by a `chars().all(..)` over an SQS body. Both arms read the same bytes in
//! the same process, so a round compares them under the same conditions.

use std::ops::RangeInclusive;

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use meticulous::ResultExt as _;
use nervix_simd_kernels::{ByteClass, ByteScanner, XmlChars};

/// Bytes outside RFC 5424 `PRINTUSASCII`, as the syslog header check classifies them.
struct OutsidePrintUsAscii;

impl ByteClass for OutsidePrintUsAscii {
    const ORDINARY: RangeInclusive<u8> = b'!'..=b'~';
}

/// The bytes a structured-data `PARAM-VALUE` gives meaning to.
struct ParamValueSpecial;

impl ByteClass for ParamValueSpecial {
    const ORDINARY: RangeInclusive<u8> = u8::MIN..=u8::MAX;
    const LISTED: &'static [u8] = b"\"\\]";
}

/// The scalar header check the decoder ran before: the first byte outside printable US-ASCII.
fn scalar_first_outside_printable(bytes: &[u8]) -> Option<usize> {
    bytes.iter().position(|byte| !(b'!'..=b'~').contains(byte))
}

/// Visits every `"`, `\` and `]` the way the per-byte parameter-value loop met them: one byte at a
/// time from the cursor until the next one, which stops the walk, and counts them. The early stop
/// is what kept the compiler from vectorizing the loop the parser ran.
fn scalar_value_specials(bytes: &[u8]) -> usize {
    let mut specials = 0;
    let mut from = 0;
    while let Some(offset) = bytes[from..]
        .iter()
        .position(|byte| matches!(byte, b'"' | b'\\' | b']'))
    {
        specials += 1;
        from += offset + 1;
    }
    specials
}

fn scanned_value_specials(bytes: &[u8]) -> usize {
    let mut scanner = ByteScanner::<ParamValueSpecial>::new(bytes);
    let mut specials = 0;
    let mut from = 0;
    while let Some(found) = scanner.next(from) {
        specials += 1;
        from = found + 1;
    }
    specials
}

/// The SQS body check the sink ran before: standard UTF-8 validation, then one decoded character
/// at a time against the XML 1.0 `Char` production.
fn scalar_xml_text(bytes: Vec<u8>) -> Option<String> {
    let text = String::from_utf8(bytes).ok()?;
    let admitted = text.chars().all(|character| {
        matches!(character, '\t' | '\n' | '\r')
            || matches!(
                u32::from(character),
                0x20..=0xD7FF | 0xE000..=0xFFFD | 0x1_0000..=0x10_FFFF
            )
    });
    admitted.then_some(text)
}

/// Structured data whose parameter values run `value_bytes` long, the shape that dominates a
/// structured-data scan.
fn structured_data(elements: usize, value_bytes: usize) -> Vec<u8> {
    let value = "v".repeat(value_bytes);
    let mut text = String::new();
    for element in 0..elements {
        text.push_str(&format!(
            "[element{element}@32473 first=\"{value}\" escaped=\"{value}\\\"{value}\"]"
        ));
    }
    text.into_bytes()
}

/// A JSON body of `length` bytes, mostly ASCII or mostly multi-byte characters.
fn json_body(length: usize, multilingual: bool) -> Vec<u8> {
    let note = if multilingual {
        "Nervix überträgt Ereignisse — 事件流处理 \u{1F680} "
    } else {
        "nervix forwards one event per record "
    };
    let mut text = String::from("{\"note\":\"");
    while text.len() + note.len() + 2 < length {
        text.push_str(note);
    }
    text.push_str("\"}");
    text.into_bytes()
}

fn headers(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("header_first_outside_printable");
    for length in [8_usize, 48, 255] {
        let header = vec![b'h'; length];
        group.throughput(Throughput::Bytes(
            u64::try_from(length).assured("a header length fits u64"),
        ));
        group.bench_with_input(
            BenchmarkId::new("kernel", length),
            &header,
            |bencher, header| {
                bencher.iter(|| OutsidePrintUsAscii::first_in(black_box(header)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scalar", length),
            &header,
            |bencher, header| {
                bencher.iter(|| scalar_first_outside_printable(black_box(header)));
            },
        );
    }
    group.finish();
}

fn parameter_values(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("structured_data_value_specials");
    for (elements, value_bytes) in [(2_usize, 8_usize), (4, 64), (8, 512)] {
        let bytes = structured_data(elements, value_bytes);
        let label = format!("{elements}x{value_bytes}");
        group.throughput(Throughput::Bytes(
            u64::try_from(bytes.len()).assured("a buffer length fits u64"),
        ));
        group.bench_with_input(
            BenchmarkId::new("scanner", &label),
            &bytes,
            |bencher, bytes| {
                bencher.iter(|| scanned_value_specials(black_box(bytes)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scalar", &label),
            &bytes,
            |bencher, bytes| {
                bencher.iter(|| scalar_value_specials(black_box(bytes)));
            },
        );
    }
    group.finish();
}

fn xml_text(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("sqs_body_xml_chars");
    for (length, multilingual) in [
        (256_usize, false),
        (4_096, false),
        (4_096, true),
        (262_144, false),
        (262_144, true),
    ] {
        let body = json_body(length, multilingual);
        let label = format!(
            "{}{}",
            length,
            if multilingual {
                "_multilingual"
            } else {
                "_ascii"
            }
        );
        group.throughput(Throughput::Bytes(
            u64::try_from(body.len()).assured("a body length fits u64"),
        ));
        group.bench_with_input(
            BenchmarkId::new("kernel", &label),
            &body,
            |bencher, body| {
                bencher.iter_batched(
                    || body.clone(),
                    |body| XmlChars::into_string(black_box(body)).ok(),
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scalar", &label),
            &body,
            |bencher, body| {
                bencher.iter_batched(
                    || body.clone(),
                    |body| scalar_xml_text(black_box(body)),
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(byte_classes, headers, parameter_values, xml_text);
criterion_main!(byte_classes);
