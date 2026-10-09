//! Numbers through programs and the native formats.
//!
//! Layer: test harness.
//!
//! - **Owns.** The reproducers and the property that a number keeps its value, and a float its
//!   bits, through a program's run and through each native format's writer and reader.
//! - **Depends on.** The compiled program and the native formats.
//! - **Must not know.** Codecs, schemas, or which connector carries a payload.
//!
//! A program runs on jaq's own values and answers in JSON values, and a native format is read into
//! jaq's values before any program runs, so every number a codec or a signaling protocol carries
//! crosses the conversions held here. `XML` and `RAW` hold text rather than numbers and stay
//! outside this module.

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue, json};

use super::{CompiledJaqProgram, JaqNativeFormat};

/// The most numbers one generated case holds.
const NUMBERS: usize = 8;

/// The native formats that hold numbers.
const NUMERIC_FORMATS: [JaqNativeFormat; 4] = [
    JaqNativeFormat::Json,
    JaqNativeFormat::Yaml,
    JaqNativeFormat::Toml,
    JaqNativeFormat::Cbor,
];

/// Floats whose shortest decimal a best-effort decimal reader reads as a neighbouring float: the
/// product of `0.1` and `14`, the sum of ten tenths, a third of `271`, and a float far from one.
const NEIGHBOURED: [f64; 4] = [
    1.4000000000000001,
    0.9999999999999999,
    90.33333333333333,
    2.0030397744267762e-253,
];

/// Floats a generated case lands on as often as on any other: both zeros, the smallest and largest
/// magnitudes, the floats of [`NEIGHBOURED`], powers of ten on both sides of the exact ones, an
/// integer above 2^53, and two `F32` values widened.
const BOUNDARY_FLOATS: [f64; 16] = [
    0.0,
    -0.0,
    5e-324,
    f64::MIN_POSITIVE,
    f64::MAX,
    f64::MIN,
    1.4000000000000001,
    0.9999999999999999,
    90.33333333333333,
    2.0030397744267762e-253,
    1e22,
    1e23,
    9_007_199_254_740_994.0,
    0.1,
    1.100000023841858,
    7.038530691851209e-26,
];

/// A JSON number as its kind and exact value. A float keeps its bits, so a negative zero and the
/// last place of every float are compared.
#[derive(Debug, PartialEq, Eq)]
enum ExactNumber {
    Unsigned(u64),
    Signed(i64),
    Float(u64),
}

impl ExactNumber {
    fn of(number: &JsonNumber) -> Self {
        if let Some(value) = number.as_u64() {
            return Self::Unsigned(value);
        }
        if let Some(value) = number.as_i64() {
            return Self::Signed(value);
        }
        let value = number
            .as_f64()
            .assured("a JSON number that is no 64-bit integer is a float");
        Self::Float(value.to_bits())
    }
}

/// Asserts that `actual` holds the numbers of `expected`: the same members and elements, every
/// integer with its value and signedness and every float with its bits.
fn assert_same_numbers(actual: &JsonValue, expected: &JsonValue) {
    match (actual, expected) {
        (JsonValue::Number(actual), JsonValue::Number(expected)) => {
            assert_eq!(
                ExactNumber::of(actual),
                ExactNumber::of(expected),
                "{actual} is not {expected}"
            );
        }
        (JsonValue::Array(actual), JsonValue::Array(expected)) => {
            assert_eq!(actual.len(), expected.len(), "every element is kept");
            for (actual, expected) in actual.iter().zip(expected) {
                assert_same_numbers(actual, expected);
            }
        }
        (JsonValue::Object(actual), JsonValue::Object(expected)) => {
            assert_eq!(actual.len(), expected.len(), "every member is kept");
            for (name, expected) in expected {
                let Some(actual) = actual.get(name) else {
                    panic!("member {name} is kept");
                };
                assert_same_numbers(actual, expected);
            }
        }
        (actual, expected) => panic!("{actual} is not {expected}"),
    }
}

/// The program that answers with its input.
fn identity() -> CompiledJaqProgram {
    CompiledJaqProgram::compile(".").assured("the identity program is valid jaq")
}

/// What `format`'s reader reads from `payload` and the identity program then answers, as a codec
/// reads a payload before its ingestion program runs.
fn read_through_identity(
    format: JaqNativeFormat,
    identity: &CompiledJaqProgram,
    payload: Vec<u8>,
) -> JsonValue {
    let payload = Bytes::from(payload);
    let mut values = format.read_values(&payload);
    let input = values
        .next()
        .assured("a written payload holds its value")
        .assured("a written payload reads back");
    assert!(values.next().is_none(), "a written payload holds one value");
    let mut outputs = identity.outputs(input);
    let output = outputs
        .next()
        .assured("the identity program answers once")
        .assured("the identity program answers every value");
    assert!(
        outputs.next().is_none(),
        "the identity program answers once"
    );
    JsonValue::try_from(output).assured("a value of numbers is a JSON value")
}

/// The largest unsigned integer `format` carries. `TOML` holds the integers of the signed 64-bit
/// range: its writer writes a larger unsigned integer as it stands and its reader refuses it.
fn largest_unsigned(format: JaqNativeFormat) -> u64 {
    match format {
        JaqNativeFormat::Toml => i64::MAX.cast_unsigned(),
        JaqNativeFormat::Json
        | JaqNativeFormat::Yaml
        | JaqNativeFormat::Xml
        | JaqNativeFormat::Cbor
        | JaqNativeFormat::Raw => u64::MAX,
    }
}

/// One number of `format`'s domain, chosen by `selector` from `bits`: a finite float of any bit
/// pattern, a boundary float, a signed integer or an unsigned integer.
fn drawn_number(format: JaqNativeFormat, selector: u8, bits: u64) -> JsonValue {
    match selector % 4 {
        0 => {
            let value = f64::from_bits(bits);
            if value.is_finite() {
                return json!(value);
            }
            // Clearing the exponent's top bit keeps the sign and mantissa of a non-finite pattern
            // and lands on a finite float.
            json!(f64::from_bits(bits & !0x4000_0000_0000_0000))
        }
        1 => {
            let boundaries =
                u64::try_from(BOUNDARY_FLOATS.len()).assured("the boundary count fits in 64 bits");
            let index = usize::try_from(bits % boundaries)
                .verified("a remainder below the boundary count is an index");
            json!(BOUNDARY_FLOATS[index])
        }
        2 => json!(bits.cast_signed()),
        _ => {
            if bits > largest_unsigned(format) {
                return json!(bits >> 1);
            }
            json!(bits)
        }
    }
}

#[test]
fn a_program_answers_with_the_float_it_was_given() {
    let identity = identity();
    for value in NEIGHBOURED {
        let input = json!({"value": value, "values": [value]});

        let answered = identity
            .run_single(input.clone())
            .assured("the identity program answers every JSON value");

        assert_same_numbers(&answered, &input);
    }
}

#[test]
fn a_program_answers_with_the_float_it_computed() {
    let scaled = CompiledJaqProgram::compile(".value * 14").assured("the program is valid jaq");

    let answered = scaled
        .run_single(json!({"value": 0.1}))
        .assured("a product of two numbers is a number");

    assert_same_numbers(&answered, &json!(0.1_f64 * 14.0));
}

#[test]
fn a_text_format_reads_a_decimal_as_the_float_nearest_to_it() {
    let payloads: [(JaqNativeFormat, &[u8]); 3] = [
        (JaqNativeFormat::Json, br#"{"value": 1.4000000000000001}"#),
        (JaqNativeFormat::Yaml, b"value: 1.4000000000000001\n"),
        (JaqNativeFormat::Toml, b"value = 1.4000000000000001\n"),
    ];
    let identity = identity();
    for (format, payload) in payloads {
        let expected = json!({"value": 1.4000000000000001_f64});

        let single = format
            .read_single_value(payload)
            .assured("the payload is one value of its format");
        let through_identity = read_through_identity(format, &identity, payload.to_vec());

        assert_same_numbers(&single, &expected);
        assert_same_numbers(&through_identity, &expected);
    }
}

#[test]
fn a_binary_format_reads_a_float_with_its_bits() {
    let expected = json!({"value": 1.4000000000000001_f64});
    let mut payload = Vec::new();
    ciborium::into_writer(&expected, &mut payload).assured("a JSON value is a CBOR item");

    let single = JaqNativeFormat::Cbor
        .read_single_value(&payload)
        .assured("the payload is one CBOR item");
    let through_identity = read_through_identity(JaqNativeFormat::Cbor, &identity(), payload);

    assert_same_numbers(&single, &expected);
    assert_same_numbers(&through_identity, &expected);
}

/// Every number keeps its value through a program and through each native format that holds
/// numbers: a finite float of any bit pattern, both zeros included, keeps its bits, and a 64-bit
/// integer keeps its value and signedness, whether the identity program answers with it, a format
/// writes it and reads it back, or a payload is read and handed to the identity program as a codec
/// hands it to its ingestion program.
#[test]
fn bolero_programs_and_native_formats_keep_every_number() {
    let identity = identity();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .with_type::<(u8, Vec<(u8, u64)>)>()
        .for_each(|(format, drawn)| {
            let format = NUMERIC_FORMATS[usize::from(*format) % NUMERIC_FORMATS.len()];
            let mut members = JsonMap::new();
            let mut elements = Vec::new();
            for (position, (selector, bits)) in drawn.iter().take(NUMBERS).enumerate() {
                let number = drawn_number(format, *selector, *bits);
                members.insert(format!("n{position}"), number.clone());
                elements.push(number);
            }
            members.insert(String::from("list"), JsonValue::Array(elements));
            let value = JsonValue::Object(members);

            let answered = identity
                .run_single(value.clone())
                .assured("the identity program answers every JSON value");
            assert_same_numbers(&answered, &value);

            let payload = format
                .write_value(value.clone())
                .assured("a format writes every number of its domain");
            let single = format
                .read_single_value(&payload)
                .assured("a format reads the payload it wrote");
            assert_same_numbers(&single, &value);

            let through_identity = read_through_identity(format, &identity, payload);
            assert_same_numbers(&through_identity, &value);
        });
}
