//! Complete protobuf signaling values and damaged frames through the production conversion.
//!
//! Layer: test harness.
//! - **Owns.** A bounded lossless descriptor domain, exact scalar/list oracles and wire damage.
//! - **Depends on.** The signaling conversion, protobuf descriptors and the shared byte generator.
//! - **Must not know.** Connection scheduling, graph routing or source intake.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the conversion harness declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::Entropy;
use prost_reflect::{
    DescriptorPool, MessageDescriptor,
    prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::{Label, Type},
    },
};
use serde_json::{Map, Value, json};

use super::{SignalingProtobufError, decode_protobuf_payload, encode_protobuf_payload};

const LIST_LIMIT: usize = 8;
const TEXT_LIMIT: usize = 64;

struct SignalingFixture {
    descriptor: MessageDescriptor,
}

impl SignalingFixture {
    fn new() -> Self {
        macro_rules! field {
            ($name:literal, $number:literal, $kind:ident, $label:ident) => {
                FieldDescriptorProto {
                    name: Some($name.into()),
                    json_name: Some($name.into()),
                    number: Some($number),
                    label: Some(Label::$label.into()),
                    r#type: Some(Type::$kind.into()),
                    ..Default::default()
                }
            };
        }
        // Explicit scalar presence keeps both signed zeros and explicitly supplied defaults.
        // Empty repeated fields have no presence, so the generated JSON omits those fields.
        let file = FileDescriptorProto {
            name: Some("signaling_values.proto".into()),
            package: Some("nervix.test".into()),
            syntax: Some("proto2".into()),
            message_type: vec![DescriptorProto {
                name: Some("SignalingValues".into()),
                field: vec![
                    field!("double_value", 1, Double, Optional),
                    field!("float_value", 2, Float, Optional),
                    field!("signed_value", 3, Int64, Optional),
                    field!("unsigned_value", 4, Uint64, Optional),
                    field!("integer_value", 5, Int32, Optional),
                    field!("flag", 6, Bool, Optional),
                    field!("text", 7, String, Optional),
                    field!("doubles", 8, Double, Repeated),
                    field!("floats", 9, Float, Repeated),
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .assured("the fixture declares distinct numbered fields of supported protobuf types");
        let descriptor = pool
            .get_message_by_name("nervix.test.SignalingValues")
            .assured("the fixture declares this message");
        Self { descriptor }
    }

    fn assert_round_trip(&self, input: &Value) {
        let encoded = encode_protobuf_payload(&self.descriptor, input)
            .assured("the generator supplies finite values of the descriptor's types");
        let decoded = decode_protobuf_payload(&self.descriptor, &encoded)
            .assured("the production encoder wrote a complete message of this descriptor");
        let expected = input
            .as_object()
            .assured("the generator constructs an object");
        let actual = decoded
            .as_object()
            .assured("a message serializes as an object");
        assert_eq!(
            actual.len(),
            expected.len(),
            "every field and its presence are kept"
        );
        for (name, expected) in expected {
            let Some(actual) = actual.get(name) else {
                panic!("the decoded message lost field {name}");
            };
            match name.as_str() {
                "double_value" => assert_eq!(
                    actual
                        .as_f64()
                        .assured("the descriptor field is a double")
                        .to_bits(),
                    expected
                        .as_f64()
                        .assured("the generated field is a double")
                        .to_bits(),
                    "{name}"
                ),
                "float_value" => Self::assert_float(actual, expected),
                "doubles" | "floats" => {
                    let actual = actual
                        .as_array()
                        .assured("the descriptor field is repeated");
                    let expected = expected
                        .as_array()
                        .assured("the generated field is repeated");
                    assert_eq!(actual.len(), expected.len(), "{name} keeps every element");
                    for (actual, expected) in actual.iter().zip(expected) {
                        if name == "doubles" {
                            assert_eq!(
                                actual.as_f64().assured("the element is a double").to_bits(),
                                expected
                                    .as_f64()
                                    .assured("the element is a double")
                                    .to_bits(),
                                "{name}"
                            );
                        } else {
                            Self::assert_float(actual, expected);
                        }
                    }
                }
                _ => assert_eq!(actual, expected, "{name} keeps its complete value"),
            }
        }
    }

    fn assert_float(actual: &Value, expected: &Value) {
        // Protobuf's JSON writer prints an f32's decimal, not its exact widening to f64.
        // Read it as the descriptor's f32 and compare the original 32 bits.
        let actual: f32 = serde_json::from_value(actual.clone())
            .assured("the descriptor's finite float has a numeric JSON representation");
        let expected: f32 = serde_json::from_value(expected.clone())
            .assured("the generated value is a finite float");
        assert_eq!(actual.to_bits(), expected.to_bits());
    }

    fn assert_damaged(&self, payload: &[u8], damage: &str) {
        let Err(error) = decode_protobuf_payload(&self.descriptor, payload) else {
            panic!("{damage} must be refused: {payload:?}");
        };
        let SignalingProtobufError::Decode { message, .. } = error.current_context() else {
            panic!("{damage} must fail at the wire decoder: {error:#}");
        };
        assert_eq!(message, self.descriptor.full_name());
    }
}

struct SignalingCase {
    presence: u8,
    double: f64,
    float: f32,
    signed: i64,
    unsigned: u64,
    integer: i32,
    flag: bool,
    text: String,
    doubles: Vec<f64>,
    floats: Vec<f32>,
}

impl SignalingCase {
    fn draw(bytes: &[u8]) -> Self {
        let mut entropy = Entropy::new(bytes);
        let presence = entropy.byte();
        let double = Self::double(&mut entropy);
        let float = Self::float(&mut entropy);
        let signed = entropy.any_i64();
        let unsigned = entropy.any_u64();
        let integer_bits = u32::try_from(entropy.up_to(u64::from(u32::MAX)))
            .verified("the choice is bounded by u32::MAX");
        let integer = integer_bits.cast_signed();
        let flag = entropy.flag();
        let text_len = entropy.count(TEXT_LIMIT);
        let text_bytes: Vec<u8> = (0..text_len).map(|_| entropy.byte()).collect();
        let text = String::from_utf8_lossy(&text_bytes).into_owned();
        let doubles_len = entropy.count(LIST_LIMIT);
        let doubles = (0..doubles_len)
            .map(|_| Self::double(&mut entropy))
            .collect();
        let floats_len = entropy.count(LIST_LIMIT);
        let floats = (0..floats_len).map(|_| Self::float(&mut entropy)).collect();
        Self {
            presence,
            double,
            float,
            signed,
            unsigned,
            integer,
            flag,
            text,
            doubles,
            floats,
        }
    }

    fn double(entropy: &mut Entropy<'_>) -> f64 {
        if entropy.flag() {
            return entropy.pick([
                0.0,
                -0.0,
                f64::MAX,
                f64::MIN,
                f64::MIN_POSITIVE,
                f64::from_bits(1),
                1.4000000000000001,
                90.33333333333333,
            ]);
        }
        let bits = entropy.up_to(u64::MAX);
        let value = f64::from_bits(bits);
        if value.is_finite() {
            value
        } else {
            f64::from_bits(bits & !0x4000_0000_0000_0000)
        }
    }

    fn float(entropy: &mut Entropy<'_>) -> f32 {
        if entropy.flag() {
            return entropy.pick([
                0.0,
                -0.0,
                f32::MAX,
                f32::MIN,
                f32::MIN_POSITIVE,
                f32::from_bits(1),
                0.1,
                1.1,
            ]);
        }
        let bits = u32::try_from(entropy.up_to(u64::from(u32::MAX)))
            .verified("the choice is bounded by u32::MAX");
        let value = f32::from_bits(bits);
        if value.is_finite() {
            value
        } else {
            f32::from_bits(bits & !0x4000_0000)
        }
    }

    fn json(&self) -> Value {
        let mut fields = Map::new();
        if self.presence & 1 != 0 {
            fields.insert("double_value".into(), json!(self.double));
        }
        if self.presence & 2 != 0 {
            fields.insert("float_value".into(), json!(self.float));
        }
        if self.presence & 4 != 0 {
            fields.insert("signed_value".into(), json!(self.signed));
        }
        if self.presence & 8 != 0 {
            fields.insert("unsigned_value".into(), json!(self.unsigned));
        }
        if self.presence & 16 != 0 {
            fields.insert("integer_value".into(), json!(self.integer));
        }
        if self.presence & 32 != 0 {
            fields.insert("flag".into(), json!(self.flag));
        }
        if self.presence & 64 != 0 {
            fields.insert("text".into(), json!(self.text));
        }
        if !self.doubles.is_empty() {
            fields.insert("doubles".into(), json!(self.doubles));
        }
        if !self.floats.is_empty() {
            fields.insert("floats".into(), json!(self.floats));
        }
        Value::Object(fields)
    }
}

#[test]
fn bolero_protobuf_signaling_preserves_every_value() {
    let fixture = SignalingFixture::new();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            let case = SignalingCase::draw(bytes);
            fixture.assert_round_trip(&case.json());
        });
}

#[test]
fn bolero_damaged_protobuf_signaling_frames_are_refused() {
    let fixture = SignalingFixture::new();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            let mut zero_field = vec![0];
            zero_field.extend_from_slice(bytes);
            fixture.assert_damaged(&zero_field, "field number zero");
            fixture.assert_damaged(&[0x0f], "wire type seven");
            for (tag, width) in [(0x09, 8), (0x15, 4)] {
                let length = bytes.len() % width;
                let mut truncated = vec![tag];
                truncated.extend_from_slice(&bytes[..length]);
                fixture.assert_damaged(&truncated, "truncated fixed-width field");
            }
            let mut varint = vec![0x18];
            varint.extend_from_slice(&[0x80; 11]);
            fixture.assert_damaged(&varint, "unterminated oversized varint");
            let length = bytes.len().min(TEXT_LIMIT);
            let declared =
                u8::try_from(length + 1).assured("at most 65 bytes fit in a one-byte varint");
            let mut text = vec![0x3a, declared];
            text.extend_from_slice(&bytes[..length]);
            fixture.assert_damaged(&text, "string longer than its payload");
            for (tag, width) in [(0x42, 8), (0x4a, 4)] {
                let length = bytes.len() % (width - 1) + 1;
                let encoded =
                    u8::try_from(length).assured("at most seven bytes fit in a one-byte varint");
                let mut packed = vec![tag, encoded];
                packed.resize(length + 2, 0);
                fixture.assert_damaged(&packed, "packed float with an incomplete element");
            }
        });
}
