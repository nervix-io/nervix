//! Bounded peer branch keys and their complete, float-bit-sensitive test oracle.
//!
//! Layer: test harness.
//! - **Owns.** Valid remote branch fields, bounded nested values and exact comparisons of them.
//! - **Depends on.** Current vocabulary values and the shared entropy cursor.
//! - **Must not know.** The wire encoder, runtime branch validation or checkpoint storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "generates and compares current remote branch fields"
    )
)]

use meticulous::OptionExt as _;
use nervix_models::{
    RemoteRuntimeElementValue as Element, RemoteRuntimeField, RemoteRuntimeValue as Value,
};

use crate::{Arbitrary, Entropy};

impl Arbitrary<'_> {
    /// An absent key, or one to three distinct, canonically ordered fields. Values nest at most
    /// twice with at most three elements per collection. Datetimes are valid RFC 3339 and every
    /// float is finite, including signed zeros, so runtime branch validation can read every key.
    pub fn remote_branch_key(&mut self) -> Option<Vec<RemoteRuntimeField>> {
        if !self.entropy().flag() {
            return None;
        }
        let count = self.entropy().count(2) + 1;
        let mut fields = Vec::with_capacity(count);
        for index in 0..count {
            let value = self.remote_branch_value(2);
            fields.push(RemoteRuntimeField {
                name: format!("key_{index}"),
                value,
            });
        }
        Some(fields)
    }

    fn remote_branch_value(&mut self, depth: u8) -> Value {
        match self.remote_branch_element(depth) {
            Element::U8(v) => Value::U8(v),
            Element::I8(v) => Value::I8(v),
            Element::U16(v) => Value::U16(v),
            Element::I16(v) => Value::I16(v),
            Element::U32(v) => Value::U32(v),
            Element::I32(v) => Value::I32(v),
            Element::U64(v) => Value::U64(v),
            Element::I64(v) => Value::I64(v),
            Element::Bool(v) => Value::Bool(v),
            Element::String(v) => Value::String(v),
            Element::Datetime(v) => Value::Datetime(v),
            Element::F32(v) => Value::F32(v),
            Element::F64(v) => Value::F64(v),
            Element::Array(v) => Value::Array(v),
            Element::Vec(v) => Value::Vec(v),
        }
    }

    fn remote_branch_element(&mut self, depth: u8) -> Element {
        let variants = if depth == 0 { 13 } else { 15 };
        match self.entropy().byte() % variants {
            0 => Element::U8(self.entropy().byte()),
            1 => Element::I8(i8::from_le_bytes([self.entropy().byte()])),
            2 => Element::U16(u16::from_le_bytes(bytes(self.entropy()))),
            3 => Element::I16(i16::from_le_bytes(bytes(self.entropy()))),
            4 => Element::U32(u32::from_le_bytes(bytes(self.entropy()))),
            5 => Element::I32(i32::from_le_bytes(bytes(self.entropy()))),
            6 => Element::U64(self.entropy().any_u64()),
            7 => Element::I64(self.entropy().any_i64()),
            8 => Element::Bool(self.entropy().flag()),
            9 => Element::String(self.string()),
            10 => Element::Datetime(self.entropy().pick([
                "1970-01-01T00:00:00+00:00".to_string(),
                "2026-10-13T12:34:56.123456789-03:30".to_string(),
                "0001-01-01T00:00:00+00:00".to_string(),
                "9999-12-31T23:59:59+00:00".to_string(),
            ])),
            11 => {
                let bits = u32::from_le_bytes(bytes(self.entropy()));
                let drawn = f32::from_bits(bits);
                let finite = if drawn.is_finite() { drawn } else { -0.0 };
                Element::F32(self.entropy().pick([
                    0.0,
                    -0.0,
                    f32::MIN,
                    f32::MAX,
                    f32::from_bits(1),
                    finite,
                ]))
            }
            12 => {
                let drawn = f64::from_bits(self.entropy().any_u64());
                let finite = if drawn.is_finite() { drawn } else { -0.0 };
                Element::F64(self.entropy().pick([
                    0.0,
                    -0.0,
                    f64::MIN,
                    f64::MAX,
                    f64::from_bits(1),
                    finite,
                ]))
            }
            variant => {
                let below = depth
                    .checked_sub(1)
                    .assured("collections are drawn only above depth zero");
                let count = self.entropy().count(3);
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.remote_branch_element(below));
                }
                if variant == 13 {
                    Element::Array(values)
                } else {
                    Element::Vec(values)
                }
            }
        }
    }
}

fn bytes<const N: usize>(entropy: &mut Entropy<'_>) -> [u8; N] {
    std::array::from_fn(|_| entropy.byte())
}

/// Compare complete keys, preserving absence, field and collection order, names, variants and
/// every float bit. Unlike ordinary floating equality, this distinguishes both signed zeros and
/// NaN payloads, including floats nested in arrays and vectors.
pub fn assert_remote_keys_eq(
    actual: &Option<Vec<RemoteRuntimeField>>,
    expected: &Option<Vec<RemoteRuntimeField>>,
) {
    match (actual, expected) {
        (None, None) => {}
        (Some(actual), Some(expected)) => {
            assert_eq!(actual.len(), expected.len());
            for (actual, expected) in actual.iter().zip(expected) {
                assert_eq!(actual.name, expected.name);
                match (&actual.value, &expected.value) {
                    (Value::F32(a), Value::F32(b)) => assert_eq!(a.to_bits(), b.to_bits()),
                    (Value::F64(a), Value::F64(b)) => assert_eq!(a.to_bits(), b.to_bits()),
                    (Value::Array(a), Value::Array(b)) | (Value::Vec(a), Value::Vec(b)) => {
                        assert_elements_eq(a, b)
                    }
                    (a, b) => assert_eq!(a, b),
                }
            }
        }
        (actual, expected) => panic!("branch presence changed: {actual:?} != {expected:?}"),
    }
}

fn assert_elements_eq(actual: &[Element], expected: &[Element]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        match (actual, expected) {
            (Element::F32(a), Element::F32(b)) => assert_eq!(a.to_bits(), b.to_bits()),
            (Element::F64(a), Element::F64(b)) => assert_eq!(a.to_bits(), b.to_bits()),
            (Element::Array(a), Element::Array(b)) | (Element::Vec(a), Element::Vec(b)) => {
                assert_elements_eq(a, b)
            }
            (a, b) => assert_eq!(a, b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: Value) -> Option<Vec<RemoteRuntimeField>> {
        Some(vec![RemoteRuntimeField {
            name: "value".to_string(),
            value,
        }])
    }

    #[test]
    #[should_panic]
    fn the_oracle_distinguishes_signed_scalar_zero() {
        assert_remote_keys_eq(&key(Value::F32(0.0)), &key(Value::F32(-0.0)));
    }

    #[test]
    #[should_panic]
    fn the_oracle_distinguishes_nested_signed_zero() {
        let nested = |v| key(Value::Array(vec![Element::Vec(vec![Element::F64(v)])]));
        assert_remote_keys_eq(&nested(0.0), &nested(-0.0));
    }

    #[test]
    fn the_oracle_preserves_nan_payloads() {
        let fields = key(Value::Array(vec![
            Element::F32(f32::from_bits(0x7fc0_0001)),
            Element::F64(f64::from_bits(0x7ff8_0000_0000_0001)),
        ]));
        assert_remote_keys_eq(&fields, &fields);
    }
}
