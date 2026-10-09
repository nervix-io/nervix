//! Full current WASM archive descriptors and typed branch-field conversions.
//!
//! Layer: test harness.
//! - **Owns.** Bounded descriptor cases, exact bit equality and separate invalid descriptor inputs.
//! - **Depends on.** Production archive records, their current wire types and vocabulary generators.
//! - **Must not know.** Guest callbacks, live ACKs or checkpoint capture/publication protocols.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{BranchKeyFingerprint, SchemaFingerprint, WasmStateGeneration};

use crate::{
    ArchiveReadError, ArchiveRecord,
    section::{decode_record, encode_record},
    state::WasmStateDescriptor,
    wire::{StateField, StateValue, WasmStateDescriptorWire},
};

pub(super) struct Descriptors<'a>(pub(super) Arbitrary<'a>);

impl Descriptors<'_> {
    fn scalar(&mut self, kind: u8) -> StateValue {
        let bytes = std::array::from_fn(|_| self.0.entropy().byte());
        match kind {
            0 => StateValue::U8(bytes[0]),
            1 => StateValue::I8(i8::from_le_bytes([bytes[0]])),
            2 => StateValue::U16(u16::from_le_bytes([bytes[0], bytes[1]])),
            3 => StateValue::I16(i16::from_le_bytes([bytes[0], bytes[1]])),
            4 => StateValue::U32(u32::from_le_bytes(
                bytes[..4]
                    .try_into()
                    .assured("four bytes of an eight-byte array"),
            )),
            5 => StateValue::I32(i32::from_le_bytes(
                bytes[..4]
                    .try_into()
                    .assured("four bytes of an eight-byte array"),
            )),
            6 => StateValue::U64(self.0.entropy().any_u64()),
            7 => StateValue::I64(self.0.entropy().any_i64()),
            8 => StateValue::Bool(self.0.entropy().flag()),
            9 => StateValue::String(self.0.string()),
            10 => StateValue::Datetime(
                self.0
                    .entropy()
                    .pick([
                        "1970-01-01T00:00:00+00:00",
                        "1969-12-31T23:59:59.999999999+00:00",
                    ])
                    .into(),
            ),
            11 => StateValue::F32Bits(u32::from_le_bytes(
                bytes[..4]
                    .try_into()
                    .assured("four bytes of an eight-byte array"),
            )),
            _ => StateValue::F64Bits(u64::from_le_bytes(bytes)),
        }
    }

    pub(super) fn descriptor(&mut self, branched: bool) -> WasmStateDescriptor {
        let mut fields = Vec::new();
        for kind in 0..13 {
            let scalar = self.scalar(kind);
            let value = if self.0.entropy().flag() {
                StateValue::Vec(vec![StateValue::Array(vec![scalar.clone(), scalar])])
            } else {
                scalar
            };
            fields.push(StateField {
                name: format!("field_{kind:02}"),
                value,
            });
        }
        // Float payload extrema are deliberate, including signed zero and NaN bits.
        fields.push(StateField {
            name: "float_bits".into(),
            value: StateValue::Vec(vec![
                StateValue::F64Bits(0x8000_0000_0000_0000),
                StateValue::F64Bits(0x7ff8_0000_0000_1234),
            ]),
        });
        fields.push(StateField {
            name: "zero_length".into(),
            value: StateValue::Vec(vec![]),
        });
        for field in &fields {
            assert_eq!(StateField::from_remote(field.clone().into_remote()), *field);
        }
        let branch = if branched { Some(fields) } else { None };
        let branch_fingerprint = if branched {
            Some(BranchKeyFingerprint::new(std::array::from_fn(|_| {
                self.0.entropy().byte()
            })))
        } else {
            None
        };
        WasmStateDescriptor {
            domain: self.0.name(),
            entity: self.0.name(),
            schema: SchemaFingerprint::from_digest(std::array::from_fn(|_| {
                self.0.entropy().byte()
            })),
            branch_fingerprint,
            branch,
            generation: WasmStateGeneration::try_from(self.0.positive_u64().get())
                .assured("a positive generation is valid"),
            revision: self.0.entropy().any_u64(),
        }
    }
}

#[test]
fn bolero_wasm_descriptors_preserve_every_identity_generation_and_branch_value() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = Descriptors(Arbitrary::new(bytes, Domain::Vocabulary));
            for branched in [false, true] {
                let original = values.descriptor(branched);
                let encoded = original.encode().assured("a valid descriptor encodes");
                let decoded = WasmStateDescriptor::decode("state/wasm.rkyv", &encoded)
                    .assured("a valid descriptor decodes");
                assert_eq!(decoded, original);
                assert_eq!(
                    decoded.encode().assured("the decoded descriptor encodes"),
                    encoded
                );
            }
        });
}

#[test]
fn bolero_malformed_wasm_descriptors_fail_at_the_owning_boundary() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes| {
            let mut values = Descriptors(Arbitrary::new(bytes, Domain::Vocabulary));
            let descriptor = values.descriptor(true);
            let encoded = descriptor
                .encode()
                .assured("the starting descriptor is valid");
            for defect in 0..8 {
                let mut wire: WasmStateDescriptorWire = decode_record(
                    "state/wasm.rkyv",
                    WasmStateDescriptor::KIND,
                    WasmStateDescriptor::VERSION,
                    &encoded,
                )
                .assured("the current wire verifies");
                let field = match defect {
                    0 => {
                        wire.generation = 0;
                        "WASM state generation"
                    }
                    1 => {
                        wire.branch = Some(vec![]);
                        "branch key"
                    }
                    2 => {
                        wire.branch = None;
                        "WASM branch fingerprint"
                    }
                    3 => {
                        wire.branch_fingerprint = None;
                        "WASM branch fingerprint"
                    }
                    4 => {
                        wire.branch.as_mut().assured("the descriptor is branched")[0]
                            .name
                            .clear();
                        "branch field name"
                    }
                    5 => {
                        wire.branch
                            .as_mut()
                            .assured("the descriptor is branched")
                            .reverse();
                        "branch field order"
                    }
                    6 => {
                        wire.domain.clear();
                        "domain name"
                    }
                    _ => {
                        wire.entity.clear();
                        "state entity name"
                    }
                };
                let invalid = encode_record(
                    WasmStateDescriptor::KIND,
                    WasmStateDescriptor::VERSION,
                    &wire,
                )
                .assured("a bounded current wire record encodes");
                crate::malformed_properties::assert_decode_frees_allocations(|| {
                    WasmStateDescriptor::decode("state/wasm.rkyv", &invalid)
                });
                let error = WasmStateDescriptor::decode("state/wasm.rkyv", &invalid)
                    .expect_err("the malformed field must fail");
                assert_eq!(
                    error.current_context(),
                    &ArchiveReadError::InvalidValue {
                        path: "state/wasm.rkyv".into(),
                        field
                    }
                );
            }
            let mut framed = encoded[..crate::section::RECORD_HEADER_BYTES].to_vec();
            framed.extend_from_slice(bytes);
            for input in [bytes, framed.as_slice(), &encoded[..encoded.len() / 2]] {
                crate::malformed_properties::assert_decode_frees_allocations(|| {
                    WasmStateDescriptor::decode("state/wasm.rkyv", input)
                });
                match WasmStateDescriptor::decode("state/wasm.rkyv", input) {
                    Ok(decoded) => {
                        let reencoded = decoded
                            .encode()
                            .assured("a bounded decoded descriptor encodes");
                        assert_eq!(
                            WasmStateDescriptor::decode("state/wasm.rkyv", &reencoded)
                                .assured("the accepted descriptor decodes"),
                            decoded
                        );
                    }
                    Err(error) => assert!(matches!(
                        error.current_context(),
                        ArchiveReadError::ForeignMagic { .. }
                            | ArchiveReadError::ForeignRecordKind { .. }
                            | ArchiveReadError::UnsupportedRecordVersion { .. }
                            | ArchiveReadError::InvalidRecord { .. }
                            | ArchiveReadError::InvalidValue { .. }
                            | ArchiveReadError::SectionTooLarge { .. }
                    )),
                }
            }
        });
}
