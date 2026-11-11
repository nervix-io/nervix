//! Layer: test harness.
//! Owns: complete inference descriptor/value oracles and bounded malformed current inputs.
//! Depends on: the production archive contract and vocabulary generators.
//! Must not know: ONNX execution, runtime publications or restore placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "verifies the inference archive contract")
)]

use meticulous::ResultExt as _;

use crate::{ArchiveRecord, InferencerStateDescriptor, SectionDigester, archive_values::Values};

pub(super) fn generated(values: &mut Values<'_>) -> (InferencerStateDescriptor, Vec<u8>) {
    let branched = values.0.entropy().flag();
    let wasm = crate::wasm_properties::Descriptors(values.0.clone()).descriptor(branched);
    let shapes = vec![
        vec![2, 3],
        vec![],
        vec![1, u64::from(values.0.entropy().byte() % 4) + 1],
    ];
    let mut raw = Vec::new();
    for bits in [
        0u32,
        0x8000_0000,
        0x7f80_0000,
        0xff80_0000,
        0x7fc0_1234,
        u32::from_le_bytes(values.digest()[..4].try_into().assured("four digest bytes")),
    ] {
        raw.extend_from_slice(&bits.to_le_bytes());
    }
    raw.extend_from_slice(&values.digest()[..4]);
    for _ in 0..shapes[2][1] {
        raw.extend_from_slice(&values.digest()[..4]);
    }
    (
        InferencerStateDescriptor {
            domain: wasm.domain,
            entity: wasm.entity,
            unit: values.unit(),
            schema: wasm.schema,
            model: nervix_models::InferencerModelFingerprint::from_digest(values.digest()),
            branch_fingerprint: wasm.branch_fingerprint,
            branch: wasm.branch,
            incarnation: values.0.positive_u64().get(),
            revision: values.0.entropy().any_u64(),
            shapes,
            byte_length: u64::try_from(raw.len()).assured("the fixture is bounded"),
            digest: SectionDigester::digest_of(&raw),
        },
        raw,
    )
}

fn assert_values(descriptor: &InferencerStateDescriptor, raw: &[u8]) {
    let record = descriptor
        .encode()
        .assured("the current descriptor encodes");
    let decoded = InferencerStateDescriptor::decode("descriptor.rkyv", &record)
        .assured("the descriptor validates");
    assert_eq!(decoded, *descriptor);
    let tensors =
        crate::read_tensor_values("state.bin", &mut &raw[..], &decoded, descriptor.byte_length)
            .assured("the exact raw values decode");
    let mut encoded = Vec::new();
    for (values, shape) in tensors.iter().zip(&decoded.shapes) {
        let count: u64 = shape.iter().product();
        assert_eq!(
            u64::try_from(values.len()).assured("the fixture fits"),
            count
        );
        crate::write_tensor_values(&mut encoded, values).assured("values encode");
    }
    assert_eq!(encoded, raw, "all bits and declaration order survive");
    assert_eq!(SectionDigester::digest_of(&encoded), descriptor.digest);
}

#[test]
fn descriptor_and_values_preserve_special_float_bits_and_scalar_shapes() {
    for seed in [&[][..], &[0][..], &[255; 64][..]] {
        let (descriptor, raw) = generated(&mut Values::new(seed));
        assert_values(&descriptor, &raw);
        assert_eq!(
            &raw[..20],
            &[
                0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 128, 127, 0, 0, 128, 255, 52, 18, 192, 127
            ]
        );
    }
}

#[test]
fn tensor_values_preserve_order_across_encoding_block_boundaries() {
    for length in [16_383usize, 16_384, 16_385] {
        let pattern = [0u32, 0x8000_0000, 0x7fc0_1234, 0x7f80_0000, 0xff80_0000];
        let raw = (0..length)
            .flat_map(|index| pattern[index % pattern.len()].to_le_bytes())
            .collect::<Vec<_>>();
        let (mut descriptor, _) = generated(&mut Values::new(&[]));
        descriptor.shapes = vec![vec![
            u64::try_from(length).assured("a bounded test shape fits"),
        ]];
        descriptor.byte_length = u64::try_from(raw.len()).assured("the raw fixture is bounded");
        descriptor.digest = SectionDigester::digest_of(&raw);
        assert_values(&descriptor, &raw);
    }
}

#[test]
fn bolero_inferencer_descriptors_and_raw_tensors_preserve_every_value() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .for_each(|bytes| {
            let (descriptor, raw) = generated(&mut Values::new(bytes));
            assert_values(&descriptor, &raw);
        });
}

#[test]
fn bolero_inferencer_descriptors_and_raw_tensors_refuse_malformed_inputs() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .for_each(|bytes| {
            let (descriptor, raw) = generated(&mut Values::new(bytes));
            for mutation in 0..7 {
                let mut changed = descriptor.clone();
                match mutation {
                    0 => changed.incarnation = 0,
                    1 => changed.byte_length ^= 1,
                    2 => changed.shapes[0][0] = 0,
                    3 => changed.shapes[0] = vec![u64::MAX, 2],
                    4 => {
                        if changed.branch.is_branched() {
                            changed.branch_fingerprint = None;
                        } else {
                            changed.branch_fingerprint =
                                Some(nervix_models::BranchKeyFingerprint::new([0; 32]));
                        }
                    }
                    5 => changed.shapes.clear(),
                    _ => {
                        changed.branch = nervix_models::BranchKeyFields::Direct(
                            vec![crate::StateField {
                                name: "invalid field".into(),
                                value: crate::StateValue::U8(0),
                            }]
                            .into(),
                        );
                        changed.branch_fingerprint =
                            Some(nervix_models::BranchKeyFingerprint::new([0; 32]));
                    }
                }
                let encoded = changed
                    .encode()
                    .assured("a bounded malformed current record encodes");
                assert!(InferencerStateDescriptor::decode("descriptor.rkyv", &encoded).is_err());
            }
            assert!(
                crate::read_tensor_values(
                    "state.bin",
                    &mut &raw[..raw.len() - 1],
                    &descriptor,
                    descriptor.byte_length
                )
                .is_err()
            );
            let mut extra = raw.clone();
            extra.push(0);
            assert!(
                crate::read_tensor_values(
                    "state.bin",
                    &mut extra.as_slice(),
                    &descriptor,
                    descriptor.byte_length
                )
                .is_err()
            );
            assert!(
                crate::read_tensor_values("state.bin", &mut raw.as_slice(), &descriptor, 0)
                    .is_err()
            );
            if let Ok(decoded) = InferencerStateDescriptor::decode("descriptor.rkyv", bytes) {
                crate::archive_properties::assert_record(&decoded);
            }
        });
}
