//! P-256 known answers, cross-build signatures, secret ownership and bounded sign/verify
//! properties.

use meticulous::{OptionExt as _, ResultExt as _};

use crate::{
    ecdsa::{EcdsaP256KeyPair, EcdsaP256Signature, verify},
    provider::{ModuleMode, module_mode},
};

#[test]
fn nist_cavp_p256_sha256_verification_vectors() {
    let mut message = Vec::new();
    let mut public_key = vec![4];
    let mut signature = Vec::new();
    let mut checked = 0;
    for line in include_str!("test_data/p256_sha256_sigver.rsp").lines() {
        let Some((field, value)) = line.split_once(" = ") else {
            continue;
        };
        match field {
            "Msg" => message = hex(value),
            "Qx" => {
                public_key = vec![4];
                public_key.extend(hex(value));
            }
            "Qy" => public_key.extend(hex(value)),
            "R" => signature = hex(value),
            "S" => signature.extend(hex(value)),
            "Result" => {
                assert_eq!(
                    verify(&public_key, &message, &signature).is_ok(),
                    value.starts_with('P')
                );
                checked += 1;
            }
            _ => panic!("unexpected CAVP field: {field}"),
        }
    }
    assert_eq!(checked, 15);
}

fn hex(value: &str) -> Vec<u8> {
    let mut bytes = vec![0; value.len() / 2];
    faster_hex::hex_decode(value.as_bytes(), &mut bytes)
        .assured("committed CAVP and signature fixtures are hexadecimal");
    bytes
}

#[test]
fn generated_key_round_trips_through_pkcs8_and_exposes_coordinates() {
    let document = EcdsaP256KeyPair::generate_pkcs8().assured("AWS-LC generates a P-256 key");
    let key = EcdsaP256KeyPair::from_pkcs8(document.as_ref()).assured("generated PKCS#8 imports");
    let exported = key.to_pkcs8().assured("the key exports as PKCS#8");
    assert_eq!(exported.as_ref(), document.as_ref());
    let restored =
        EcdsaP256KeyPair::from_pkcs8(exported.as_ref()).assured("exported PKCS#8 imports");
    assert_eq!(key.public_key(), restored.public_key());
    let coordinates = key.public_coordinates();
    assert_eq!(key.public_key()[0], 4);
    assert_eq!(&key.public_key()[1..33], &coordinates.x);
    assert_eq!(&key.public_key()[33..], &coordinates.y);
    assert_eq!(format!("{key:?}"), "EcdsaP256KeyPair { .. }");
    assert_eq!(format!("{document:?}"), "Pkcs8PrivateKey { .. }");
    assert_sign_verify(&key, b"complete message");
    assert_sign_verify(&restored, b"");
}

/// The message the cross-build signatures sign (licensing specification §17.6).
const PORTABLE_MESSAGE: &[u8] = b"Nervix P-256 cross-build signature fixture";

/// Public test-only private key. Never use it outside these interoperability fixtures.
fn portable_key() -> EcdsaP256KeyPair {
    EcdsaP256KeyPair::from_pkcs8(include_bytes!("test_data/p256_test_key.pk8"))
        .assured("the committed test key imports")
}

/// The committed signature of [`PORTABLE_MESSAGE`] that `module`'s build produced. AWS-LC's ECDSA
/// signatures are randomized, so each build's signature is produced once and saved;
/// `just signature-vectors <build>` rewrites it through [`write_the_portable_signature`].
fn saved_signature(module: ModuleMode) -> Vec<u8> {
    let text = match module {
        ModuleMode::Ordinary => include_str!("test_data/p256_signature_ordinary.hex"),
        ModuleMode::Fips => include_str!("test_data/p256_signature_fips.hex"),
    };
    hex(text.trim_end())
}

fn saved_signature_path(module: ModuleMode) -> &'static str {
    match module {
        ModuleMode::Ordinary => concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/test_data/p256_signature_ordinary.hex"
        ),
        ModuleMode::Fips => concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/test_data/p256_signature_fips.hex"
        ),
    }
}

/// Each build verifies the signature the other build produced as well as its own: Nervix CI runs
/// this on the ordinary module, and the licensing service's FIPS lane on the FIPS module.
#[test]
fn saved_signatures_of_both_builds_verify_on_this_build() {
    let key = portable_key();
    assert_sign_verify(&key, PORTABLE_MESSAGE);
    let ordinary = saved_signature(ModuleMode::Ordinary);
    let fips = saved_signature(ModuleMode::Fips);
    assert_ne!(
        ordinary, fips,
        "randomized signatures differ when each build signs on its own"
    );
    for saved in [ordinary, fips] {
        verify(key.public_key(), PORTABLE_MESSAGE, &saved)
            .assured("a saved signature verifies on either module");
        let mut changed = saved.clone();
        let last = changed
            .last_mut()
            .assured("a saved signature holds its 64 bytes");
        *last ^= 1;
        assert!(verify(key.public_key(), PORTABLE_MESSAGE, &changed).is_err());
        let mut changed_message = PORTABLE_MESSAGE.to_vec();
        let first = changed_message
            .first_mut()
            .assured("the fixture message is not empty");
        *first ^= 1;
        assert!(verify(key.public_key(), &changed_message, &saved).is_err());
    }
}

/// Rewrites the linked module's saved signature. `just signature-vectors <build>` runs it.
#[test]
#[ignore = "writes src/test_data/p256_signature_<build>.hex for the linked AWS-LC module"]
fn write_the_portable_signature() {
    let key = portable_key();
    let signature = key
        .sign(PORTABLE_MESSAGE)
        .assured("AWS-LC signs the fixture message");
    verify(key.public_key(), PORTABLE_MESSAGE, signature.as_bytes())
        .assured("a signature verifies on the module that made it");
    let mut text = faster_hex::hex_string(signature.as_bytes());
    text.push('\n');
    std::fs::write(saved_signature_path(module_mode()), text)
        .assured("the test data directory is writable in a checkout");
}

#[test]
fn malformed_keys_and_signatures_keep_typed_causes() {
    use aws_lc_rs::error::{KeyRejected, Unspecified};

    use crate::ecdsa::EcdsaP256Error;

    let Err(error) = EcdsaP256KeyPair::from_pkcs8(b"invalid key") else {
        panic!("non-DER bytes must not import a key");
    };
    assert!(matches!(error.current_context(), EcdsaP256Error::ImportKey));
    assert!(error.contains::<KeyRejected>());
    let Err(error) = verify(&[0; 65], b"message", &[0; 64]) else {
        panic!("invalid points and signatures must not verify");
    };
    assert!(matches!(error.current_context(), EcdsaP256Error::Verify));
    assert!(error.contains::<Unspecified>());
    for length in [0, 1, 63, 65, 128] {
        assert!(verify(&[0; 65], b"message", &vec![0; length]).is_err());
    }
}

fn assert_sign_verify(key: &EcdsaP256KeyPair, message: &[u8]) {
    let signature = key.sign(message).assured("AWS-LC signs a bounded message");
    let bytes: &[u8; 64] = signature.as_bytes();
    assert_eq!(EcdsaP256Signature::from_bytes(*bytes), signature);
    verify(key.public_key(), message, bytes).assured("signature verifies against its public key");
    let mut changed = bytes.to_owned();
    changed[0] ^= 1;
    assert!(verify(key.public_key(), message, &changed).is_err());
    let mut changed_message = message.to_vec();
    if let Some(first) = changed_message.first_mut() {
        *first ^= 1;
    } else {
        changed_message.push(0);
    }
    assert!(verify(key.public_key(), &changed_message, bytes).is_err());
}

#[test]
fn a_different_curve_or_key_cannot_authenticate_a_message() {
    use aws_lc_rs::{
        rand::SystemRandom,
        signature::{ECDSA_P384_SHA384_FIXED_SIGNING, EcdsaKeyPair},
    };
    let p384 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P384_SHA384_FIXED_SIGNING, &SystemRandom::new())
        .assured("AWS-LC generates the wrong-curve test key");
    assert!(EcdsaP256KeyPair::from_pkcs8(p384.as_ref()).is_err());
    let document = EcdsaP256KeyPair::generate_pkcs8().assured("AWS-LC generates a P-256 key");
    let key = EcdsaP256KeyPair::from_pkcs8(document.as_ref()).assured("generated PKCS#8 imports");
    let other_document = EcdsaP256KeyPair::generate_pkcs8().assured("AWS-LC generates another key");
    let other_key =
        EcdsaP256KeyPair::from_pkcs8(other_document.as_ref()).assured("generated PKCS#8 imports");
    let signature = key
        .sign(b"message")
        .assured("AWS-LC signs the test message");
    assert!(verify(other_key.public_key(), b"message", signature.as_bytes()).is_err());
}

#[test]
fn digest_comparison_checks_every_position() {
    assert!(crate::digest::equal(&[0_u8; 0], &[0_u8; 0]));
    assert!(crate::digest::equal(&[7; 32], &[7; 32]));
    assert!(crate::digest::equal(&[7; 64], &[7; 64]));
    for position in 0..64 {
        let mut changed = [7; 64];
        changed[position] ^= 1;
        assert!(!crate::digest::equal(&[7; 64], &changed));
    }
}

#[test]
fn bolero_p256_signatures_bind_every_message_byte() {
    let document = EcdsaP256KeyPair::generate_pkcs8().assured("AWS-LC generates a P-256 key");
    let key = EcdsaP256KeyPair::from_pkcs8(document.as_ref()).assured("generated PKCS#8 imports");
    bolero::check!()
        .with_iterations(128)
        .with_max_len(4096)
        .for_each(|message| {
            assert_sign_verify(&key, message);
        });
}

#[test]
fn bolero_p256_pkcs8_preserves_the_complete_key() {
    use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};

    bolero::check!()
        .with_iterations(128)
        .with_max_len(32)
        .with_type::<[u8; 32]>()
        .for_each(|input| {
            // Test fixture only: RFC 5915 ECPrivateKey with a named P-256 curve. Clearing the
            // first scalar byte puts it below the order; setting the final bit makes it nonzero.
            let mut der = [0_u8; 51];
            der[..7].copy_from_slice(&[0x30, 0x31, 0x02, 0x01, 0x01, 0x04, 0x20]);
            der[7..39][..input.len()].copy_from_slice(input);
            der[7] = 0;
            der[38] |= 1;
            der[39..].copy_from_slice(&[
                0xa0, 0x0a, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07,
            ]);
            let reference =
                EcdsaKeyPair::from_private_key_der(&ECDSA_P256_SHA256_FIXED_SIGNING, &der)
                    .assured("the fixture scalar is nonzero and below the P-256 order");
            let original = reference
                .to_pkcs8v1()
                .assured("AWS-LC encodes the fixture key");
            let key =
                EcdsaP256KeyPair::from_pkcs8(original.as_ref()).assured("valid PKCS#8 imports");
            let exported = key.to_pkcs8().assured("valid key exports");
            assert_eq!(original.as_ref(), exported.as_ref());
            let restored =
                EcdsaP256KeyPair::from_pkcs8(exported.as_ref()).assured("exported key imports");
            assert_eq!(key.public_key(), restored.public_key());
            assert_eq!(key.public_coordinates(), restored.public_coordinates());
            assert_sign_verify(&restored, input);
        });
}
