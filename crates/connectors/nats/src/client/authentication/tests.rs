//! Interoperability evidence for the production NATS authentication owner.
//!
//! Layer: test harness.
//! - **Owns.** Bounded wire-format and nonce-signature checks against the independent SDK.
//! - **Depends on.** The authentication owner, public fixtures, Bolero and test-only nkeys.
//! - **Must not know.** Connector lifecycle or graph state.

use data_encoding::BASE32_NOPAD;
use meticulous::{OptionExt as _, ResultExt as _};

const CHECKSUM: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);

use super::{
    AuthenticationIdentity, NatsAuthenticationError, SigningIdentity, credentials_identity,
};

const SEED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../docker/nats/auth-user.nk"
));
const CREDENTIALS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../docker/nats/auth-user.creds"
));

#[test]
fn aws_lc_authentication_matches_the_broker_user_identity() {
    let signer = SigningIdentity::from_seed(SEED.trim()).assured("the public user seed decodes");
    assert_eq!(
        signer.public,
        "UDZFLOU5CPK4QI5ASKWEODVVLRKWXYAKF6SKOKKGBTORCIVH3TRDVRP3"
    );
    let oracle = nkeys::KeyPair::from_seed(SEED.trim()).assured("the SDK reads the same user");
    let auth = AuthenticationIdentity::Nkey(signer)
        .authenticate(b"the broker nonce")
        .assured("AWS-LC signs the challenge");
    assert_eq!(auth.nkey.as_deref(), Some(oracle.public_key().as_str()));
    assert!(auth.jwt.is_none());
    oracle
        .verify(
            b"the broker nonce",
            auth.signature
                .as_deref()
                .assured("nkey authentication signs"),
        )
        .assured("the independent SDK verifies the AWS-LC signature");
}

#[test]
fn credentials_keep_the_jwt_and_sign_with_its_user_seed() {
    for text in [CREDENTIALS.to_owned(), CREDENTIALS.replace('\n', "\r\n")] {
        let identity = credentials_identity(text.trim()).assured("a complete credentials file");
        let auth = identity
            .authenticate(b"nonce")
            .assured("the callback signs");
        assert_eq!(auth.jwt.as_deref(), CREDENTIALS.lines().nth(1),);
        assert!(auth.nkey.is_none());
        let oracle =
            nkeys::KeyPair::from_seed("SUAOMZMTJNGUJ42PSH7WBDJHLPOUA5FAUOSGVVNEYZYELLGBOO5N5ACJGI")
                .assured("the credentials fixture's independent SDK identity");
        oracle
            .verify(b"nonce", auth.signature.as_deref().assured("the signature"))
            .assured("the credentials user's key signs the challenge");
    }
}

#[test]
fn seed_refusals_are_typed_and_do_not_render_secret_values() {
    for length in 0..58 {
        let Err(error) = SigningIdentity::from_seed(&SEED.trim()[..length]) else {
            panic!("an incomplete seed cannot prepare a signer");
        };
        assert!(matches!(
            error.current_context(),
            NatsAuthenticationError::SeedLength
        ));
    }
    let mut raw = BASE32_NOPAD
        .decode(SEED.trim().as_bytes())
        .assured("the fixture");
    raw[2] ^= 1;
    let changed = BASE32_NOPAD.encode(&raw);
    let Err(error) = SigningIdentity::from_seed(&changed) else {
        panic!("the seed checksum must be verified");
    };
    assert!(matches!(
        error.current_context(),
        NatsAuthenticationError::SeedChecksum
    ));
    assert!(format!("{error:#}").starts_with("invalid NATS user seed checksum"));
    assert!(error.downcast_ref::<nkeys_codec::error::Error>().is_some());
    raw[0] = 0;
    let checksum = CHECKSUM.checksum(&raw[..34]).to_le_bytes();
    raw[34..].copy_from_slice(&checksum);
    let Err(error) = SigningIdentity::from_seed(&BASE32_NOPAD.encode(&raw)) else {
        panic!("a different identity kind is not a user seed");
    };
    assert!(matches!(
        error.current_context(),
        NatsAuthenticationError::SeedKind
    ));
    let Err(error) = SigningIdentity::from_seed(&"!".repeat(58)) else {
        panic!("seed text must be canonical base32");
    };
    assert!(matches!(
        error.current_context(),
        NatsAuthenticationError::SeedEncoding
    ));
    let cause = error
        .downcast_ref::<nkeys_codec::error::Error>()
        .assured("the codec keeps its typed refusal");
    let source = std::error::Error::source(cause).assured("the codec keeps the base32 source");
    assert!(
        source
            .downcast_ref::<data_encoding::DecodeError>()
            .is_some()
    );
}

#[test]
fn credentials_require_both_complete_sections() {
    for text in [
        "",
        SEED,
        "-----BEGIN NATS USER JWT-----\nuser.jwt.signature\n------END NATS USER JWT------",
        "-----BEGIN NATS USER JWT-----\nuser.jwt.signature",
        "-----BEGIN NATS USER JWT-----\n\n------END NATS USER JWT------",
    ] {
        let Err(error) = credentials_identity(text) else {
            panic!("authentication requires complete user credentials");
        };
        assert!(matches!(
            error.current_context(),
            NatsAuthenticationError::Credentials
        ));
    }
}

#[test]
fn bolero_nkey_authentication_preserves_identity_and_nonce() {
    bolero::check!()
        .with_type::<([u8; 32], [u8; 64], u8)>()
        .for_each(|(seed, nonce, length)| {
            let nonce = &nonce[..usize::from(*length) % 65];
            let oracle = nkeys::KeyPair::new_from_raw(nkeys::KeyPairType::User, *seed)
                .assured("every 32-byte Ed25519 seed forms a user");
            let encoded = oracle.seed().assured("the generated user has its seed");
            let decoded = nkeys_codec::decode_seed_for(&encoded, nkeys_codec::KeyPairType::User)
                .assured("the maintained codec decodes the complete user seed");
            assert_eq!(*decoded, *seed);
            assert_eq!(
                nkeys_codec::encode_seed(&nkeys_codec::KeyPairType::User, seed),
                encoded
            );
            let signer = SigningIdentity::from_seed(&encoded)
                .assured("the production decoder accepts an SDK seed");
            assert_eq!(signer.public, oracle.public_key());
            let identity = AuthenticationIdentity::Nkey(signer);
            let auth = identity.authenticate(nonce).assured("AWS-LC signs");
            assert_eq!(auth.nkey.as_deref(), Some(oracle.public_key().as_str()));
            oracle
                .verify(nonce, auth.signature.as_deref().assured("a signature"))
                .assured("the SDK verifies the production signature");
            let text = format!(
                "-----BEGIN NATS USER JWT-----\nuser.jwt.signature\n------END NATS USER \
                 JWT------\n\n-----BEGIN USER NKEY SEED-----\n{encoded}\n------END USER NKEY \
                 SEED------\n"
            );
            let auth = credentials_identity(&text)
                .assured("the production decoder accepts decorated credentials")
                .authenticate(nonce)
                .assured("the credentials callback signs");
            assert_eq!(auth.jwt.as_deref(), Some("user.jwt.signature"));
            assert!(auth.nkey.is_none());
            oracle
                .verify(nonce, auth.signature.as_deref().assured("a JWT signature"))
                .assured("the SDK verifies the credentials user's signature");
        });
}

#[test]
fn identity_kind_is_checked_before_the_transcription_checksum() {
    let mut raw = BASE32_NOPAD
        .decode(SEED.trim().as_bytes())
        .assured("the fixture decodes");
    raw[0] = 0;
    raw[2] ^= 1;
    let Err(error) = SigningIdentity::from_seed(&BASE32_NOPAD.encode(&raw)) else {
        panic!("a different identity kind cannot prepare a user signer");
    };
    assert!(matches!(
        error.current_context(),
        NatsAuthenticationError::SeedKind
    ));
}

#[test]
fn credentials_preserve_whitespace_section_order_and_eof() {
    let jwt = CREDENTIALS
        .lines()
        .nth(1)
        .assured("the fixture has its JWT");
    let seed = "SUAOMZMTJNGUJ42PSH7WBDJHLPOUA5FAUOSGVVNEYZYELLGBOO5N5ACJGI";
    let text = format!(
        "decoration\n-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY \
         SEED------\n-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------"
    );
    let text = text
        .lines()
        .map(|line| format!("\u{2003}{line}\t"))
        .collect::<Vec<_>>()
        .join("\r\n");
    let auth = credentials_identity(&text)
        .assured("current decorated credentials prepare")
        .authenticate(b"nonce")
        .assured("the callback signs");
    assert_eq!(auth.jwt.as_deref(), Some(jwt));
    let oracle = nkeys::KeyPair::from_seed(seed).assured("the independent SDK reads the fixture");
    oracle
        .verify(b"nonce", auth.signature.as_deref().assured("a signature"))
        .assured("the SDK verifies the signature");
}

#[test]
fn bolero_malformed_authentication_keeps_typed_value_free_refusals() {
    bolero::check!()
        .with_type::<([u8; 32], u8)>()
        .for_each(|(seed, choice)| {
            let oracle = nkeys::KeyPair::new_from_raw(nkeys::KeyPairType::User, *seed)
                .assured("every fixed-size seed forms a user");
            let encoded = oracle.seed().assured("a generated user has its seed");
            let mut raw = BASE32_NOPAD
                .decode(encoded.as_bytes())
                .assured("the SDK emits base32");
            let (malformed, expected) = match choice % 6 {
                0 => (
                    encoded[..usize::from(*choice) % encoded.len()].to_owned(),
                    NatsAuthenticationError::SeedLength,
                ),
                1 => (
                    "!".repeat(encoded.len()),
                    NatsAuthenticationError::SeedEncoding,
                ),
                2 => {
                    raw[2] ^= 1;
                    (
                        BASE32_NOPAD.encode(&raw),
                        NatsAuthenticationError::SeedChecksum,
                    )
                }
                3 => {
                    raw[1] |= 1;
                    let checksum = CHECKSUM.checksum(&raw[..34]).to_le_bytes();
                    raw[34..].copy_from_slice(&checksum);
                    (BASE32_NOPAD.encode(&raw), NatsAuthenticationError::SeedKind)
                }
                4 => {
                    let account = nkeys::KeyPair::new_from_raw(nkeys::KeyPairType::Account, *seed)
                        .assured("every fixed-size seed forms an account");
                    (
                        account.seed().assured("the account has a seed"),
                        NatsAuthenticationError::SeedKind,
                    )
                }
                _ => {
                    raw[0] = 0;
                    raw[2] ^= 1;
                    (BASE32_NOPAD.encode(&raw), NatsAuthenticationError::SeedKind)
                }
            };
            let Err(error) = SigningIdentity::from_seed(&malformed) else {
                panic!("a malformed user seed cannot prepare authentication");
            };
            assert_eq!(
                std::mem::discriminant(error.current_context()),
                std::mem::discriminant(&expected)
            );
            assert!(!format!("{error:#}").contains(&encoded));

            let jwt = "private.jwt.value";
            let contents = match choice % 4 {
                0 => format!("-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------"),
                1 => format!(
                    "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER \
                     JWT------\n-----BEGIN USER NKEY SEED-----\n{encoded}"
                ),
                2 => format!(
                    "-----BEGIN NATS USER JWT-----\n\n------END NATS USER JWT------\n-----BEGIN \
                     USER NKEY SEED-----\n{encoded}\n------END USER NKEY SEED------"
                ),
                _ => format!(
                    "-----BEGIN NATS USER JWT-----\n{jwt}\ninvalid ending\n-----BEGIN USER NKEY \
                     SEED-----\n{encoded}\n------END USER NKEY SEED------"
                ),
            };
            let Err(error) = credentials_identity(&contents) else {
                panic!("incomplete credentials cannot prepare authentication");
            };
            assert!(matches!(
                error.current_context(),
                NatsAuthenticationError::Credentials
            ));
            assert!(
                error
                    .downcast_ref::<async_nats::auth_utils::CredentialsError>()
                    .is_some()
            );
            let rendered = format!("{error:#}");
            assert!(!rendered.contains(jwt));
            assert!(!rendered.contains(&encoded));
        });
}
