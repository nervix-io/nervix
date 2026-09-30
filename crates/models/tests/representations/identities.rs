//! Validated identities keep their text through every representation, and their parsers and
//! decoders accept exactly the text their rules allow.
//!
//! Command execution references and resource upload identities hold 1 through 128 bytes of ASCII
//! letters, digits, `.`, `_` and `-`, case kept. A node endpoint is a host and a port: a hostname
//! without `:`, an IPv4 address, or an IPv6 address in its canonical text. Connection-pool bounds
//! pair a minimum with a maximum it does not exceed.

use std::{
    net::{Ipv4Addr, Ipv6Addr},
    num::NonZeroU32,
};

use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{
    ClientPoolBounds, CommandExecutionReference, CommandExecutionReferenceError, NodeEndpoint,
    ResourceUploadIdentity, ResourceUploadIdentityError,
};

/// The longest reference or identity, in bytes.
const MAX_IDENTITY_BYTES: usize = 128;

const IDENTITY_ALPHABET: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-";

/// Characters an identity never holds.
const FOREIGN_CHARACTERS: [char; 6] = ['/', ' ', ':', '~', 'é', '🎉'];

fn identity_text(entropy: &mut Entropy<'_>) -> String {
    let length = entropy.boundary_biased(1..=128);
    let mut text = String::new();
    for _ in 0..length {
        let index = usize::try_from(entropy.up_to(64)).expect("64 fits in usize");
        text.push(char::from(IDENTITY_ALPHABET[index]));
    }
    text
}

fn arbitrary_identity_text(entropy: &mut Entropy<'_>) -> String {
    let length = entropy.boundary_biased(0..=130);
    let mut text = String::new();
    for _ in 0..length {
        if entropy.byte().is_multiple_of(16) {
            text.push(entropy.pick(FOREIGN_CHARACTERS));
        } else {
            let index = usize::try_from(entropy.up_to(64)).expect("64 fits in usize");
            text.push(char::from(IDENTITY_ALPHABET[index]));
        }
    }
    text
}

/// Whether `text` satisfies the identity rule both references and upload identities share.
fn follows_identity_rule(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= MAX_IDENTITY_BYTES
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn endpoint(entropy: &mut Entropy<'_>) -> NodeEndpoint {
    let port =
        u16::try_from(entropy.up_to(u64::from(u16::MAX))).expect("the draw ends at u16::MAX");
    let host = match entropy.byte() % 3 {
        0 => {
            let bits = u32::try_from(entropy.up_to(u64::from(u32::MAX)))
                .expect("the draw ends at u32::MAX");
            Ipv4Addr::from(bits).to_string()
        }
        1 => {
            let high = u128::from(entropy.any_u64());
            let low = u128::from(entropy.any_u64());
            Ipv6Addr::from((high << 64) | low).to_string()
        }
        _ => {
            let labels = entropy.boundary_biased(1..=4);
            let mut hostname = String::new();
            for label in 0..labels {
                if label > 0 {
                    hostname.push('.');
                }
                let length = entropy.boundary_biased(1..=63);
                for _ in 0..length {
                    let index = usize::try_from(entropy.up_to(64)).expect("64 fits in usize");
                    hostname.push(char::from(IDENTITY_ALPHABET[index]));
                }
            }
            hostname
        }
    };
    NodeEndpoint::new(host, port)
}

#[test]
fn bolero_identities_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entropy = arbitrary.entropy();

            let text = identity_text(entropy);
            let reference =
                CommandExecutionReference::parse(text.clone()).expect("the text follows the rule");
            assert_eq!(reference.as_str(), text);
            assert_eq!(reference.to_string(), text);
            let json = serde_json::to_string(&reference).expect("a reference has a JSON form");
            assert_eq!(
                serde_json::from_str::<CommandExecutionReference>(&json).ok(),
                Some(reference.clone())
            );
            assert_eq!(
                crate::archive_round_trip!(&reference, CommandExecutionReference),
                reference
            );

            let text = identity_text(entropy);
            let identity =
                ResourceUploadIdentity::parse(text.clone()).expect("the text follows the rule");
            assert_eq!(identity.as_str(), text);
            assert_eq!(identity.to_string(), text);
            let json = serde_json::to_string(&identity).expect("an identity has a JSON form");
            assert_eq!(
                serde_json::from_str::<ResourceUploadIdentity>(&json).ok(),
                Some(identity.clone())
            );
            assert_eq!(
                crate::archive_round_trip!(&identity, ResourceUploadIdentity),
                identity
            );

            let advertised = endpoint(entropy);
            let text = advertised.to_string();
            assert_eq!(
                text.parse::<NodeEndpoint>().ok(),
                Some(advertised),
                "{text}"
            );

            let bounds = arbitrary.pool_bounds();
            let json = serde_json::to_string(&bounds).expect("pool bounds have a JSON form");
            assert_eq!(
                serde_json::from_str::<ClientPoolBounds>(&json).ok(),
                Some(bounds)
            );
            assert_eq!(
                crate::archive_round_trip!(&bounds, ClientPoolBounds),
                bounds
            );
        });
}

/// The archived layout of connection-pool bounds, written field for field so a test can archive a
/// pair the validating constructor refuses and hand it to the decoder.
#[derive(rkyv::Archive, rkyv::Serialize)]
struct ArchivedPoolBoundsFields {
    minimum: u32,
    maximum: NonZeroU32,
}

#[test]
fn bolero_identity_input_is_validated_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entropy = arbitrary.entropy();

            let text = arbitrary_identity_text(entropy);
            let valid = follows_identity_rule(&text);
            let json = serde_json::to_string(&text).expect("a string has a JSON form");
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&text).expect("a string archives");

            let reference = CommandExecutionReference::parse(text.clone());
            assert_eq!(reference.is_ok(), valid, "{text:?}");
            if let Err(report) = &reference {
                assert!(matches!(
                    report.current_context(),
                    CommandExecutionReferenceError::Empty
                        | CommandExecutionReferenceError::TooLong { .. }
                        | CommandExecutionReferenceError::InvalidCharacter
                ));
            }
            assert_eq!(
                serde_json::from_str::<CommandExecutionReference>(&json).is_ok(),
                valid,
                "JSON decoding of {text:?}"
            );
            assert_eq!(
                rkyv::from_bytes::<CommandExecutionReference, rkyv::rancor::Error>(&archived)
                    .is_ok(),
                valid,
                "archive decoding of {text:?}"
            );

            let identity = ResourceUploadIdentity::parse(text.clone());
            assert_eq!(identity.is_ok(), valid, "{text:?}");
            if let Err(report) = &identity {
                assert!(matches!(
                    report.current_context(),
                    ResourceUploadIdentityError::Empty
                        | ResourceUploadIdentityError::TooLong { .. }
                        | ResourceUploadIdentityError::InvalidCharacter
                ));
            }
            assert_eq!(
                serde_json::from_str::<ResourceUploadIdentity>(&json).is_ok(),
                valid,
                "JSON decoding of {text:?}"
            );
            assert_eq!(
                rkyv::from_bytes::<ResourceUploadIdentity, rkyv::rancor::Error>(&archived).is_ok(),
                valid,
                "archive decoding of {text:?}"
            );

            // An advertised endpoint either fails with a typed error or reads as an endpoint whose
            // own text reads back unchanged.
            let advertised = format!("{text}:{}", entropy.up_to(70_000));
            if let Ok(endpoint) = advertised.parse::<NodeEndpoint>() {
                let canonical = endpoint.to_string();
                assert_eq!(
                    canonical.parse::<NodeEndpoint>().ok(),
                    Some(endpoint),
                    "{advertised}"
                );
            }

            let minimum = u32::try_from(entropy.up_to(u64::from(u32::MAX)))
                .expect("the draw ends at u32::MAX");
            let maximum =
                u32::try_from(entropy.boundary_biased(1..=u64::from(u32::MAX))).unwrap_or(1);
            let maximum = NonZeroU32::new(maximum).expect("the range starts at one");
            let orderable = minimum <= maximum.get();
            assert_eq!(ClientPoolBounds::new(minimum, maximum).is_ok(), orderable);
            let json = format!(r#"{{"minimum":{minimum},"maximum":{maximum}}}"#);
            assert_eq!(
                serde_json::from_str::<ClientPoolBounds>(&json).is_ok(),
                orderable,
                "JSON decoding of {minimum} through {maximum}"
            );
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&ArchivedPoolBoundsFields {
                minimum,
                maximum,
            })
            .expect("the fields archive");
            assert_eq!(
                rkyv::from_bytes::<ClientPoolBounds, rkyv::rancor::Error>(&archived).is_ok(),
                orderable,
                "archive decoding of {minimum} through {maximum}"
            );
        });
}
