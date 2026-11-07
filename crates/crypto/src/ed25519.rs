//! AWS-LC Ed25519 signing without an external credential or wire format.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Preparing an Ed25519 signing key from a seed and signing a byte string.
//! - **Depends on.** AWS-LC and typed error reports.
//! - **Must not know.** NATS, credentials files, wire encodings, Models or runtime policy.

pub use aws_lc_rs::signature::Signature;
use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair as _};
use error_stack::{Report, ResultExt as _};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Ed25519Error {
    #[error("failed to prepare the Ed25519 signing key")]
    PrepareKey,
    #[error("failed to sign with the Ed25519 key")]
    Sign,
}

pub struct Ed25519SigningKey {
    key: Ed25519KeyPair,
}

impl Ed25519SigningKey {
    pub fn from_seed(seed: &[u8]) -> Result<Self, Report<Ed25519Error>> {
        let key =
            Ed25519KeyPair::from_seed_unchecked(seed).change_context(Ed25519Error::PrepareKey)?;
        Ok(Self { key })
    }

    pub fn public_key(&self) -> &[u8] {
        self.key.public_key().as_ref()
    }

    pub fn sign(&self, message: &[u8]) -> Result<Signature, Report<Ed25519Error>> {
        self.key
            .try_sign(message)
            .change_context(Ed25519Error::Sign)
    }
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::{
        error::KeyRejected,
        signature::{ED25519, UnparsedPublicKey},
    };
    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn a_signature_binds_the_complete_message_and_public_key() {
        let key = Ed25519SigningKey::from_seed(&[7; 32]).assured("a complete Ed25519 seed");
        let message = b"the complete nonce";
        let signature = key.sign(message).assured("AWS-LC signs the message");
        let verifier = UnparsedPublicKey::new(&ED25519, key.public_key());
        verifier
            .verify(message, signature.as_ref())
            .assured("the public key verifies its signature");
        assert!(
            verifier
                .verify(b"a different nonce", signature.as_ref())
                .is_err()
        );
    }

    #[test]
    fn an_incomplete_seed_keeps_the_typed_aws_lc_cause() {
        let Err(error) = Ed25519SigningKey::from_seed(&[7; 31]) else {
            panic!("an incomplete seed cannot prepare a signing key");
        };
        assert!(matches!(error.current_context(), Ed25519Error::PrepareKey));
        assert!(error.contains::<KeyRejected>());
    }
}
