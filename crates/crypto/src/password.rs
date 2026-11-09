//! Password derivation through AWS-LC's PBKDF2-HMAC-SHA-512 implementation.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Deriving one digest and comparing it in constant time.
//! - **Depends on.** AWS-LC and nonzero iteration counts.
//! - **Must not know.** Credential formats, user identities, cost policy or worker admission.

use std::num::NonZeroU32;

/// Derive the profile's complete 512-bit digest.
pub fn derive(iterations: NonZeroU32, salt: &[u8], password: &[u8]) -> [u8; 64] {
    let mut digest = [0; 64];
    aws_lc_rs::pbkdf2::derive(
        aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA512,
        iterations,
        salt,
        password,
        &mut digest,
    );
    digest
}

/// Compare a complete digest using the maintained constant-time verification operation.
pub fn verify(iterations: NonZeroU32, salt: &[u8], password: &[u8], digest: &[u8; 64]) -> bool {
    aws_lc_rs::pbkdf2::verify(
        aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA512,
        iterations,
        salt,
        password,
        digest,
    )
    .is_ok()
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;

    #[test]
    fn sha512_derivation_matches_the_maintained_aws_lc_known_answer() {
        let iterations = NonZeroU32::new(4096).assured("4096 is positive");
        let digest = derive(
            iterations,
            b"saltSALTsaltSALTsaltSALTsaltSALTsalt",
            b"passwordPASSWORDpassword",
        );
        assert_eq!(faster_hex::hex_string(&digest), "8c0511f4c6e597c6ac6315d8f0362e225f3c501495ba23b868c005174dc4ee71115b59f9e60cd9532fa33e0f75aefe30225c583a186cd82bd4daea9724a3d3b8");
        assert!(verify(
            iterations,
            b"saltSALTsaltSALTsaltSALTsaltSALTsalt",
            b"passwordPASSWORDpassword",
            &digest
        ));
        assert!(!verify(
            iterations,
            b"saltSALTsaltSALTsaltSALTsaltSALTsalt",
            b"wrong password",
            &digest
        ));
    }
    #[test]
    fn short_password_known_answers_cover_the_iteration_boundary() {
        for (count, expected) in [
            (1, "867f70cf1ade02cff3752599a3a53dc4af34c7a669815ae5d513554e1c8cf252c02d470a285a0501bad999bfe943c08f050235d7d68b1da55e63f73b60a57fce"),
            (2, "e1d9c16aa681708a45f5c7c4e215ceb66e011a2e9f0040713f18aefdb866d53cf76cab2868a39b9f7840edce4fef5a82be67335c77a6068e04112754f27ccf4e"),
        ] {
            let count = NonZeroU32::new(count).assured("the two known-answer counts are positive");
            let digest = derive(count, b"salt", b"password");
            assert_eq!(faster_hex::hex_string(&digest), expected);
        }
    }
}
