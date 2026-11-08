//! SHA-2 digests obtained from AWS-LC.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** SHA-256 and SHA-512 byte digests and incremental SHA-256 computation.
//! - **Depends on.** AWS-LC's maintained digest implementation.
//! - **Must not know.** Encodings, fingerprints, schemas, credentials or runtime policy.

use std::mem::MaybeUninit;

use aws_lc_rs::digest;
use meticulous::{OptionExt as _, ResultExt as _};

/// Hash the complete input with SHA-256, without allocating an output buffer on the heap.
#[must_use]
pub fn sha256(input: &[u8]) -> [u8; 32] {
    digest::digest(&digest::SHA256, input)
        .as_ref()
        .try_into()
        .assured("AWS-LC's SHA-256 digest is exactly 32 bytes")
}

/// Hash the complete input with SHA-512, without allocating an output buffer on the heap.
#[must_use]
pub fn sha512(input: &[u8]) -> [u8; 64] {
    digest::digest(&digest::SHA512, input)
        .as_ref()
        .try_into()
        .assured("AWS-LC's SHA-512 digest is exactly 64 bytes")
}

/// One incremental SHA-256 computation, retaining the order and exact bytes of its updates.
pub struct Sha256 {
    // aws-lc-rs's incremental EVP context allocates its algorithm state. The maintained native
    // SHA256_CTX holds that state inline, preserving the fingerprint computation's stack storage.
    context: aws_lc_sys::SHA256_CTX,
}

impl Sha256 {
    #[must_use]
    pub fn new() -> Self {
        let mut context = MaybeUninit::uninit();
        // SAFETY: SHA256_Init writes the complete context, including its padding, and retains no
        // pointer to it. AWS-LC initializes the structure with memset and always returns one.
        let initialized = unsafe { aws_lc_sys::SHA256_Init(context.as_mut_ptr()) };
        (initialized == 1)
            .then_some(())
            .assured("AWS-LC SHA256_Init initializes the complete context and returns one");
        // SAFETY: the successful initialization above wrote every field of the native context.
        let context = unsafe { context.assume_init() };
        Self { context }
    }

    pub fn update(&mut self, input: &[u8]) {
        // SAFETY: the context was initialized by SHA256_Init and is exclusively borrowed. The
        // slice stays readable for the complete call; SHA256_Update retains no input pointer.
        let updated = unsafe {
            aws_lc_sys::SHA256_Update(&mut self.context, input.as_ptr().cast(), input.len())
        };
        (updated == 1)
            .then_some(())
            .assured("AWS-LC SHA256_Update returns one for an initialized SHA-256 context");
    }

    #[must_use]
    pub fn finish(mut self) -> [u8; 32] {
        let mut output = [0; 32];
        // SAFETY: SHA256_Final receives an initialized, exclusively owned SHA-256 context and
        // writable storage for all 32 digest bytes. Consuming self prevents further updates.
        let finalized = unsafe { aws_lc_sys::SHA256_Final(output.as_mut_ptr(), &mut self.context) };
        (finalized == 1)
            .then_some(())
            .assured("AWS-LC SHA256_Final returns one for an initialized SHA-256 context");
        output
    }
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}
