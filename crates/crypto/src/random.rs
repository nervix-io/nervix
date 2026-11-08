//! Secure random bytes obtained from AWS-LC.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Filling caller-owned storage and producing fixed-size secure random values.
//! - **Depends on.** AWS-LC's operating-system-seeded cryptographic random generator.
//! - **Must not know.** Salts, identities, wire formats, simulation seeds or runtime policy.

use meticulous::ResultExt as _;

/// Fill the complete destination with secure random bytes.
///
/// AWS-LC's `RAND_bytes` contract returns one on success and aborts if sufficient entropy cannot
/// be obtained. The pinned aws-lc-rs `rand::fill` calls that function directly; its error result
/// is therefore unreachable. No caller receives a partial or fallback random value.
pub fn fill(destination: &mut [u8]) {
    aws_lc_rs::rand::fill(destination)
        .assured("AWS-LC RAND_bytes returns one or aborts when sufficient entropy is unavailable");
}

/// Produce exactly `N` secure random bytes in a fixed-size array.
#[must_use]
pub fn bytes<const N: usize>() -> [u8; N] {
    let mut output = [0; N];
    fill(&mut output);
    output
}

/// Draw an unsigned 64-bit value from the complete secure random bit domain.
#[must_use]
pub fn u64() -> u64 {
    u64::from_le_bytes(bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filling_a_slice_preserves_the_storage_outside_it() {
        let mut storage = [0xa5; 66];
        fill(&mut storage[1..65]);
        assert_eq!(storage[0], 0xa5);
        assert_eq!(storage[65], 0xa5);
        fill(&mut storage[1..1]);
        assert_eq!(storage[0], 0xa5);
        assert_eq!(storage[65], 0xa5);
    }
}
