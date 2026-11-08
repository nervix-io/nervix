//! Known answers and a bounded independent oracle for the cryptography boundary's digests.

use sha2::{Digest as _, Sha256 as ReferenceSha256, Sha512 as ReferenceSha512};

use crate::digest::{Sha256, sha256, sha512};

#[test]
fn sha256_matches_known_answers() {
    assert_eq!(Sha256::default().finish(), sha256(b""));
    assert_eq!(
        faster_hex::hex_string(&sha256(b"")),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        faster_hex::hex_string(&sha256(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn sha512_matches_known_answers() {
    assert_eq!(
        faster_hex::hex_string(&sha512(b"")),
        concat!(
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce",
            "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        )
    );
    assert_eq!(
        faster_hex::hex_string(&sha512(b"abc")),
        concat!(
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a",
            "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        )
    );
}

#[test]
fn digests_match_the_reference_at_padding_and_block_boundaries() {
    for length in [0, 1, 55, 56, 63, 64, 65, 111, 112, 127, 128, 129, 4096] {
        let input = vec![0xa5; length];
        assert_matches_reference(&input);
    }
}

fn assert_matches_reference(input: &[u8]) {
    assert_eq!(
        sha256(input).as_slice(),
        ReferenceSha256::digest(input).as_slice()
    );
    assert_eq!(
        sha512(input).as_slice(),
        ReferenceSha512::digest(input).as_slice()
    );
    for chunk_size in [1, 7, 64, 128] {
        let mut context = Sha256::new();
        context.update(b"");
        for chunk in input.chunks(chunk_size) {
            context.update(chunk);
        }
        context.update(b"");
        assert_eq!(context.finish(), sha256(input));
    }
}

#[test]
fn bolero_sha2_digests_match_the_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(assert_matches_reference);
}
