//! Authored non-AWS-LC cryptography sites, their controls and complete current representations.

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;

const RULE: &str = "non_aws_lc_crypto";

/// The line and column of every counted site in one file.
fn crypto_sites(path: &str, source: &str) -> Vec<(usize, usize)> {
    let sites = scan(vec![SourceInput {
        path: path.into(),
        source: source.into(),
    }])
    .assured("the fixture supplies valid current Rust source");
    sites
        .into_iter()
        .filter(|site| site.rule == RULE)
        .map(|site| (site.line, site.column))
        .collect()
}

/// The line and column of the `occurrence`-th appearance of `token` in the source.
fn at(source: &str, line: usize, token: &str, occurrence: usize) -> (usize, usize) {
    let text = source
        .lines()
        .nth(line - 1)
        .assured("the fixture has the line");
    let (column, _) = text
        .match_indices(token)
        .nth(occurrence)
        .assured("the fixture line has the token");
    (line, column)
}

#[test]
fn every_imported_name_and_written_path_of_a_counted_crate_is_one_site() {
    let source = "\
use sha2::{Digest as _, Sha256};
fn digest(data: &[u8]) -> Vec<u8> { Sha256::digest(data).to_vec() }
fn fingerprint(data: &[u8]) -> blake3::Hash { blake3::hash(data) }
extern crate argon2 as kdf;
fn derive() { kdf::Argon2::default(); }
fn mac() -> ::hmac::SimpleHmac<u8> { make() }
use pbkdf2::{self as pbkdf};
fn legacy() { sha1::Sha1::new(); }
";
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 1, "Digest", 0),
            at(source, 1, "Sha256", 0),
            at(source, 2, "Sha256", 0),
            at(source, 3, "blake3", 0),
            at(source, 3, "blake3", 1),
            at(source, 4, "argon2", 0),
            at(source, 5, "kdf", 0),
            at(source, 6, "hmac", 0),
            at(source, 7, "self", 0),
            at(source, 8, "sha1", 0),
        ]
    );
}

#[test]
fn renames_globs_and_inactive_configurations_do_not_hide_a_use() {
    let source = "\
use rand_core as entropy;
fn salt() -> u64 { entropy::OsRng.next_u64() }
#[cfg(feature = \"legacy\")] use pbkdf2::pbkdf2_hmac as derive;
#[cfg(any())] fn retired() { sha1::Sha1::new(); }
use rustls::crypto::aws_lc_rs;
fn install() { aws_lc_rs::default_provider().install_default(); }
use rand::rngs::*;
use argon2::password_hash::rand_core::OsRng as Generator;
fn entropy() { getrandom::fill(&mut [0u8; 8]); }
use rustls::crypto::aws_lc_rs::default_provider as provider;
mod nested { use super::entropy as e; fn draw() { e::OsRng.next_u32(); } }
";
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 2, "entropy", 0),
            at(source, 3, "pbkdf2_hmac", 0),
            at(source, 4, "sha1", 0),
            at(source, 6, "aws_lc_rs", 0),
            at(source, 7, "*", 0),
            at(source, 8, "OsRng", 0),
            at(source, 9, "getrandom", 0),
            at(source, 10, "default_provider", 0),
            at(source, 11, "e::", 0),
        ]
    );
}

#[test]
fn names_that_only_resemble_a_counted_path_are_not_sites() {
    let source = "\
fn region() { aws_config::default_provider::region::DefaultRegionChain::builder(); }
fn local() { crate::sha2::digest(); self::hmac::sign(); super::blake3::hash(); }
fn text() -> &'static str { \"sha2::Sha256 and blake3::hash\" }
// sha2::Sha256 in a comment
#[cfg(feature = \"sha2\")] fn gated() {}
#[doc = \"blake3::hash\"] fn documented() {}
#[cfg(test)] mod checks { use sha2::Sha256; fn probe() { blake3::hash(b\"\"); } }
#[test] fn probe() { blake3::hash(b\"\"); }
fn shadow() { let hash = 1; let OsRng = hash; }
use rand_core::{RngCore as _, SeedableRng};
use rustls::crypto::aws_lc_rs;
";
    assert_eq!(crypto_sites("src/fixture.rs", source), []);
}

#[test]
fn the_boundary_may_draw_entropy_and_build_the_provider_but_uses_no_other_cryptography() {
    let source = "\
use rand_core::OsRng;
fn provider() { rustls::crypto::aws_lc_rs::default_provider(); }
fn entropy() { getrandom::fill(&mut [0u8; 8]); }
use sha2::Sha256;
";
    assert_eq!(
        crypto_sites("crates/crypto/src/lib.rs", source),
        [at(source, 4, "Sha256", 0)]
    );
    assert_eq!(
        crypto_sites("src/application/tls.rs", source),
        [
            at(source, 1, "OsRng", 0),
            at(source, 2, "rustls", 0),
            at(source, 3, "getrandom", 0),
            at(source, 4, "Sha256", 0),
        ]
    );
}

#[test]
fn tooling_and_harness_crates_are_outside_the_product() {
    let source = "use sha2::Sha256;\nfn hash() { blake3::hash(b\"\"); }\n";
    for path in [
        "tools/nervix-lint/report/src/fixture.rs",
        "crates/arbitrary/src/fixture.rs",
        "crates/benchmark/src/fixture.rs",
        "crates/model-harness/src/fixture.rs",
        "crates/test-environment/src/fixture.rs",
    ] {
        assert_eq!(crypto_sites(path, source), [], "{path}");
    }
    assert_eq!(
        crypto_sites("crates/web-console/src/fixture.rs", source).len(),
        2
    );
}

#[test]
fn macro_arguments_and_definitions_keep_each_authored_location() {
    let source = "\
macro_rules! digest { ($data:expr) => { sha2::Sha256::digest($data) }; }
fn call() { let _ = format!(\"{}\", blake3::hash(b\"x\")); }
fn traced(data: &[u8]) { tracing::debug!(digest = %blake3::hash(data), \"hashed\"); }
macro_rules! own { () => { $crate::blake3::hash(b\"\") }; }
fn entropy() { let _ = vec![rand_core::OsRng.next_u32(), 1]; }
use rustls::crypto;
fn install() { assert!(crypto::aws_lc_rs::default_provider().install_default().is_ok()); }
";
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 1, "sha2", 0),
            at(source, 2, "blake3", 0),
            at(source, 3, "blake3", 0),
            at(source, 5, "rand_core", 0),
            at(source, 7, "crypto", 0),
        ]
    );
}

#[test]
fn bolero_non_aws_lc_crypto_preserves_every_authored_site() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
        let boundary = seed[0] % 2 == 1;
        let path = if boundary {
            format!("crates/crypto/src/source_{}.rs", seed[1])
        } else {
            format!("src/source_{}.rs", seed[1])
        };
        let mut source = String::from("// bounded Unicode π sources\nuse sha2 as digests;\n");
        // The renamed crate import is itself a use, in the boundary too.
        let mut expected = vec![SourceSite {
            path: path.clone(),
            line: 2,
            column: "use ".len(),
            rule: RULE.into(),
        }];
        let count = usize::from(seed[2] % 8);
        for index in 0..count {
            let variant = seed[index + 3] % 8;
            let (line, token, entropy) = match variant {
                0 => (
                    format!("#[cfg(any())] use blake3::Hasher as H{index};\n"),
                    "Hasher",
                    false,
                ),
                1 => (
                    format!("fn d_{index}() {{ digests::Sha256::new(); }}\n"),
                    "digests",
                    false,
                ),
                2 => (
                    format!(
                        "macro_rules! m_{index} {{ () => {{ argon2::Argon2::default() }}; }}\n"
                    ),
                    "argon2",
                    false,
                ),
                3 => (
                    format!("fn r_{index}() {{ ::rand_core::OsRng.next_u64(); }}\n"),
                    "rand_core",
                    true,
                ),
                4 => (
                    format!(
                        "fn p_{index}() {{ rustls::crypto::aws_lc_rs::default_provider(); }}\n"
                    ),
                    "rustls",
                    true,
                ),
                5 => (
                    format!("fn g_{index}() {{ getrandom::fill(&mut [0u8; {index}]); }}\n"),
                    "getrandom",
                    true,
                ),
                6 => (
                    format!(
                        "fn t_{index}() {{ tracing::debug!(key = %hmac::Hmac::<u8>::new(), \
                         \"x\"); }}\n"
                    ),
                    "hmac",
                    false,
                ),
                _ => (
                    format!("fn c_{index}() {{ aws_config::default_provider::region(); }}\n"),
                    "aws_config",
                    true,
                ),
            };
            let counted = variant != 7 && !(boundary && entropy);
            if counted {
                let column = line
                    .find(token)
                    .assured("the generated line contains its counted token");
                expected.push(SourceSite {
                    path: path.clone(),
                    line: index + 3,
                    column,
                    rule: RULE.into(),
                });
            }
            source.push_str(&line);
        }
        source.push_str(
            "#[cfg(test)] mod checks { use sha2::Sha256; fn probe() { blake3::hash(b\"\"); } }\n",
        );
        source.push_str("fn text() -> &'static str { \"pbkdf2::pbkdf2 and sha1::Sha1\" }\n");
        source.push_str("fn local() { crate::sha2::digest(); let OsRng = 1; let _ = OsRng; }\n");
        let input = SourceInput { path, source };
        let encoded =
            serde_json::to_vec(&input).assured("current source input has a JSON representation");
        let decoded: SourceInput =
            serde_json::from_slice(&encoded).assured("the encoded complete input is valid JSON");
        assert_eq!(decoded, input);
        let result: Vec<SourceSite> = scan(vec![decoded])
            .assured("the generator supplies valid current Rust syntax")
            .into_iter()
            .filter(|site| site.rule == RULE)
            .collect();
        assert_eq!(result, expected);
        let encoded =
            serde_json::to_vec(&result).assured("complete sites have a JSON representation");
        let decoded: Vec<SourceSite> =
            serde_json::from_slice(&encoded).assured("the complete sites were just serialized");
        assert_eq!(decoded, result);
    });
}
