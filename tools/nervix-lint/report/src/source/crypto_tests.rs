//! Authored non-AWS-LC cryptography sites, their controls and complete current representations.

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;

const RULE: &str = AUTHORED_NON_AWS_LC_CRYPTO;

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
fn the_analysis_configuration_and_its_expectations_do_not_hide_a_use() {
    let source = "\
#[cfg(nervix_lint)] use sha2::Sha256;
#[cfg(not(nervix_lint))] fn entropy() { getrandom::fill(&mut [0u8; 8]); }
#[cfg_attr(nervix_lint, expect(nervix::bare_error_signature, reason = \"fixture\"))]
fn expected() { blake3::hash(b\"x\"); }
#[cfg(any(test, nervix_lint))] fn analysis() { hmac::Hmac::<u8>::new(); }
#[cfg(all(feature = \"fips\", not(nervix_lint)))] mod fips { use pbkdf2::pbkdf2_hmac; }
#[cfg(any())] extern crate argon2;
#[cfg_attr(nervix_lint, doc = \"Non-security BLAKE3: stated only for the analysis.\")]
fn conditional_purpose() { blake3::hash(b\"x\"); }
";
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 1, "Sha256", 0),
            at(source, 2, "getrandom", 0),
            at(source, 4, "blake3", 0),
            at(source, 5, "hmac", 0),
            at(source, 6, "pbkdf2_hmac", 0),
            at(source, 7, "argon2", 0),
            at(source, 9, "blake3", 0),
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
fn documented_non_security_blake3_is_scoped_to_its_operation() {
    let source = r#"
/// Non-security BLAKE3: identifies trusted configuration; never authenticates it.
use blake3::Hasher as Fingerprint;
/// Non-security BLAKE3: compares bytes inside an authenticated transfer.
fn checksum(data: &[u8]) -> blake3::Hash { blake3::hash(data) }
fn unclassified(data: &[u8]) -> blake3::Hash { blake3::hash(data) }
/// Non-security BLAKE3: identifies trusted configuration.
fn with_nested_item() {
    let _ = blake3::hash(b"config");
    fn nested() { blake3::hash(b"separate operation"); }
    sha2::Sha256::new();
}
struct Transfer {
    /// Non-security BLAKE3: incremental corruption detection, not a signature.
    checksum: blake3::Hasher,
    unclassified: blake3::Hash,
}
"#;
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 6, "blake3", 0),
            at(source, 6, "blake3", 1),
            at(source, 10, "blake3", 0),
            at(source, 11, "sha2", 0),
            at(source, 16, "blake3", 0),
        ]
    );
}

#[test]
fn broad_empty_or_unrelated_hash_comments_leave_blake3_counted() {
    let source = r#"
//! Non-security BLAKE3: this crate-level comment grants no exemption.
/// Non-security BLAKE3: this module-level comment grants no exemption.
mod nested { fn checksum() { blake3::hash(b"data"); } }
/// Non-security BLAKE3:
fn empty_reason() { blake3::hash(b"data"); }
/// Non-security SHA-1: another algorithm cannot classify BLAKE3.
fn other_algorithm() { blake3::hash(b"data"); }
fn ordinary_comment() {
    // Non-security BLAKE3: ordinary body comments do not classify an owner.
    blake3::hash(b"data");
}
/// Non-security BLAKE3: imports no cryptography other than BLAKE3.
use sha1::Sha1;
"#;
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 4, "blake3", 0),
            at(source, 6, "blake3", 0),
            at(source, 8, "blake3", 0),
            at(source, 11, "blake3", 0),
            at(source, 14, "Sha1", 0),
        ]
    );
}

#[test]
fn purpose_comments_keep_renames_macros_and_method_owners_separate() {
    let source = r#"
/// Non-security BLAKE3: fingerprints admitted configuration.
use blake3 as fingerprint;
struct Owner;
/// Non-security BLAKE3: an impl comment cannot classify methods.
impl Owner {
    /// Non-security BLAKE3: compares controlled content inside this operation.
    fn classified() { tracing::debug!(key = %fingerprint::hash(b"x")); }
    fn unclassified() { fingerprint::hash(b"x"); }
}
/// Non-security BLAKE3: a type comment cannot classify fields.
struct Unclassified { hash: fingerprint::Hash }
/// Non-security BLAKE3: fingerprints controlled macro inputs.
macro_rules! checksum { ($bytes:expr) => { blake3::hash($bytes) }; }
macro_rules! unclassified { () => { blake3::hash(b"x") }; }
trait Contract {
    /// Non-security BLAKE3: carries a checksum, not an authentication result.
    fn checksum(&self) -> fingerprint::Hash;
    fn unclassified(&self) -> fingerprint::Hash;
}
"#;
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 9, "fingerprint", 0),
            at(source, 12, "fingerprint", 0),
            at(source, 15, "blake3", 0),
            at(source, 19, "fingerprint", 0),
        ]
    );
}

#[test]
fn only_literal_purpose_docs_classify_an_operation() {
    let source = r#"
#[doc(hidden)] fn hidden() { blake3::hash(b"x"); }
#[doc = include_str!("purpose.txt")] fn included() { blake3::hash(b"x"); }
#[doc = 42] fn numeric() { blake3::hash(b"x"); }
#[doc = "A checksum owner."] fn ordinary() { blake3::hash(b"x"); }
#[doc = "Description.\n Non-security BLAKE3: detects corrupt controlled bytes."]
fn documented() { blake3::hash(b"x"); }
"#;
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 2, "blake3", 0),
            at(source, 3, "blake3", 0),
            at(source, 4, "blake3", 0),
            at(source, 5, "blake3", 0),
        ]
    );
}

#[test]
fn classified_imports_leave_later_qualified_operations_counted() {
    let source = r#"
/// Non-security BLAKE3: imports controlled checksum operations.
use blake3::*;
fn unclassified() { blake3::hash(b"x"); }
/// Non-security BLAKE3: cannot classify SHA-2 imports.
use sha2::*;
/// Non-security BLAKE3: names controlled checksum operations.
extern crate blake3 as digest;
fn another_operation() { digest::hash(b"x"); }
"#;
    assert_eq!(
        crypto_sites("src/fixture.rs", source),
        [
            at(source, 4, "blake3", 0),
            at(source, 6, "*", 0),
            at(source, 9, "digest", 0),
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
            let (mut line, token, entropy) = match variant {
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
            // The same inactive BLAKE3 import is unclassified, classified, or carries an empty
            // purpose. A purpose never suppresses another algorithm or entropy site.
            let purpose = seed[31] % 3;
            if purpose == 1 {
                line = format!(
                    "#[doc = \"Non-security BLAKE3: identifies controlled fixture content.\"] \
                     {line}"
                );
            } else if purpose == 2 {
                line = format!("#[doc = \"Non-security BLAKE3:   \"] {line}");
            }
            let classified = variant == 0 && purpose == 1;
            let counted = variant != 7 && !classified && !(boundary && entropy);
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
