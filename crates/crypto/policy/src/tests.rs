//! Complete ordered selection and current-shape serialization checks.
//!
//! Layer: test harness.
//! - **Owns.** Oracles for alias expansion, exact names and lossless value projections.
//! - **Depends on.** The production algorithm catalog and serialization owners.
//! - **Must not know.** TLS engines or running services.

use meticulous::ResultExt as _;

use super::*;

#[test]
fn aliases_expand_in_place_and_repeats_keep_their_first_position() {
    let suites = TlsCipherSuites::resolve([
        "TLS13_CHACHA20_POLY1305_SHA256",
        "fips",
        "TLS13_AES_128_GCM_SHA256",
        "default",
    ])
    .assured("all tokens name declared algorithms or aliases");
    let mut expected = vec![TlsCipherSuite::ChaCha20Poly1305Sha256];
    for suite in TlsCipherSuite::ALL {
        if suite.is_fips_approved() {
            expected.push(*suite);
        }
    }
    for suite in TlsCipherSuite::ALL {
        // The expected sequence is bounded by the catalog, not input or graph size.
        if !expected.contains(suite) {
            expected.push(*suite);
        }
    }
    assert_eq!(suites.iter().collect::<Vec<_>>(), expected);
    assert_eq!(suites.iter().count(), TlsCipherSuite::ALL.len());
}

#[test]
fn iana_and_rustls_names_are_case_insensitive_and_render_one_exact_name() {
    let suites = TlsCipherSuites::resolve(["tls_aes_128_gcm_sha256", "TLS13_AES_128_GCM_SHA256"])
        .assured("both spellings identify the same IANA suite");
    assert_eq!(suites.to_string(), "TLS13_AES_128_GCM_SHA256");
    let groups = TlsKeyExchangeGroups::resolve(["secp256R1", "x25519mlkem768"])
        .assured("case does not change algorithm identity");
    assert_eq!(groups.to_string(), "secp256r1,X25519MLKEM768");
    assert_eq!(TlsCipherSuite::Aes128GcmSha256.iana(), 0x1301);
    assert_eq!(TlsKeyExchangeGroup::X25519MlKem768.iana(), 0x11ec);
}

#[test]
fn defaults_name_only_approved_algorithms() {
    let suites = TlsCipherSuites::default();
    let groups = TlsKeyExchangeGroups::default();
    assert!(suites.iter().all(TlsCipherSuite::is_fips_approved));
    assert!(groups.iter().all(TlsKeyExchangeGroup::is_fips_approved));
    assert!(suites.iter().any(TlsCipherSuite::is_tls13));
    assert!(groups.iter().any(TlsKeyExchangeGroup::supports_tls12));
}

#[test]
fn empty_and_unknown_selections_keep_typed_causes() {
    let empty = TlsCipherSuites::resolve(std::iter::empty::<&str>())
        .expect_err("a selection needs at least one algorithm");
    assert_eq!(
        empty.current_context(),
        &AlgorithmSelectionError::Empty {
            kind: AlgorithmKind::CipherSuite
        }
    );
    let unknown = TlsKeyExchangeGroups::resolve(["not-a-group"])
        .expect_err("unknown group names have no typed identity");
    assert_eq!(
        unknown.current_context(),
        &AlgorithmSelectionError::Unknown {
            kind: AlgorithmKind::KeyExchangeGroup,
            item: "not-a-group".to_owned(),
        }
    );
}

#[test]
fn current_serializations_refuse_gaps_and_duplicates() {
    let mut slots = [None; TlsCipherSuite::ALL.len()];
    slots[0] = Some(TlsCipherSuite::Aes128GcmSha256);
    slots[2] = Some(TlsCipherSuite::Aes256GcmSha384);
    let json = serde_json::to_vec(&slots).assured("an array of current values serializes");
    assert!(serde_json::from_slice::<TlsCipherSuites>(&json).is_err());
    let invalid = TlsCipherSuites(slots);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&invalid)
        .assured("the deliberately invalid current array serializes");
    assert!(rkyv::from_bytes::<TlsCipherSuites, rkyv::rancor::Error>(&bytes).is_err());
    slots[1] = Some(TlsCipherSuite::Aes128GcmSha256);
    slots[2] = None;
    let invalid = TlsCipherSuites(slots);
    assert!(matches!(
        invalid
            .validate()
            .expect_err("a stored selection is unique")
            .current_context(),
        AlgorithmSelectionError::Duplicate { .. }
    ));
}

fn selections(input: &[u8]) -> (TlsCipherSuites, TlsKeyExchangeGroups) {
    let suite_choices = TlsCipherSuites::names().collect::<Vec<_>>();
    let group_choices = TlsKeyExchangeGroups::names().collect::<Vec<_>>();
    let mut suite_names = Vec::new();
    let mut group_names = Vec::new();
    for byte in input.iter().take(64) {
        let index = usize::from(*byte);
        suite_names.push(suite_choices[index % suite_choices.len()]);
        group_names.push(group_choices[index % group_choices.len()]);
    }
    if suite_names.is_empty() {
        suite_names.push(AlgorithmAlias::Fips.name());
        group_names.push(AlgorithmAlias::Fips.name());
    }
    // Names, aliases, overlaps and repeated input items all pass through the production resolver.
    (
        TlsCipherSuites::resolve(suite_names).assured("declared choices resolve"),
        TlsKeyExchangeGroups::resolve(group_names).assured("declared choices resolve"),
    )
}

#[test]
fn bolero_tls_algorithm_selections_preserve_order_and_membership() {
    bolero::check!().for_each(|input: &[u8]| {
        let (suites, groups) = selections(input);
        let suite_text = suites.to_string();
        let group_text = groups.to_string();
        assert_eq!(
            suite_text
                .parse::<TlsCipherSuites>()
                .assured("canonical names resolve"),
            suites
        );
        assert_eq!(
            group_text
                .parse::<TlsKeyExchangeGroups>()
                .assured("canonical names resolve"),
            groups
        );
        let json = serde_json::to_vec(&(suites, groups)).assured("all current values serialize");
        assert_eq!(
            serde_json::from_slice::<(TlsCipherSuites, TlsKeyExchangeGroups)>(&json)
                .assured("the complete current pair deserializes"),
            (suites, groups)
        );
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&(suites, groups))
            .assured("all current values archive");
        assert_eq!(
            rkyv::from_bytes::<(TlsCipherSuites, TlsKeyExchangeGroups), rkyv::rancor::Error>(
                &bytes
            )
            .assured("the complete current pair deserializes"),
            (suites, groups)
        );
    });
}

#[test]
fn bolero_unknown_tls_algorithm_names_are_typed_refusals() {
    bolero::check!().for_each(|input: &[u8]| {
        let input = &input[..input.len().min(64)];
        let name = format!("unrecognized_{}", String::from_utf8_lossy(input));
        let error =
            TlsCipherSuites::resolve([name.as_str()]).expect_err("the prefix names no suite");
        assert_eq!(
            error.current_context(),
            &AlgorithmSelectionError::Unknown {
                kind: AlgorithmKind::CipherSuite,
                item: name.clone(),
            }
        );
        let error =
            TlsKeyExchangeGroups::resolve([name.as_str()]).expect_err("the prefix names no group");
        assert_eq!(
            error.current_context(),
            &AlgorithmSelectionError::Unknown {
                kind: AlgorithmKind::KeyExchangeGroup,
                item: name,
            }
        );
    });
}
