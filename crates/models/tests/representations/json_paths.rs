//! A JSON path keeps every step through its text, serde and archived forms, and path text reads as
//! a path whose own text is a fixed point, or fails with a typed error.
//!
//! The valid domain is up to `JsonPath::MAX_STEPS` steps, each a member named by any string or an
//! element at any `u32` index. Text accepts both `.name` and `["name"]` for a plain member and
//! renders the first, which is a canonicalizing contract tested on arbitrary text.

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{JsonPath, JsonPathError, JsonPathStep};

/// Pieces arbitrary path text is assembled from: the path's own punctuation, JSON string escapes,
/// digits with and without leading zeros, and characters outside ASCII.
const PATH_PIECES: [&str; 22] = [
    "$",
    ".",
    "[",
    "]",
    "\"",
    "\\\"",
    "\\\\",
    "\\u0041",
    "\\q",
    "0",
    "01",
    "4294967295",
    "4294967296",
    "a",
    "_b2",
    "1st",
    " ",
    "é",
    "🎉",
    "'",
    ",",
    "$.",
];

#[test]
fn bolero_json_paths_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let path = arbitrary.json_path();
            let text = path.to_string();
            let reparsed = JsonPath::parse(&text)
                .unwrap_or_else(|error| panic!("{text} must parse: {error:?}"));
            assert_eq!(reparsed, path, "{text}");

            let json = serde_json::to_string(&path).expect("a path has a JSON form");
            assert_eq!(
                serde_json::from_str::<JsonPath>(&json).ok(),
                Some(path.clone())
            );
            assert_eq!(crate::archive_round_trip!(&path, JsonPath), path);
        });
}

/// The archived layout of a JSON path, written field for field so a test can archive more steps
/// than the validating constructor allows and hand them to the decoder.
#[derive(rkyv::Archive, rkyv::Serialize)]
struct ArchivedPathFields {
    steps: Vec<JsonPathStep>,
}

#[test]
fn bolero_json_path_input_is_validated_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entropy = arbitrary.entropy();
            let pieces = entropy.boundary_biased(0..=24);
            let mut text = String::new();
            for _ in 0..pieces {
                text.push_str(entropy.pick(PATH_PIECES));
            }
            match JsonPath::parse(&text) {
                Ok(path) => {
                    let canonical = path.to_string();
                    let reparsed = JsonPath::parse(&canonical)
                        .unwrap_or_else(|error| panic!("{canonical} must parse: {error:?}"));
                    assert_eq!(reparsed, path, "{text} read as {canonical}");
                    assert_eq!(reparsed.to_string(), canonical);
                }
                Err(report) => assert!(matches!(
                    report.current_context(),
                    JsonPathError::MissingRoot
                        | JsonPathError::ExpectedStep { .. }
                        | JsonPathError::InvalidMemberName { .. }
                        | JsonPathError::InvalidBracket { .. }
                        | JsonPathError::InvalidIndex { .. }
                        | JsonPathError::InvalidQuotedName { .. }
                        | JsonPathError::UnclosedBracket { .. }
                        | JsonPathError::TooManySteps { .. }
                )),
            }

            // A decoder admits exactly the step counts the constructor does.
            let count = usize::try_from(entropy.boundary_biased(0..=70)).expect("70 fits in usize");
            let steps = (0..count)
                .map(|index| {
                    JsonPathStep::Element(
                        u32::try_from(index).expect("an index below 70 fits in u32"),
                    )
                })
                .collect::<Vec<_>>();
            let admitted = JsonPath::new(steps.clone()).is_ok();
            assert_eq!(admitted, count <= JsonPath::MAX_STEPS);
            let json = serde_json::to_string(&serde_json::json!({ "steps": steps }))
                .expect("steps have a JSON form");
            assert_eq!(
                serde_json::from_str::<JsonPath>(&json).is_ok(),
                admitted,
                "JSON decoding of {count} steps"
            );
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&ArchivedPathFields { steps })
                .expect("the steps archive");
            assert_eq!(
                rkyv::from_bytes::<JsonPath, rkyv::rancor::Error>(&archived).is_ok(),
                admitted,
                "archive decoding of {count} steps"
            );
        });
}
