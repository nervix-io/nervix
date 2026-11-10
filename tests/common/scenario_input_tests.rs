//! Regression matrix for public feature selection and composed Gherkin parsing.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** CLI file/directory/glob selection and outline-expansion regression cases.
//! - **Depends on.** The production harness selector, Clap and temporary feature files.
//! - **Must not know.** Scenario state, scheduling or product behavior.

use std::path::Path;

use clap::Parser as _;

use super::scenario_input::{ScenarioInput, parse_features};

#[derive(clap::Parser)]
struct InputCli {
    #[command(flatten)]
    input: ScenarioInput,
}

fn cli(input: Option<String>) -> ScenarioInput {
    let mut args = vec!["scenarios".to_owned()];
    if let Some(input) = input {
        args.extend(["--input".to_owned(), input]);
    }
    InputCli::try_parse_from(args)
        .expect("the public input option parses")
        .input
}

fn fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("the feature fixture directory is available");
    let directory = root.path().join("tests/features/runtime");
    std::fs::create_dir_all(&directory).expect("the feature directory is writable");
    for name in ["first", "second"] {
        std::fs::write(
            directory.join(format!("{name}.feature")),
            format!(
                "Feature: {name}\n  Scenario: selected\n    Given a feature-selection fixture\n",
            ),
        )
        .expect("the feature fixture is writable");
    }
    let generated = root.path().join("target/generated/tests/features/runtime");
    std::fs::create_dir_all(&generated).expect("the generated directory is writable");
    std::fs::write(generated.join("ghost.feature"), "invalid generated Gherkin")
        .expect("the generated fixture is writable");
    root
}

fn names(root: &Path, input: Option<String>) -> Vec<String> {
    parse_features(root, Path::new("tests/features"), cli(input))
        .into_iter()
        .map(|feature| feature.expect("the selected feature parses").name)
        .collect()
}

#[test]
fn literal_files_select_the_same_feature_from_relative_and_absolute_paths() {
    let root = fixture();
    let relative = "tests/features/runtime/first.feature";
    assert_eq!(names(root.path(), Some(relative.to_owned())), ["first"]);
    assert_eq!(
        names(
            root.path(),
            Some(root.path().join(relative).display().to_string())
        ),
        ["first"]
    );
}

#[test]
fn directories_and_globs_select_only_the_named_feature_subtree() {
    let root = fixture();
    for input in [
        None,
        Some("tests/features/runtime".to_owned()),
        Some("tests/features/runtime/*.feature".to_owned()),
        Some(
            root.path()
                .join("tests/features/runtime/{first,second}.feature")
                .display()
                .to_string(),
        ),
    ] {
        assert_eq!(names(root.path(), input), ["first", "second"]);
    }
    assert_eq!(
        names(
            root.path(),
            Some("tests/features/runtime/f?rst.feature".to_owned())
        ),
        ["first"]
    );
    assert_eq!(
        names(
            root.path(),
            Some("tests/features/runtime/[fs]*.feature".to_owned())
        ),
        ["first", "second"]
    );
}

#[test]
fn missing_literal_inputs_are_errors_and_unmatched_globs_select_nothing() {
    let root = fixture();
    let features = parse_features(
        root.path(),
        Path::new("tests/features"),
        cli(Some("tests/features/runtime/missing.feature".to_owned())),
    );
    assert_eq!(features.len(), 1);
    assert!(features[0].is_err());
    let malformed = parse_features(
        root.path(),
        Path::new("tests/features"),
        cli(Some(
            "target/generated/tests/features/runtime/ghost.feature".to_owned(),
        )),
    );
    assert_eq!(malformed.len(), 1);
    assert!(malformed[0].is_err());
    assert!(
        names(
            root.path(),
            Some("tests/features/runtime/missing*.feature".to_owned())
        )
        .is_empty()
    );
}

#[test]
fn outlines_expand_names_steps_payloads_tables_and_example_tags_before_selection() {
    let root = fixture();
    std::fs::write(
        root.path().join("tests/features/runtime/topology.feature"),
        r#"Feature: topology
  Scenario Outline: cluster <nodes>
    Given cluster <nodes>
      """
      topology <nodes>
      """
    And rows
      | nodes   |
      | <nodes> |

    @topology_example
    Examples:
      | nodes |
      | 1     |
      | 3     |
"#,
    )
    .expect("the outline fixture is writable");
    let mut features = parse_features(
        root.path(),
        Path::new("tests/features"),
        cli(Some("tests/features/runtime/topology.feature".to_owned())),
    );
    let feature = features.remove(0).expect("the outline parses and expands");
    assert_eq!(feature.scenarios.len(), 2);
    for (scenario, nodes) in feature.scenarios.iter().zip(["1", "3"]) {
        assert_eq!(scenario.name, format!("cluster {nodes}"));
        assert_eq!(scenario.steps[0].value, format!("cluster {nodes}"));
        assert_eq!(
            scenario.steps[0].docstring.as_deref(),
            Some(format!("\ntopology {nodes}\n").as_str())
        );
        assert_eq!(
            scenario.steps[1]
                .table
                .as_ref()
                .expect("the table remains")
                .rows[1],
            [nodes]
        );
        assert!(scenario.tags.iter().any(|tag| tag == "topology_example"));
    }
    assert_ne!(
        feature.scenarios[0].position.line,
        feature.scenarios[1].position.line
    );
}
