//! Repository test harness, outside the product layer order.
//! Owns: declaration normalization and malformed-declaration refusals.
//! Depends on: production parsing and current matrix values.
//! Must not know: running compilers or host memory.

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;

const CONFIGURATION: &str =
    "[[configuration]]\nname='ordinary'\nmanifest='Cargo.toml'\npackages=[]\n";
const ADMISSION: &str = "[admission]\nthreshold_gib=4\nwait_limit_minutes=30\nreservation=[]\n";

#[test]
fn cargo_arguments_preserve_packages_modes_kinds_and_cross_targets() {
    let configuration = Configuration {
        name: "selected".into(),
        manifest: "Cargo.toml".into(),
        packages: vec!["one".into(), "two".into()],
        kinds: vec!["lib".into(), "bin".into()],
        features: vec!["native".into(), "shuttle".into()],
        target: Some("native-target".into()),
    };
    assert_eq!(
        configuration.cargo_arguments(),
        [
            "--manifest-path",
            "Cargo.toml",
            "--package",
            "one",
            "--package",
            "two",
            "--lib",
            "--bins",
            "--features",
            "native shuttle",
            "--target",
            "native-target"
        ]
    );
    assert!(
        Configuration::fixture("ordinary")
            .cargo_arguments()
            .contains(&"--workspace".into())
    );
}

#[test]
fn diagnostic_configurations_keep_testing_and_browser_kinds() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .assured("the crate is tools/nervix-lint/ratchet");
    let declaration = Declaration::load(root).assured("the repository matrix is valid");
    let names: BTreeSet<_> = declaration
        .configurations
        .iter()
        .map(|configuration| configuration.name.as_str())
        .collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "ordinary",
            "browser",
            "testing",
            "shuttle",
            "loom",
            "turmoil",
            "deloxide",
            "deloxide-order",
            "deloxide-stress",
            "deloxide-binaries",
            "deloxide-order-binaries",
            "deloxide-stress-binaries"
        ])
    );
    let mut diagnostic = 0;
    for configuration in &declaration.configurations {
        if configuration.features.iter().any(|feature| {
            matches!(
                feature.as_str(),
                "deloxide" | "deloxide-order" | "deloxide-stress"
            )
        }) {
            diagnostic += 1;
            assert!(configuration.features.contains(&"testing".into()));
        }
        if configuration.name == "browser" {
            assert_eq!(
                configuration.target.as_deref(),
                Some("wasm32-unknown-unknown")
            );
            assert_eq!(configuration.kinds, ["lib", "bin"]);
        }
    }
    assert_eq!(diagnostic, 6);
    let policy = declaration
        .policy(root)
        .assured("the admission declaration is valid");
    assert_eq!(policy.directory, host_directory());
    assert!(
        policy
            .reservation(&Compiled {
                crate_name: "nervix_server".into(),
                kind: "lib".into()
            })
            .assured("the server library has its declared reservation")
            > policy.threshold_bytes
    );
}

#[test]
fn invalid_configurations_and_admission_name_their_declaration() {
    let directory = tempfile::tempdir().assured("the declaration fixture can create its directory");
    let path = directory.path().join(TOOLING).join("configurations.toml");
    fs::create_dir_all(path.parent().assured("the declaration has a parent"))
        .assured("the declaration directory can be created");
    let valid = format!("{CONFIGURATION}{ADMISSION}");
    fs::write(&path, &valid).assured("the valid declaration can be written");
    let normalized =
        Declaration::load(directory.path()).assured("optional declaration fields normalize once");
    assert_eq!(normalized.configurations[0].kinds, ["lib"]);
    assert!(normalized.configurations[0].features.is_empty());
    for text in [
        format!("{CONFIGURATION}{CONFIGURATION}{ADMISSION}"),
        format!("{CONFIGURATION}features=['loom','shuttle']\n{ADMISSION}"),
        format!("{CONFIGURATION}kinds=['test']\n{ADMISSION}"),
        format!("{CONFIGURATION}unknown=true\n{ADMISSION}"),
        format!("configuration=[]\n{ADMISSION}"),
        valid.replace("threshold_gib=4", "threshold_gib=0"),
        valid.replace("threshold_gib=4", "threshold_gib=true"),
        valid.replace("wait_limit_minutes=30", "wait_limit_minutes=0.5"),
        valid.replace("reservation=[]", "reservation='example'"),
        valid.replace("reservation=[]", "reservation=[1]"),
        valid.replace("reservation=[]", "unknown=1\nreservation=[]"),
        valid.replace(
            "reservation=[]",
            "[[admission.reservation]]\ncrate='example'\nkind='lib'\ngib=4",
        ),
        valid.replace(
            "reservation=[]",
            "[[admission.reservation]]\ncrate=''\nkind='lib'\ngib=16",
        ),
        valid.replace(
            "reservation=[]",
            "[[admission.reservation]]\ncrate='example'\nkind='test'\ngib=16",
        ),
        valid.replace(
            "reservation=[]",
            "[[admission.reservation]]\ncrate='example'\nkind='lib'\ngib=16\n[[admission.\
             reservation]]\ncrate='example'\nkind='lib'\ngib=16",
        ),
    ] {
        fs::write(&path, text).assured("the malformed current declaration can be written");
        let report = Declaration::load(directory.path())
            .err()
            .assured("the malformed declaration is refused");
        assert!(format!("{report:#}").contains("tools/nervix-lint/configurations.toml"));
    }
}

#[test]
fn completed_configuration_evidence_never_defaults_missing_fields() {
    let value = serde_json::to_value(Configuration::fixture("ordinary"))
        .assured("the current configuration serializes");
    for field in ["kinds", "features", "target"] {
        let mut incomplete = value.clone();
        incomplete
            .as_object_mut()
            .assured("the configuration is an object")
            .remove(field);
        assert!(serde_json::from_value::<Configuration>(incomplete).is_err());
    }
}
