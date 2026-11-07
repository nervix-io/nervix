//! Repository test harness, outside the product layer order.
//! Owns: isolated Cargo-process observations without invoking a compiler.
//! Depends on: current Cargo/report shapes, a private filesystem and Git discovery.
//! Must not know: real compiler behavior or other worktrees' environment.

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
};

use meticulous::ResultExt as _;
use serde_json::json;
use tempfile::TempDir;

use crate::{
    admission::Policy, configuration::Configuration, identity, io, process, runner::Runner,
};

pub struct Harness {
    pub directory: TempDir,
    pub runner: Runner,
    pub configuration: Configuration,
}

pub fn executable(path: &Path, source: &str) {
    fs::write(path, source).assured("the private process fixture can be written");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .assured("the private process fixture can execute");
}

impl Harness {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().assured("the isolated process fixture can be created");
        let root = directory.path();
        fs::create_dir_all(root.join("src")).assured("the fixture source directory can be created");
        fs::create_dir_all(root.join("target/bin"))
            .assured("the fixture executable directory can be created");
        fs::write(root.join("src/lib.rs"), "pub fn empty() {}\n")
            .assured("the fixture source can be written");
        fs::write(root.join(".gitignore"), "target/\n")
            .assured("process observations stay outside source inputs");
        process::output("git", &["init".into(), "-q".into()], root, &BTreeMap::new())
            .assured("the isolated fixture can initialize Git discovery");
        let target = json!({"name":"example", "kind":["lib"], "src_path":root.join("src/lib.rs")});
        io::atomic_json(&root.join("target/metadata.json"), &json!({"workspace_members":["example-id"], "packages":[{"id":"example-id", "name":"example", "targets":[target]}]})).assured("the current metadata fixture serializes");
        executable(&root.join("target/bin/cargo"), CARGO);
        let inherited_path =
            std::env::var("PATH").assured("the test process has its executable search path");
        let environment = BTreeMap::from([
            (
                "PATH".into(),
                format!("{}:{inherited_path}", root.join("target/bin").display()),
            ),
            ("NERVIX_TEST_ROOT".into(), root.display().to_string()),
            ("NERVIX_LINT_MODE".into(), "gate".into()),
            ("RUSTC_WRAPPER".into(), "kache".into()),
            ("KACHE_KEY_SALT".into(), "qualified".into()),
        ]);
        let runner = Runner {
            root: root.into(),
            target: root.join("target"),
            driver: root.join("target/driver"),
            wrapper: root.join("target/wrapper"),
            policy: Policy {
                directory: root.join("target/queue"),
                threshold_bytes: 4096,
                wait_limit_seconds: 60.0,
                reservations: BTreeMap::new(),
            },
            compiler: "qualified compiler".into(),
            identity: "current worktree fingerprint".into(),
            inputs: identity::source_inputs(root).assured("fixture source inputs can be captured"),
            environment,
            work: root.join("target/evidence"),
        };
        let configuration = Configuration {
            name: "ordinary".into(),
            manifest: "Cargo.toml".into(),
            packages: vec!["example".into()],
            kinds: vec!["lib".into()],
            features: Vec::new(),
            target: None,
        };
        Self {
            directory,
            runner,
            configuration,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.runner.environment.insert(key.into(), value.into());
    }
    pub fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join("target").join(name)
    }
    pub fn runs(&self) -> usize {
        match fs::read_to_string(self.path("ordinary.count")) {
            Ok(text) => text
                .parse()
                .assured("the process fixture writes an integer run count"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => panic!("the process fixture count cannot be read: {error}"),
        }
    }
}

const CARGO: &str = r#"#!/usr/bin/env python3
import json, os, pathlib, sys, time
root = pathlib.Path(os.environ['NERVIX_TEST_ROOT'])
state = root / 'target'
args = sys.argv[1:]
if 'metadata' in args:
    print((state / 'metadata.json').read_text())
    sys.exit(0)
if 'clean' in args:
    (state / 'clean.json').write_text(json.dumps({'args':args, 'wrapper':os.environ.get('RUSTC_WRAPPER')}))
    sys.exit(0)
name = os.environ['NERVIX_LINT_CONFIGURATION']
count_path = state / (name + '.count')
count = int(count_path.read_text()) + 1 if count_path.exists() else 1
count_path.write_text(str(count))
captured = {key:value for key,value in os.environ.items() if key in ['RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','KACHE_KEY_SALT','NERVIX_LINT_ADMISSION','LLVM_PROFILE_FILE']}
(state / (name + '.environment.json')).write_text(json.dumps(captured))
(state / (name + '.started')).write_text('started')
time.sleep(float(os.environ.get('NERVIX_TEST_DELAY', '0')))
reports = pathlib.Path(os.environ['NERVIX_LINT_REPORTS'])
source = root / 'src/lib.rs'
artifact = state / ('libexample-' + name + '.rmeta')
artifact.write_text('current metadata')
report = dict(compiler='qualified compiler', identity=os.environ['NERVIX_LINT_IDENTITY'], configuration=name, crate_name='example', crate_source=str(source), arguments=[], findings=[], audit=[], excluded_generated=0, excluded_external=0, complete=True)
if count >= int(os.environ.get('NERVIX_TEST_REPORT_AFTER', '1')):
    (reports / ('example-' + name + '.json')).write_text(json.dumps(report))
records = reports / 'compilations'
records.mkdir(exist_ok=True)
if 'NERVIX_TEST_OMIT_RECORD' not in os.environ:
    (records / ('example-' + name + '.json')).write_text(json.dumps(dict(crate_name='example', crate_types=['lib'], extra_filename='-' + name, source=str(source), ending={'kind':'exit','status':0}, peak_resident_bytes=int(os.environ.get('NERVIX_TEST_PEAK','1024')), started_unix_ns=1, seconds=0.1, oom_kills=0, admission=None)))
if 'NERVIX_TEST_CHANGE_SOURCE' in os.environ: source.write_text(source.read_text() + '\n// Changed during analysis.\n')
target = dict(name='example', kind=['lib'], src_path=str(source))
print(json.dumps(dict(reason='compiler-artifact', package_id='example-id', target=target, filenames=[str(artifact)], fresh='NERVIX_TEST_FRESH' in os.environ)))
success = name != os.environ.get('NERVIX_TEST_FAIL')
print(json.dumps(dict(reason='build-finished', success=success)))
(state / (name + '.finished')).write_text('finished')
sys.exit(0 if success else 1)
"#;
