use nervix_server::runtime::state_capability_compile_tests::MaterializedRelaySnapshotInstaller;

fn forbidden(installer: &MaterializedRelaySnapshotInstaller) {
    let _ = installer.remove_key(&None);
}

fn main() {}
