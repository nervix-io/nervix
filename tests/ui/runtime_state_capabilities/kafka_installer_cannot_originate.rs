use nervix_server::runtime::state_capability_compile_tests::KafkaOffsetSnapshotInstaller;

fn forbidden(installer: &KafkaOffsetSnapshotInstaller) {
    let _ = installer.apply_committed_offset("events", 0, 1);
}

fn main() {}
