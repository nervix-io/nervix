use nervix_server::runtime::state_capability_compile_tests::KafkaOffsetStateOriginator;

fn forbidden(originator: &KafkaOffsetStateOriginator) {
    let _ = originator.install_snapshot(1, &[]);
}

fn main() {}
