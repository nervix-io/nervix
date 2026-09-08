use nervix_server::runtime::state_capability_compile_tests::KafkaOffsetStateRead;

fn forbidden(reader: &KafkaOffsetStateRead) {
    let _ = reader.install_snapshot(1, &[]);
}

fn main() {}
