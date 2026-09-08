use nervix_server::runtime::state_capability_compile_tests::KafkaOffsetStateRead;

fn forbidden(reader: &KafkaOffsetStateRead) {
    let _ = reader.apply_committed_offset("events", 0, 1);
}

fn main() {}
