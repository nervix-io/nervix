use nervix_server::runtime::state_capability_compile_tests::MaterializedRelayStateRead;

fn forbidden(reader: &MaterializedRelayStateRead) {
    let _ = reader.remove_key(&None);
}

fn main() {}
