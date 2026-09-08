use nervix_server::runtime::state_capability_compile_tests::MaterializedRelayStateRead;

fn forbidden(reader: &MaterializedRelayStateRead) {
    let _ = reader.install_snapshot(1, &[]);
}

fn main() {}
