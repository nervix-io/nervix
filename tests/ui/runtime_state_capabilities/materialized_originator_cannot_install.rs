use nervix_server::runtime::state_capability_compile_tests::MaterializedRelayStateOriginator;

fn forbidden(originator: &MaterializedRelayStateOriginator) {
    let _ = originator.install_snapshot(1, &[]);
}

fn main() {}
