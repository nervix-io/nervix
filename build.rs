fn main() {
    println!("cargo:rustc-check-cfg=cfg(runtime_ack_loom)");
    println!("cargo:rustc-check-cfg=cfg(relay_dispatch_gate_loom)");
}
