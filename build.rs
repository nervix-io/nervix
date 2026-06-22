fn main() {
    println!("cargo:rustc-check-cfg=cfg(runtime_ack_loom)");
}
