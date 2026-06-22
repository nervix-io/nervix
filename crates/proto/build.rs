fn main() {
    println!("cargo:rerun-if-changed=../../proto/io/nervix/api/v1/nervix.proto");
    let build_client = std::env::var_os("CARGO_FEATURE_CLIENT").is_some();
    let build_server = std::env::var_os("CARGO_FEATURE_SERVER").is_some();
    tonic_build::configure()
        .build_server(build_server)
        .build_client(build_client)
        .compile_protos(
            &["../../proto/io/nervix/api/v1/nervix.proto"],
            &["../../proto"],
        )
        .expect("failed to compile protobuf definitions");
}
