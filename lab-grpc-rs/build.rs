/// Compiles protocol buffer code using [`tonic_prost_build`].
fn main() {
    // Use a vendored protoc so students don't need one installed.
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("Failed to find vendored protoc");
    // SAFETY: build scripts are single-threaded at this point.
    unsafe { std::env::set_var("PROTOC", protoc) };

    // Ignore errors (the directory may already exist)
    let _ = std::fs::create_dir("src/rpc");

    tonic_prost_build::configure()
        .out_dir("src/rpc")
        .compile_protos(&["proto/kv_store.proto"], &["proto/"])
        .unwrap_or_else(|e| panic!("Failed to compile protos: {:?}", e));
}
