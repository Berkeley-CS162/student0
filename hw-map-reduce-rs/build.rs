/// Compiles protocol buffer code using [`tonic_prost_build`].
fn main() {
    // Ignore errors (the directory may already exist)
    let _ = std::fs::create_dir("src/proto");

    #[cfg(feature = "autograder")]
    let _ = std::fs::create_dir("src/autograder/rpc/");

    tonic_prost_build::configure()
        .out_dir("src/proto")
        .compile_with_config(
            config(),
            &["proto/coordinator.proto", "proto/worker.proto"],
            &["proto/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos: {:?}", e));

    #[cfg(feature = "autograder")]
    tonic_prost_build::configure()
        .out_dir("src/autograder/rpc")
        .compile_with_config(
            config(),
            &["src/autograder/proto/grader.proto"],
            &["src/autograder/proto/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile autograder protos: {:?}", e));
}

/// Uses a vendored `protoc` binary so that no system install is required.
fn config() -> tonic_prost_build::Config {
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .unwrap_or_else(|e| panic!("Failed to find vendored protoc: {:?}", e));
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc);
    config
}
