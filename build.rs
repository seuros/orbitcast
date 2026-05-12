fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/anycable.proto");
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(&["proto/anycable.proto"], &["proto"])?;
    Ok(())
}
