use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "../proto/scheduler.proto";
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("scheduler_descriptor.bin"))
        .compile_protos(&[proto_file], &["../proto"])?;
    
    println!("cargo:rerun-if-changed={}", proto_file);
    
    Ok(())
}
