pub fn run() -> anyhow::Result<()> {
    let input = ["rpc.proto"];

    let root = super::project_root();
    let protos_dir = root
        .join("rpc")
        .join("src")
        .join("endpoint")
        .join("proto")
        .join("protos");

    prost_build::Config::new()
        .out_dir(&protos_dir)
        .include_file("mod.rs")
        // For old protoc versions
        .protoc_arg("--experimental_allow_proto3_optional")
        // Replace Vec<u8> to Bytes
        .bytes(["."])
        .compile_protos(
            &input
                .into_iter()
                .map(|x| protos_dir.join(x))
                .collect::<Vec<_>>(),
            &[protos_dir],
        )?;

    Ok(())
}
