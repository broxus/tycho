use std::path::Path;

use anyhow::anyhow;

fn main() -> anyhow::Result<()> {
    let input = ["rpc.proto"];

    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| anyhow!("project root dir not found"))?;
    let protos_dir = root
        .join("rpc")
        .join("src")
        .join("endpoint")
        .join("proto")
        .join("protos");

    prost_build::Config::new()
        .out_dir(&protos_dir)
        .include_file("mod.rs")
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
