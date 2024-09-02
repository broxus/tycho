fn main() -> std::io::Result<()> {
    let mut prost_build = prost_build::Config::new();

    // Replace Vec<u8> to Bytes
    prost_build.bytes(["."]);

    prost_build.compile_protos(&["src/endpoint/proto/rpc.proto"], &["src/"])
}
