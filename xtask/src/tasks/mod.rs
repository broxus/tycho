pub mod gen_network;
pub mod gen_proto;

use std::path::{Path, PathBuf};

/// Get the project root directory
pub fn project_root() -> PathBuf {
    Path::new(&env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(1)
        .unwrap()
        .to_path_buf()
}
