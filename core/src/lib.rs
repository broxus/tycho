pub mod block_strider;
pub mod blockchain_rpc;
pub mod global_config;
pub mod node;
pub mod overlay_client;
pub mod proto;
pub mod storage;

#[cfg(feature = "s3")]
pub mod s3;

mod util {
    pub(crate) mod downloader;
}

pub fn record_version_metric() {
    use std::sync::Once;

    static VERSION_METRIC: Once = Once::new();

    VERSION_METRIC.call_once(|| {
        let commit = option_env!("TYCHO_BUILD").unwrap_or("unknown");
        metrics::gauge!(
            "tycho_version",
            "crate" => "tycho-core",
            "version" => env!("CARGO_PKG_VERSION"),
            "commit" => commit,
        )
        .set(1.0);
    });
}
