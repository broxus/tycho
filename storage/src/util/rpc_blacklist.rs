use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use everscale_types::models::StdAddr;
use serde::{Deserialize, Serialize};
use tycho_util::{serde_helpers, FastDashSet};

pub fn watcher_init(config: &Option<PathBuf>, blacklist: Arc<FastDashSet<[u8; 33]>>) {
    if let Some(rpc_blacklist_config) = config {
        let rpc_blacklist_config = rpc_blacklist_config.clone();
        tokio::spawn(async move {
            tracing::info!(
                logger_config = %rpc_blacklist_config.display(),
                "started watching for changes in rpc blacklist config"
            );

            let get_metadata = || {
                std::fs::metadata(&rpc_blacklist_config)
                    .ok()
                    .and_then(|m| m.modified().ok())
            };

            let handle = |config: BlackListConfig, blacklist: &ArcSwap<FastDashSet<[u8; 33]>>| {
                let new_list = Arc::new(
                    config
                        .accounts
                        .into_iter()
                        .map(|addr| {
                            let mut addr_bytes = [0u8; 33];
                            addr_bytes[0] = addr.workchain as u8;
                            addr_bytes[1..33].copy_from_slice(addr.address.as_slice());
                            addr_bytes
                        })
                        .collect::<FastDashSet<_>>(),
                );

                // Atomic swap blacklist values
                blacklist.store(new_list);
            };

            let blacklist = ArcSwap::from(blacklist);

            let mut last_modified = None;

            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                let modified = get_metadata();
                if last_modified == modified {
                    continue;
                }
                last_modified = modified;

                // Handle
                match BlackListConfig::load_from(&rpc_blacklist_config) {
                    Ok(config) => handle(config, &blacklist),
                    Err(e) => {
                        tracing::error!("failed to load logger config: {e}");
                    }
                }
            }
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct BlackListConfig {
    accounts: Vec<StdAddr>,
}

impl BlackListConfig {
    fn load_from<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        serde_helpers::load_json_from_file(path)
    }
}
