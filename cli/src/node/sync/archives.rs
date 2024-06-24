use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::BlockId;
use tokio::sync::mpsc::UnboundedSender;
use tycho_block_util::archive::{Archive, ArchiveWritersPool};

use crate::node::Node;

pub struct ArchivesDownloader {
    node: Arc<Node>,
    writers_pool: ArchiveWritersPool,
}

impl ArchivesDownloader {
    pub fn new(node: &Arc<Node>) -> Self {
        // TODO: add to config
        let save_to_disk_threshold = Default::default();

        Self {
            node: node.clone(),
            writers_pool: ArchiveWritersPool::new(
                node.storage.root().path(),
                save_to_disk_threshold,
            ),
        }
    }

    pub fn run(&self, mut last_mc_block_id: u32, archives_tx: UnboundedSender<Result<Archive>>) {
        tokio::spawn({
            let writers_pool = self.writers_pool.clone();
            let blockchain_rpc_client = self.node.blockchain_rpc_client.clone();

            async move {
                loop {
                    let mut writer = writers_pool.acquire();

                    if let Err(e) = blockchain_rpc_client
                        .download_archive(last_mc_block_id, &mut writer)
                        .await
                    {
                        let is_sent = archives_tx.send(Err(e.into())).is_ok();

                        tracing::error!(last_mc_block_id, is_sent, "failed to download archive");
                        break;
                    }

                    let archive = match writer.parse_archive() {
                        Ok(archive) => archive,
                        Err(e) => {
                            tracing::error!(
                                last_mc_block_id,
                                "failed to parse downloaded archive: {e}"
                            );
                            continue;
                        }
                    };

                    if archives_tx.send(Ok(archive)).is_err() {
                        tracing::info!(last_mc_block_id, "stop downloading archives");
                        break;
                    }

                    last_mc_block_id += 1;
                }
            }
        });
    }
}
