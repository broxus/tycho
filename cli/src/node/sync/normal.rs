use std::sync::Arc;

use anyhow::Context;
use everscale_types::models::BlockId;
use tokio::sync::mpsc;
use tycho_block_util::archive::Archive;
use tycho_block_util::block::{BlockProofStuffAug, BlockStuffAug};
use tycho_storage::BlockHandle;

use crate::node::sync::archives::ArchivesDownloader;
use crate::node::Node;

pub async fn run(node: &Arc<Node>) -> anyhow::Result<()> {
    tracing::info!(target: "sync", "started normal sync");

    let last_mc_block_id = node
        .storage
        .node_state()
        .load_last_mc_block_id()
        .expect("shouldn't happen");

    tracing::info!(
        target: "sync",
        %last_mc_block_id,
        "creating archives downloader"
    );

    let archives_downloader = ArchivesDownloader::new(node);

    let (tx, mut rx) = mpsc::unbounded_channel();
    archives_downloader.run(last_mc_block_id.seqno + 1, tx);

    while let Some(archive) = rx.recv().await {
        let archive = archive?;

        // TODO: apply blocks

        if node.is_synced()? {
            break;
        }
    }

    Ok(())
}
