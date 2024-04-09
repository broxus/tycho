use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::FutureExt;

use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::{BlockHandle, BlockMetaData, Storage};

use super::subscriber::BlockSubscriber;

pub struct ShardStateUpdater<S> {
    min_ref_mc_state_tracker: MinRefMcStateTracker,

    storage: Arc<Storage>,
    state_subscriber: Arc<S>,
}

impl<S> ShardStateUpdater<S>
where
    S: BlockSubscriber,
{
    pub(crate) fn new(
        min_ref_mc_state_tracker: MinRefMcStateTracker,
        storage: Arc<Storage>,
        state_subscriber: S,
    ) -> Self {
        Self {
            min_ref_mc_state_tracker,
            storage,
            state_subscriber: Arc::new(state_subscriber),
        }
    }
}

impl<S> BlockSubscriber for ShardStateUpdater<S>
where
    S: BlockSubscriber,
{
    type HandleBlockFut = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

    fn handle_block(
        &self,
        block: &BlockStuff,
        _state: Option<&ShardStateStuff>,
    ) -> Self::HandleBlockFut {
        tracing::info!(id = ?block.id(), "applying block");
        let block = block.clone();
        let min_ref_mc_state_tracker = self.min_ref_mc_state_tracker.clone();
        let storage = self.storage.clone();
        let subscriber = self.state_subscriber.clone();

        async move {
            let block_h = Self::get_block_handle(&block, &storage)?;

            let (prev_id, _prev_id_2) = block //todo: handle merge
                .construct_prev_id()
                .context("Failed to construct prev id")?;

            let prev_state = storage
                .shard_state_storage()
                .load_state(&prev_id)
                .await
                .context("Prev state should exist")?;

            let start = std::time::Instant::now();
            let new_state = Self::compute_and_store_state_update(
                &block,
                &min_ref_mc_state_tracker,
                storage,
                &block_h,
                prev_state,
            )
            .await?;
            let elapsed = start.elapsed();
            metrics::histogram!("tycho_subscriber_compute_and_store_state_update_seconds")
                .record(elapsed);

            let gen_utime = block_h.meta().gen_utime() as f64;
            let seqno = block_h.id().seqno as f64;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();

            if block.id().is_masterchain() {
                metrics::gauge!("tycho_last_mc_block_utime").set(gen_utime);
                metrics::gauge!("tycho_last_mc_block_seqno").set(seqno);
                metrics::gauge!("tycho_last_mc_block_applied").set(now);
            } else {
                metrics::gauge!("tycho_last_shard_block_utime").set(gen_utime);
                metrics::gauge!("tycho_last_shard_block_seqno").set(seqno);
                metrics::gauge!("tycho_last_shard_block_applied").set(now);
            }

            let start = std::time::Instant::now();
            subscriber
                .handle_block(&block, Some(&new_state))
                .await
                .context("Failed to notify subscriber")?;
            let elapsed = start.elapsed();
            metrics::histogram!("tycho_subscriber_handle_block_seconds").record(elapsed);

            Ok(())
        }
        .boxed()
    }
}

impl<S> ShardStateUpdater<S>
where
    S: BlockSubscriber,
{
    fn get_block_handle(block: &BlockStuff, storage: &Arc<Storage>) -> Result<Arc<BlockHandle>> {
        let info = block
            .block()
            .info
            .load()
            .context("Failed to load block info")?;

        let (block_h, _) = storage
            .block_handle_storage()
            .create_or_load_handle(
                block.id(),
                BlockMetaData {
                    is_key_block: info.key_block,
                    gen_utime: info.gen_utime,
                    mc_ref_seqno: info
                        .master_ref
                        .map(|r| {
                            r.load()
                                .context("Failed to load master ref")
                                .map(|mr| mr.seqno)
                        })
                        .transpose()
                        .context("Failed to process master ref")?,
                },
            )
            .context("Failed to create or load block handle")?;

        Ok(block_h)
    }

    async fn compute_and_store_state_update(
        block: &BlockStuff,
        min_ref_mc_state_tracker: &MinRefMcStateTracker,
        storage: Arc<Storage>,
        block_h: &Arc<BlockHandle>,
        prev_state: Arc<ShardStateStuff>,
    ) -> Result<ShardStateStuff> {
        let update = block
            .block()
            .load_state_update()
            .context("Failed to load state update")?;

        let new_state =
            tokio::task::spawn_blocking(move || update.apply(&prev_state.root_cell().clone()))
                .await
                .context("Failed to join blocking task")?
                .context("Failed to apply state update")?;
        let new_state = ShardStateStuff::new(*block.id(), new_state, min_ref_mc_state_tracker)
            .context("Failed to create new state")?;

        storage
            .shard_state_storage()
            .store_state(block_h, &new_state)
            .await
            .context("Failed to store new state")?;

        Ok(new_state)
    }
}

#[cfg(test)]
pub mod test {
    use super::super::test_provider::archive_provider::ArchiveProvider;
    use super::*;

    use crate::block_strider::subscriber::PrintSubscriber;
    use crate::block_strider::BlockStrider;
    use everscale_types::cell::HashBytes;
    use everscale_types::models::BlockId;
    use everscale_types::models::ShardIdent;
    use std::str::FromStr;
    use tracing_test::traced_test;
    use tycho_storage::{BlockMetaData, Db, DbOptions, Storage};

    #[tokio::test]
    #[traced_test]
    async fn test_state_apply() -> anyhow::Result<()> {
        let (provider, storage) = prepare_state_apply().await?;

        let last_mc = *provider.mc_block_ids.last_key_value().unwrap().1;

        let block_strider = BlockStrider::builder()
            .with_provider(provider)
            .with_subscriber(PrintSubscriber)
            .with_state(storage.clone())
            .build_with_state_applier(MinRefMcStateTracker::default(), storage.clone());

        block_strider.run().await?;

        assert_eq!(
            storage.node_state().load_last_mc_block_id().unwrap(),
            last_mc
        );

        Ok(())
    }

    pub async fn prepare_state_apply() -> Result<(ArchiveProvider, Arc<Storage>)> {
        let data = include_bytes!("../../tests/00001");
        let provider = ArchiveProvider::new(data).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().to_path_buf(), DbOptions::default()).unwrap();
        let storage = Storage::new(db, temp.path().join("file"), 1_000_000).unwrap();

        let master = include_bytes!("../../tests/everscale_zerostate.boc");
        let shard = include_bytes!("../../tests/everscale_shard_zerostate.boc");

        let master_id = BlockId {
            root_hash: HashBytes::from_str(
                "58ffca1a178daff705de54216e5433c9bd2e7d850070d334d38997847ab9e845",
            )
            .unwrap(),
            file_hash: HashBytes::from_str(
                "d270b87b2952b5ba7daa70aaf0a8c361befcf4d8d2db92f9640d5443070838e4",
            )
            .unwrap(),
            shard: ShardIdent::MASTERCHAIN,
            seqno: 0,
        };
        let master = ShardStateStuff::deserialize_zerostate(master_id, master).unwrap();

        // Parse block id
        let block_id = BlockId::from_str("-1:8000000000000000:0:58ffca1a178daff705de54216e5433c9bd2e7d850070d334d38997847ab9e845:d270b87b2952b5ba7daa70aaf0a8c361befcf4d8d2db92f9640d5443070838e4")?;

        // Write zerostate to db
        let (handle, _) = storage.block_handle_storage().create_or_load_handle(
            &block_id,
            BlockMetaData::zero_state(master.state().gen_utime),
        )?;

        storage
            .shard_state_storage()
            .store_state(&handle, &master)
            .await?;

        let shard_id = BlockId {
            root_hash: HashBytes::from_str(
                "95f042d1bf5b99840cad3aaa698f5d7be13d9819364faf9dd43df5b5d3c2950e",
            )
            .unwrap(),
            file_hash: HashBytes::from_str(
                "97af4602a57fc884f68bb4659bab8875dc1f5e45a9fd4fbafd0c9bc10aa5067c",
            )
            .unwrap(),
            shard: ShardIdent::BASECHAIN,
            seqno: 0,
        };

        //store workchain zerostate
        let shard = ShardStateStuff::deserialize_zerostate(shard_id, shard).unwrap();
        let (handle, _) = storage.block_handle_storage().create_or_load_handle(
            &shard_id,
            BlockMetaData::zero_state(shard.state().gen_utime),
        )?;
        storage
            .shard_state_storage()
            .store_state(&handle, &shard)
            .await?;

        storage
            .node_state()
            .store_last_mc_block_id(&master_id)
            .unwrap();
        Ok((provider, storage))
    }
}
