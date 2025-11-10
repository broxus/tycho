use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use object_store::DynObjectStore;
use object_store::path::Path;
use tycho_block_util::block::BlockStuff;
use tycho_types::models::BlockId;

use crate::block_strider::starter::{S3Config, S3ProviderConfig};
use crate::storage::{BlockHandle, CoreStorage, PersistentStateKind};

pub const S3_CHUNK_SIZE: usize = 10 * 1024 * 1024;

pub struct S3Starter {
    seqno: Option<u32>,
    storage: CoreStorage,
    s3_client: Arc<DynObjectStore>,
}

impl S3Starter {
    pub fn new(config: &S3Config, storage: &CoreStorage) -> Result<Self> {
        let s3_client: Arc<DynObjectStore> = match &config.s3_provider {
            S3ProviderConfig::Aws {
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
            } => Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(&config.bucket_name)
                    .with_endpoint(endpoint)
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key)
                    .with_client_options(
                        object_store::ClientOptions::new().with_allow_http(*allow_http),
                    )
                    .build()?,
            ),
            S3ProviderConfig::Gcs { credentials_path } => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::new()
                    .with_client_options(
                        object_store::ClientOptions::new()
                            .with_connect_timeout_disabled()
                            .with_timeout_disabled(),
                    )
                    .with_bucket_name(&config.bucket_name)
                    .with_application_credentials(credentials_path)
                    .build()?,
            ),
        };

        Ok(Self {
            seqno: config.seqno,
            storage: storage.clone(),
            s3_client,
        })
    }

    pub async fn choose_key_block(&self) -> Result<BlockId> {
        let target_seqno = self.seqno.unwrap_or(u32::MAX);
        let prefix_path = Path::from("states");

        let mut closest: Option<BlockId> = None;

        let mut list = self.s3_client.list(Some(&prefix_path));
        while let Some(item) = list.next().await {
            let item = item?;

            let Some(extension) = item.location.extension() else {
                continue;
            };

            if PersistentStateKind::from_extension(extension) != Some(PersistentStateKind::Shard) {
                continue;
            }

            let filename = item
                .location
                .filename()
                .and_then(|f| f.rsplit_once('.').map(|(name, _)| name))
                .ok_or_else(|| anyhow!("invalid filename format for persistent state"))?;

            let block_id = BlockId::from_str(filename)?;

            if block_id.seqno >= target_seqno {
                continue;
            }

            if closest.as_ref().is_none_or(|c| block_id.seqno > c.seqno) {
                closest = Some(block_id);
            }
        }

        closest.ok_or_else(|| anyhow!("key block not found"))
    }

    pub async fn download_start_blocks_and_states(&self, mc_block_id: &BlockId) -> Result<()> {
        // Download and save masterchain block and state
        let (_, init_mc_block) = self
            .download_block_with_states(mc_block_id, mc_block_id)
            .await?;

        tracing::info!(
            block_id = %init_mc_block.id(),
            "downloaded init mc block state"
        );

        // Download and save blocks and states from other shards
        for (_, block_id) in init_mc_block.shard_blocks()? {
            let (handle, _) = self
                .download_block_with_states(mc_block_id, &block_id)
                .await?;

            self.storage
                .block_handle_storage()
                .set_block_committed(&handle);
        }

        Ok(())
    }

    async fn download_block_with_states(
        &self,
        _mc_block_id: &BlockId,
        _block_id: &BlockId,
    ) -> Result<(BlockHandle, BlockStuff)> {
        todo!()
    }
}
