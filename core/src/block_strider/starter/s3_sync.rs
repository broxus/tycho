use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use object_store::DynObjectStore;
use object_store::path::Path;
use tycho_types::models::BlockId;

use crate::block_strider::starter::{S3Config, S3ProviderConfig};
use crate::storage::CoreStorage;

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
        let prefix_path = S3FileKind::ShardState.prefix(true);

        let mut closest: Option<BlockId> = None;

        let mut list = self
            .s3_client
            .list(Some(&Path::from(prefix_path.to_string_lossy().as_ref())));
        while let Some(item) = list.next().await {
            let item = item?;

            let filename = item
                .location
                .filename()
                .ok_or_else(|| anyhow!("no filename in path"))?;

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum S3FileKind {
    BlockData,
    BlockProof,
    QueueDiff,
    ShardState,
    QueueState,
}

impl S3FileKind {
    const BLOCK_DATA_DIR: &'static str = "data";
    const BLOCK_PROOF_DIR: &'static str = "proof";
    const QUEUE_DIFF_DIR: &'static str = "diff";

    const SHARD_STATE_DIR: &'static str = "boc";
    const QUEUE_STATE_DIR: &'static str = "queue";

    pub fn make_path(&self, block_id: &BlockId) -> PathBuf {
        self.prefix(block_id.is_masterchain())
            .join(block_id.to_string())
    }

    pub fn prefix(&self, is_masterchain: bool) -> PathBuf {
        let base = match self {
            Self::ShardState | Self::QueueState => "states",
            Self::BlockData | Self::BlockProof | Self::QueueDiff => "blocks",
        };

        let chain = Self::chain(is_masterchain);
        let type_dir = self.subdir();

        PathBuf::from(base).join(chain).join(type_dir)
    }

    fn subdir(&self) -> &'static str {
        match self {
            Self::BlockData => Self::BLOCK_DATA_DIR,
            Self::BlockProof => Self::BLOCK_PROOF_DIR,
            Self::QueueDiff => Self::QUEUE_DIFF_DIR,
            Self::ShardState => Self::SHARD_STATE_DIR,
            Self::QueueState => Self::QUEUE_STATE_DIR,
        }
    }

    fn chain(is_masterchain: bool) -> &'static str {
        if is_masterchain { "master" } else { "shards" }
    }
}
