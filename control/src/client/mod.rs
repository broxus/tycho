use everscale_types::models::BlockId;
use tarpc::tokio_serde::formats::Bincode;
use tarpc::{client, context};
use tokio::net::ToSocketAddrs;
use tycho_core::block_strider::ManualGcTrigger;

use crate::error::ClientResult;
use crate::server::*;

pub struct ControlClient {
    inner: ControlServerClient,
}

impl ControlClient {
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> std::io::Result<Self> {
        let mut connect = tarpc::serde_transport::tcp::connect(addr, Bincode::default);
        connect.config_mut().max_frame_length(usize::MAX);
        let transport = connect.await?;

        let inner = ControlServerClient::new(client::Config::default(), transport).spawn();

        Ok(Self { inner })
    }

    pub async fn ping(&self) -> ClientResult<u64> {
        self.inner
            .ping(context::current())
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_archives_gc(&self, trigger: ManualGcTrigger) -> ClientResult<()> {
        self.inner
            .trigger_archives_gc(context::current(), trigger)
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_blocks_gc(&self, trigger: ManualGcTrigger) -> ClientResult<()> {
        self.inner
            .trigger_blocks_gc(context::current(), trigger)
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_states_gc(&self, trigger: ManualGcTrigger) -> ClientResult<()> {
        self.inner
            .trigger_states_gc(context::current(), trigger)
            .await
            .map_err(Into::into)
    }

    pub async fn set_memory_profiler_enabled(&self, enabled: bool) -> ClientResult<bool> {
        self.inner
            .set_memory_profiler_enabled(context::current(), enabled)
            .await
            .map_err(Into::into)
    }

    pub async fn get_block(&self, block_id: &BlockId) -> ClientResult<Option<Vec<u8>>> {
        let req = BlockRequest {
            block_id: *block_id,
        };
        match self.inner.get_block(context::current(), req).await?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }

    pub async fn get_block_proof(&self, block_id: &BlockId) -> ClientResult<Option<Vec<u8>>> {
        let req = BlockProofRequest {
            block_id: *block_id,
        };

        match self
            .inner
            .get_block_proof(context::current(), req)
            .await??
        {
            BlockProofResponse::Found { data } => Ok(Some(data)),
            BlockProofResponse::NotFound => Ok(None),
        }
    }

    pub async fn find_archive(&self, mc_seqno: u32) -> ClientResult<Option<ArchiveInfo>> {
        match self
            .inner
            .get_archive_info(context::current(), ArchiveInfoRequest { mc_seqno })
            .await??
        {
            ArchiveInfoResponse::Found(info) => Ok(Some(info)),
            ArchiveInfoResponse::NotFound => Ok(None),
        }
    }
}
