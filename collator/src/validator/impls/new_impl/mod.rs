use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use tycho_util::FastDashMap;

use self::session::ValidatorSession;
use crate::validator::{NetworkContext, Validator};

mod session;

#[derive(Clone)]
#[repr(transparent)]
pub struct ValidatorStdImpl {
    inner: Arc<Inner>,
}

#[async_trait]
impl Validator for ValidatorStdImpl {
    fn key_pair(&self) -> Arc<KeyPair> {
        self.inner.keypair.clone()
    }

    async fn add_session(
        &self,
        session_id: u32,
        shard_ident: &ShardIdent,
        validators: &[ValidatorDescription],
    ) -> Result<()> {
    }

    async fn validate(&self, session_id: u32, block_id: &BlockId) -> Result<()> {
        todo!()
    }

    async fn cancel_validation(&self, before: &BlockIdShort) -> Result<()> {
        todo!()
    }
}

struct Inner {
    net_context: NetworkContext,
    keypair: Arc<KeyPair>,
    sessions: FastDashMap<(ShardIdent, u32), ValidatorSession>,
}
