use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use scc::TreeIndex;
use serde::{Deserialize, Serialize};
use tycho_util::{serde_helpers, FastHashMap};

use self::session::ValidatorSession;
use crate::validator::rpc::ExchangeSignaturesBackoff;
use crate::validator::{BriefValidatorDescr, ValidatorNetworkContext, ValidationStatus, Validator};

mod session;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorStdImplConfig {
    /// Backoff configuration for exchanging signatures.
    pub exchange_signatures_backoff: ExchangeSignaturesBackoff,

    /// Timeout for exchanging signatures request.
    ///
    /// Default: 1 second.
    #[serde(with = "serde_helpers::humantime")]
    pub exchange_signatures_timeout: Duration,

    /// Interval for failed exchange retries.
    ///
    /// Default: 10 seconds.
    pub failed_exchange_interval: Duration,
}

impl Default for ValidatorStdImplConfig {
    fn default() -> Self {
        Self {
            exchange_signatures_backoff: Default::default(),
            exchange_signatures_timeout: Duration::from_secs(1),
            failed_exchange_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ValidatorStdImpl {
    inner: Arc<Inner>,
}

impl ValidatorStdImpl {
    pub fn new(
        net_context: ValidatorNetworkContext,
        keypair: Arc<KeyPair>,
        config: ValidatorStdImplConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                net_context,
                keypair,
                sessions: Default::default(),
                config,
            }),
        }
    }
}

#[async_trait]
impl Validator for ValidatorStdImpl {
    fn key_pair(&self) -> Arc<KeyPair> {
        self.inner.keypair.clone()
    }

    fn add_session(
        &self,
        shard_ident: &ShardIdent,
        session_id: u32,
        validators: &[ValidatorDescription],
    ) -> Result<()> {
        let mut validators_map = FastHashMap::default();
        for descr in validators {
            // TODO: Skip invalid entries? But what should we do with the total weight?
            let validator_info = BriefValidatorDescr::try_from(descr)?;
            validators_map.insert(validator_info.peer_id, validator_info);
        }

        let peer_id = self.inner.net_context.network.peer_id();
        if !validators_map.contains_key(peer_id) {
            tracing::warn!(
                %peer_id,
                %shard_ident,
                session_id,
                "this node is not in the validator set"
            );
            return Ok(());
        }

        let session = ValidatorSession::new(
            shard_ident,
            session_id,
            validators_map,
            &self.inner.net_context,
            self.inner.keypair.clone(),
            &self.inner.config,
        )?;
        if self
            .inner
            .sessions
            .insert((*shard_ident, session_id), session)
            .is_err()
        {
            anyhow::bail!("validator session already exists: ({shard_ident}, {session_id})");
        }

        Ok(())
    }

    async fn validate(&self, session_id: u32, block_id: &BlockId) -> Result<ValidationStatus> {
        let Some(session) = self
            .inner
            .sessions
            .peek(&(block_id.shard, session_id), &scc::ebr::Guard::new())
            .map(|s| s.clone())
        else {
            anyhow::bail!(
                "validator session not found: ({}, {session_id})",
                block_id.shard
            );
        };

        session.validate_block(block_id).await
    }

    fn cancel_validation(&self, before: &BlockIdShort) -> Result<()> {
        let guard = scc::ebr::Guard::new();

        let session = {
            // Find the latest session and remove all previous ones.
            let mut latest_session = None;
            let sessions_range = (before.shard, 0)..=(before.shard, u32::MAX);
            for (session_key, session) in self.inner.sessions.range(sessions_range, &guard) {
                if let Some((prev_key, _)) = latest_session.replace((session_key, session)) {
                    self.inner.sessions.remove(prev_key);
                }
            }

            match latest_session {
                // Exit guard scope with the latest session.
                Some((_, session)) => session,
                // Do nothing if there are no sessions.
                None => return Ok(()),
            }
        };

        session.cancel_before(before.seqno);
        Ok(())
    }
}

struct Inner {
    net_context: ValidatorNetworkContext,
    keypair: Arc<KeyPair>,
    sessions: TreeIndex<SessionKey, ValidatorSession>,
    config: ValidatorStdImplConfig,
}

type SessionKey = (ShardIdent, u32);
