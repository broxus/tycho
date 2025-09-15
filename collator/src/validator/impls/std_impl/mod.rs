use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use indexmap::{self, IndexMap};
use serde::{Deserialize, Serialize};
use session::DebugLogValidatorSesssion;
use tycho_crypto::ed25519::KeyPair;
use tycho_types::models::*;
use tycho_util::{FastHashMap, serde_helpers};

use self::session::ValidatorSession;
use crate::tracing_targets;
use crate::validator::rpc::ExchangeSignaturesBackoff;
use crate::validator::{
    AddSession, ValidationSessionId, ValidationStatus, Validator, ValidatorNetworkContext,
};

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
    #[serde(with = "serde_helpers::humantime")]
    pub failed_exchange_interval: Duration,

    /// Maximum number of parallel requests for exchanging signatures.
    ///
    /// Default: 10.
    pub max_parallel_requests: usize,

    /// Number of slots for future signatures.
    ///
    /// Default: 3.
    pub signature_cache_slots: u32,

    /// Number of blocks to keep even after their validation.
    ///
    /// Default: 10.
    pub old_blocks_to_keep: u32,
}

impl Default for ValidatorStdImplConfig {
    fn default() -> Self {
        Self {
            exchange_signatures_backoff: Default::default(),
            exchange_signatures_timeout: Duration::from_secs(1),
            failed_exchange_interval: Duration::from_secs(10),
            max_parallel_requests: 10,
            signature_cache_slots: 3,
            old_blocks_to_keep: 10,
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
    fn add_session(&self, info: AddSession<'_>) -> Result<()> {
        let session = ValidatorSession::new(
            &self.inner.net_context,
            self.inner.keypair.clone(),
            &self.inner.config,
            info,
        )?;

        let mut sessions = self.inner.sessions.lock();
        let shard_sessions = sessions.entry(info.shard_ident).or_default();

        match shard_sessions.entry(info.session_id) {
            indexmap::map::Entry::Vacant(entry) => {
                tracing::debug!(
                    target: tracing_targets::VALIDATOR,
                    session = ?DebugLogValidatorSesssion(&session),
                    "new validator session added",
                );
                entry.insert(session);
                Ok(())
            }
            indexmap::map::Entry::Occupied(_) => {
                anyhow::bail!(
                    "validator session already exists: ({}, {:?})",
                    info.shard_ident,
                    info.session_id
                )
            }
        }
    }

    async fn validate(
        &self,
        session_id: ValidationSessionId,
        block_id: &BlockId,
    ) -> Result<ValidationStatus> {
        let session = 'session: {
            if let Some(shard_sessions) = self.inner.sessions.lock().get(&block_id.shard)
                && let Some(session) = shard_sessions.get(&session_id)
            {
                break 'session session.clone();
            }

            anyhow::bail!(
                "validator session not found: ({}, {:?})",
                block_id.shard,
                session_id,
            );
        };

        session.validate_block(block_id).await
    }

    fn cancel_validation(
        &self,
        until: &BlockIdShort,
        session_id: Option<ValidationSessionId>,
    ) -> Result<()> {
        let session = {
            let mut sessions = self.inner.sessions.lock();
            let Some(shard_sessions) = sessions.get_mut(&until.shard) else {
                return Ok(());
            };

            let (id, session) = if let Some(session_id) = session_id {
                let Some(session) = shard_sessions.get(&session_id).cloned() else {
                    tracing::warn!(
                        target: tracing_targets::VALIDATOR,
                        session_id = ?session_id,
                        shard = %until.shard,
                        "validation session not found for explicit session_id"
                    );
                    return Ok(());
                };
                (session_id, session)
            } else {
                // Find the latest session that has started before the specified block.
                let Some((id, session)) = shard_sessions.iter().rev().find_map(|(id, session)| {
                    (session.start_block_seqno() <= until.seqno).then(|| (*id, session.clone()))
                }) else {
                    return Ok(());
                };
                (id, session)
            };

            // Remove all sessions before the found one.

            // NOTE: We need to wait for some blocks in the new session before removing the old ones,
            // so that lagging validators have enough time to receive our signatures.
            let seqno_threshold = session
                .start_block_seqno()
                .saturating_add(self.inner.config.old_blocks_to_keep);

            if until.seqno >= seqno_threshold {
                while let Some(entry) = shard_sessions.first_entry() {
                    if *entry.key() < id {
                        // Fully cancel the session before removing it.
                        entry.get().cancel();
                        entry.shift_remove();
                    } else {
                        break;
                    }
                }
            }

            session
        };

        // Partially cancel the found session.
        session.cancel_until(until.seqno);
        Ok(())
    }
}

struct Inner {
    net_context: ValidatorNetworkContext,
    keypair: Arc<KeyPair>,
    sessions: parking_lot::Mutex<Sessions>,
    config: ValidatorStdImplConfig,
}

type Sessions = FastHashMap<ShardIdent, ShardSessions>;
/// We use `IndexMap` because "subset short hash" component of session id is not sequential
type ShardSessions = IndexMap<ValidationSessionId, ValidatorSession>;
