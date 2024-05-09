use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockIdShort, ShardIdent};

use tycho_core::internal_queue::{
    persistent::{
        persistent_state::PersistentStateImpl, persistent_state_snapshot::PersistentStateSnapshot,
    },
    queue::{Queue, QueueImpl},
    session::{session_state::SessionStateImpl, session_state_snapshot::SessionStateSnapshot},
    types::QueueDiff,
};

pub(crate) use tycho_core::internal_queue::iterator::QueueIterator;

use crate::{tracing_targets, utils::shard::SplitMergeAction};
