use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;

use crate::queue::proto::QueueState;

#[derive(Clone)]
#[repr(transparent)]
pub struct QueueStateStuff {
    inner: Arc<Inner>,
}

impl QueueStateStuff {
    pub fn deserialize(block_id: &BlockId, data: &[u8]) -> Result<Self> {
        let state = tl_proto::deserialize::<QueueState>(data)?;

        anyhow::ensure!(
            block_id.shard == state.shard_ident && block_id.seqno == state.seqno,
            "short block id mismatch"
        );

        Ok(Self {
            inner: Arc::new(Inner {
                block_id: *block_id,
                state,
            }),
        })
    }

    pub fn block_id(&self) -> &BlockId {
        &self.inner.block_id
    }

    // TODO: Use only `AsRef<QueueState>`?
    pub fn state(&self) -> &QueueState {
        &self.inner.state
    }
}

impl AsRef<QueueState> for QueueStateStuff {
    fn as_ref(&self) -> &QueueState {
        &self.inner.state
    }
}

unsafe impl arc_swap::RefCnt for QueueStateStuff {
    type Base = Inner;

    fn into_ptr(me: Self) -> *mut Self::Base {
        arc_swap::RefCnt::into_ptr(me.inner)
    }

    fn as_ptr(me: &Self) -> *mut Self::Base {
        arc_swap::RefCnt::as_ptr(&me.inner)
    }

    unsafe fn from_ptr(ptr: *const Self::Base) -> Self {
        Self {
            inner: arc_swap::RefCnt::from_ptr(ptr),
        }
    }
}

pub struct Inner {
    block_id: BlockId,
    state: QueueState,
}
