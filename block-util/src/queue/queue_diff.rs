use std::sync::Arc;

use anyhow::Result;
use everscale_types::error::Error;
use everscale_types::models::*;
use everscale_types::prelude::*;
use tl_proto::TlRead;

use crate::queue::proto::QueueDiff;

#[derive(Clone)]
#[repr(transparent)]
pub struct QueueDiffStuff {
    inner: Arc<Inner>,
}

impl QueueDiffStuff {
    pub fn deserialize(block_id: &BlockId, data: &[u8]) -> Result<Self> {
        let mut offset = 0;
        let mut diff = QueueDiff::read_from(data, &mut offset)?;
        anyhow::ensure!(
            block_id.shard == diff.shard_ident && block_id.seqno == diff.seqno,
            "short block id mismatch"
        );
        anyhow::ensure!(offset == data.len(), "unexpected data after the diff");

        diff.hash = QueueDiff::compute_hash(data);

        Ok(Self {
            inner: Arc::new(Inner {
                block_id: *block_id,
                diff,
            }),
        })
    }

    pub fn block_id(&self) -> &BlockId {
        &self.inner.block_id
    }

    pub fn diff_hash(&self) -> &HashBytes {
        &self.inner.diff.hash
    }

    // TODO: Use only `AsRef<QueueDiff>`?
    pub fn diff(&self) -> &QueueDiff {
        &self.inner.diff
    }

    pub fn zip(&self, out_messages: &OutMsgDescr) -> QueueDiffMessagesIter {
        QueueDiffMessagesIter {
            index: 0,
            out_messages: out_messages.dict().clone(),
            inner: self.inner.clone(),
        }
    }
}

impl AsRef<QueueDiff> for QueueDiffStuff {
    fn as_ref(&self) -> &QueueDiff {
        &self.inner.diff
    }
}

unsafe impl arc_swap::RefCnt for QueueDiffStuff {
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

#[doc(hidden)]
pub struct Inner {
    block_id: BlockId,
    diff: QueueDiff,
}

/// Iterator over the messages in the queue diff.
#[derive(Clone)]
pub struct QueueDiffMessagesIter {
    index: usize,
    out_messages: Dict<HashBytes, (CurrencyCollection, OutMsg)>,
    inner: Arc<Inner>,
}

impl Iterator for QueueDiffMessagesIter {
    type Item = Result<OutMsg, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let messages = &self.inner.diff.messages;
        if self.index >= messages.len() {
            return None;
        }

        let hash = &messages[self.index];
        self.index += 1;

        match self.out_messages.get(hash) {
            Ok(Some((_, message))) => Some(Ok(message.clone())),
            Ok(None) => Some(Err(Error::InvalidData)),
            Err(e) => Some(Err(e)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.inner.diff.messages.len() - self.index;
        (len, Some(len))
    }
}
