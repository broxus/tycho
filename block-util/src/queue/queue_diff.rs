use std::sync::Arc;

use anyhow::Result;
use everscale_types::error::Error;
use everscale_types::models::*;
use everscale_types::prelude::*;
use tl_proto::TlRead;

use crate::archive::WithArchiveData;
use crate::queue::proto::QueueDiff;

pub type QueueDiffStuffAug = WithArchiveData<QueueDiffStuff>;

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
    type Item = Result<Lazy<OwnedMessage>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let messages = &self.inner.diff.messages;
        if self.index >= messages.len() {
            return None;
        }

        let hash = &messages[self.index];
        self.index += 1;

        match self.out_messages.get(hash) {
            Ok(Some((_, out_msg))) => {
                let OutMsg::New(out_msg) = &out_msg else {
                    return Some(Err(Error::InvalidData));
                };

                // Get the last ref from the envelope, it will be the message itself
                let out_msg = out_msg.out_msg_envelope.inner();
                let ref_count = out_msg.descriptor().reference_count();
                if ref_count > 0 {
                    if let Some(cell) = out_msg.reference_cloned(ref_count - 1) {
                        return Some(Ok(Lazy::from_raw(cell)));
                    }
                }

                Some(Err(Error::InvalidData))
            }
            Ok(None) => Some(Err(Error::InvalidData)),
            Err(e) => Some(Err(e)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // NOTE: `seld.index` increment stops at `len`
        let len = self.inner.diff.messages.len() - self.index;
        (len, Some(len))
    }
}

impl ExactSizeIterator for QueueDiffMessagesIter {
    fn len(&self) -> usize {
        // NOTE: `seld.index` increment stops at `len`
        self.inner.diff.messages.len() - self.index
    }
}

#[cfg(test)]
mod tests {
    use everscale_types::num::Tokens;

    use super::*;

    #[test]
    fn queue_diff_messages_iter() -> Result<()> {
        let mut out_messages = Dict::<HashBytes, (CurrencyCollection, OutMsg)>::new();

        let dummy_tx = Lazy::from_raw(Cell::default());

        // Fill with external messages
        for i in 0..10 {
            let message = Lazy::new(&Message {
                info: MsgInfo::ExtOut(ExtOutMsgInfo {
                    src: IntAddr::Std(StdAddr::new(0, HashBytes::from([i as u8; 32]))),
                    dst: None,
                    created_lt: i,
                    created_at: 0,
                }),
                init: None,
                body: Cell::empty_cell_ref().as_slice()?,
                layout: None,
            })?;

            out_messages.set(
                message.inner().repr_hash(),
                (
                    CurrencyCollection::ZERO,
                    OutMsg::External(OutMsgExternal {
                        out_msg: message.cast_ref().clone(),
                        transaction: dummy_tx.clone(),
                    }),
                ),
            )?;
        }

        // Fill with outgoing messages
        let mut message_hashes = Vec::new();
        for i in 0..10 {
            let addr = IntAddr::Std(StdAddr::new(0, HashBytes::from([i as u8; 32])));

            let message = Lazy::new(&Message {
                info: MsgInfo::Int(IntMsgInfo {
                    src: addr.clone(),
                    dst: addr,
                    created_lt: i,
                    ..Default::default()
                }),
                init: None,
                body: Cell::empty_cell_ref().as_slice()?,
                layout: None,
            })?;

            let message_hash = *message.inner().repr_hash();
            message_hashes.push(message_hash);

            let envelope = Lazy::new(&MsgEnvelope {
                cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                next_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
                fwd_fee_remaining: Tokens::ZERO,
                message: message.cast_into(),
            })?;

            out_messages.set(
                message_hash,
                (
                    CurrencyCollection::ZERO,
                    OutMsg::New(OutMsgNew {
                        out_msg_envelope: envelope,
                        transaction: dummy_tx.clone(),
                    }),
                ),
            )?;
        }

        let out_messages = AugDict::from_parts(out_messages, CurrencyCollection::ZERO);

        // Create queue diff
        message_hashes.sort_unstable();
        assert_eq!(message_hashes.len(), 10);

        let diff = QueueDiffStuff {
            inner: Arc::new(Inner {
                block_id: BlockId::default(),
                diff: QueueDiff {
                    hash: HashBytes::ZERO,
                    prev_hash: HashBytes::ZERO,
                    shard_ident: ShardIdent::BASECHAIN,
                    seqno: 1,
                    processed_upto: Default::default(),
                    messages: message_hashes.clone(),
                },
            }),
        };

        // Verify messages
        let mut messages_iter = diff.zip(&out_messages);
        assert_eq!(message_hashes.len(), messages_iter.len());

        for expected_hash in message_hashes {
            let message = messages_iter.next().unwrap()?;
            assert_eq!(expected_hash, *message.inner().repr_hash());

            message.load().unwrap();
        }

        assert!(messages_iter.next().is_none());
        assert_eq!(messages_iter.len(), 0);
        assert_eq!(messages_iter.size_hint(), (0, Some(0)));

        Ok(())
    }
}
